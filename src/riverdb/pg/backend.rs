use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicU32, AtomicBool, AtomicPtr, AtomicI32};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release, AcqRel};
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};

use chrono::{Local, DateTime};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn, instrument};
use bytes::Bytes;

use crate::define_event;
use crate::riverdb::{config, Error, Result};
use crate::riverdb::config::TlsMode;
use crate::riverdb::pg::{BackendConnState, ClientConn, Connection, ConnectionPool};
use crate::riverdb::server::{Transport};
use crate::riverdb::server;
use crate::riverdb::pg::connection::{Backlog, read_and_flush_backlog};
use crate::riverdb::pg::backend_state::BackendState;
use crate::riverdb::common::{AtomicCell, AtomicArc, coarse_monotonic_now, AtomicRef};
use crate::riverdb::pg::protocol::{ServerParams, MessageParser, Message, MessageBuilder, Tag, SSL_REQUEST, SSL_ALLOWED, PROTOCOL_VERSION};
use tokio::io::Interest;


pub struct BackendConn {
    transport: Transport,
    /// id is set once and then read-only. Starts as 0.
    id: AtomicU32,
    /// added_to_pool is a course-grained monotonic clock that is 0, or records when this was returned to the pool
    added_to_pool: AtomicU32,
    has_send_backlog: AtomicBool,
    for_transaction: AtomicBool,
    state: BackendConnState,
    client: AtomicArc<ClientConn>,
    send_backlog: Backlog,
    pool: AtomicRef<'static, ConnectionPool>,
    server_params: Mutex<ServerParams>,
    pid: AtomicI32,
    secret: AtomicI32,
    created_at: DateTime<Local>,
}

impl BackendConn {
    #[instrument]
    pub async fn run(&self, pool: &'static ConnectionPool) -> Result<()> {
        // XXX: This code is very similar to ClientConn::run.
        // If you change this, you probably need to change that too.

        self.start(pool).await?;

        let mut parser = MessageParser::new();
        let mut client: Option<Arc<ClientConn>> = None; // keep
        loop {
            // Check first if we have another message in the parser
            if let Some(result) = parser.next() {
                let msg = result?;
                let tag = msg.tag();
                debug!(%tag, "received message from backend");
                if !self.state.msg_is_allowed(tag) {
                    return Err(Error::new(format!("unexpected message {} for state {:?}", tag, self.state)));
                }

                backend_message::run(self, client.as_ref(), msg).await?;
            } else {
                // We don't want to clone the Arc everytime, so we clone() it once calling self.get_client()
                // And then we cache that Arc, checking that it's still the current client with self.has_client()
                // Which is cheaper the the atomic-read-modify-write ops used increment and decrement and Arc.
                if client.is_none() || !self.has_client(client.as_ref().unwrap()) {
                    client = self.get_client();
                }

                read_and_flush_backlog(
                    self,
                    parser.bytes_mut(),
                    client.as_ref().map(|arc| arc.as_ref()),
                ).await?;
            }
        }
    }

    async fn start(&self, pool: &'static ConnectionPool) -> Result<()> {
        let cluster = pool.config.cluster.unwrap();
        self.pool.store(Some(pool));

        match cluster.backend_tls {
            TlsMode::Disabled | TlsMode::Invalid => (),
            _ => {
                self.ssl_handshake(pool, cluster).await?;
            }
        }

        let mut params = ServerParams::default();
        params.add("database", &pool.config.database);
        params.add("user", &pool.config.user);
        params.add("client_encoding", "UTF8");

        return backend_connected::run(self, &mut params).await;
    }

    pub async fn ssl_handshake(&self, pool: &'static ConnectionPool, cluster: &'static config::PostgresCluster) -> Result<()> {
        const SSL_REQUEST_MSG: &[u8] = &[0, 0, 0, 8, 4, 210, 22, 47];
        let ssl_request = Message::new(Bytes::from_static(SSL_REQUEST_MSG));

        self.state.transition(self, BackendState::SSLHandshake)?;
        backend_send_message::run(self, ssl_request).await?;

        self.transport.ready(Interest::READABLE).await?;
        let mut buf: [u8; 1] = [0];
        let n = self.transport.try_read(&mut buf[..])?;
        if n == 1 {
            if buf[0] == SSL_ALLOWED {
                let tls_config = cluster.backend_tls_config.clone().unwrap();
                self.transport.upgrade_client(tls_config, cluster.backend_tls, pool.config.tls_host.as_str()).await
            } else if let TlsMode::Prefer = cluster.backend_tls {
                Err(Error::new(format!("{} does not support TLS", pool.config.address.as_ref().unwrap())))
            } else {
                Ok(())
            }
        } else {
            unreachable!(); // readable, but not a single byte could be read? Not possible.
        }
    }

    pub fn get_client(&self) -> Option<Arc<ClientConn>> {
        self.client.load()
    }

    pub fn has_client(&self, client: &Arc<ClientConn>) -> bool {
        self.client.is(client)
    }

    pub fn set_client(&self, client: Option<Arc<ClientConn>>) {
        self.client.store(client);
    }

    pub async fn check_health_and_set_role(&self, role: &str) -> Result<()> {
        Ok(())
    }

    pub fn created_for_transaction(&self) -> bool {
        self.for_transaction.load(Relaxed)
    }

    pub fn in_pool(&self) -> bool {
        if let BackendState::InPool = self.state.get() {
            debug_assert_ne!(self.added_to_pool.load(Relaxed), 0);
            true
        } else {
            false
        }
    }

    pub fn set_in_pool(&self) -> bool {
        if let Err(e) = self.state.transition(self,BackendState::InPool) {
            warn!(?e, "cannot transition to InPool state");
            false
        } else {
            self.added_to_pool.store(coarse_monotonic_now(), Relaxed);
            self.for_transaction.store(false, Relaxed);
            true
        }
    }

    pub fn params(&self) -> MutexGuard<ServerParams> {
        self.server_params.lock().unwrap()
    }

    pub async fn backend_connected(&self, _: &mut backend_connected::Event, params: &mut ServerParams) -> Result<()> {
        let mut mb = MessageBuilder::new(Tag::UNTAGGED);
        mb.write_i32(PROTOCOL_VERSION);
        mb.write_params(params);
        let startup_msg = mb.finish();

        backend_send_message::run(self, startup_msg).await
    }

    pub async fn backend_message(&self, _: &mut backend_message::Event, client: Option<&Arc<ClientConn>>, msg: Message) -> Result<()> {
        match self.state.get() {
            BackendState::StateInitial | BackendState::SSLHandshake => {},
            BackendState::Authentication => {},
            BackendState::Startup => {},
            BackendState::InPool => {},
            _ => {
                // Forward the message to the client, if there is one
                if let Some(client) = client {
                    return client.write_or_buffer(msg.into_bytes());
                }
            }
        }
        Ok(())
    }

    pub async fn backend_send_message(&self, _: &mut backend_send_message::Event, msg: Message) -> Result<()> {
        self.write_or_buffer(msg.into_bytes())
    }
}

impl server::Connection for BackendConn {
    fn new(stream: TcpStream) -> Self {
        BackendConn {
            transport: Transport::new(stream),
            id: Default::default(),
            added_to_pool: Default::default(),
            has_send_backlog: Default::default(),
            for_transaction: Default::default(),
            state: Default::default(),
            client: Default::default(),
            send_backlog: Mutex::new(Default::default()),
            pool: AtomicRef::default(),
            server_params: Mutex::new(ServerParams::default()),
            pid: Default::default(),
            secret: Default::default(),
            created_at: Local::now(),
        }
    }

    fn id(&self) -> u32 {
        self.id.load(Relaxed)
    }

    fn set_id(&self, id: u32) {
        self.id.store(id, Relaxed);
    }

    fn last_active(&self) -> u32 {
        self.added_to_pool.load(Relaxed)
    }

    fn close(&self) {
        self.transport.close();
    }
}

impl Connection for BackendConn {
    fn has_backlog(&self) -> bool {
        self.has_send_backlog.load(Acquire)
    }

    fn set_has_backlog(&self, value: bool) {
        self.has_send_backlog.store(value,Release);
    }

    fn backlog(&self) -> &Mutex<VecDeque<Bytes>> {
        &self.send_backlog
    }

    fn transport(&self) -> &Transport {
        &self.transport
    }

    fn is_closed(&self) -> bool {
        if let BackendState::Closed = self.state.get() {
            true
        } else {
            false
        }
    }
}

impl Debug for BackendConn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "pg::BackendConn{{id: {}, state: {:?}}}",
            self.id.load(Relaxed),
            self.state))
    }
}

/// backend_connected is called when a new backend session is being established.
///     backend: &BackendConn : the event source handling the backend connection
///     params: &ServerParams : key-value pairs that will be passed to the connected backend in the startup message (including database and user)
/// BackendConn::backend_connected is called by default and sends ServerParams in the startup message.
/// If it returns an error, the associated session is terminated.
define_event!(backend_connected, (backend: &'a BackendConn, params: &'a mut ServerParams) -> Result<()>);

/// backend_message is called when a Postgres protocol.Message is received in a backend db connection.
///     backend: &BackendConn : the event source handling the client connection
///     msg: protocol.Message is the received protocol.Message
/// You can replace msg by creating and passing a new Message object to ev.next(...)
/// It's also possible to replace a single Message with many by calling ev.next() for each.
/// Or conversely replace many messages with fewer by buffering the Message and not immediately calling next.
/// BackendConn::backend_message is called by default and does further processing on the Message,
/// including potentially forwarding it to associated client session. Symmetric with client_message.
/// If it returns an error, the associated session is terminated.
define_event!(backend_message, (backend: &'a BackendConn, client: Option<&'a Arc<ClientConn>>, msg: Message) -> Result<()>);

/// backend_send_message is called to send a Message to a backend db connection.
///     backend: &BackendConn : the event source handling the client connection
///     msg: protocol.Message is the protocol.Message to send
/// You can replace msg by creating and passing a new Message object to ev.next(...)
/// It's also possible to replace a single Message with many by calling ev.next() for each.
/// Or conversely replace many messages with fewer by buffering the Message and not immediately calling next.
/// BackendConn::backend_send_message is called by default and sends the Message to the db server.
/// If it returns an error, the associated session is terminated.
define_event!(backend_send_message, (backend: &'a BackendConn, msg: Message) -> Result<()>);