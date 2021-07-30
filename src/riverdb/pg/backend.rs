use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicU32, AtomicBool, AtomicPtr, AtomicI32};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release, AcqRel};
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};

use chrono::{Local, DateTime};
use tokio::net::TcpStream;
use tokio::io::Interest;
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
use crate::riverdb::common::{AtomicCell, AtomicArc, coarse_monotonic_now, AtomicRef,change_lifetime};
use crate::riverdb::pg::protocol::{ServerParams, MessageParser, Message, MessageBuilder, Tag, SSL_REQUEST, SSL_ALLOWED, PROTOCOL_VERSION, MessageReader, AuthType, PostgresError, hash_md5_password};
use crate::riverdb::config::conf;
use crate::riverdb::pg::message_stream::MessageStream;


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
    pub async fn run(&self, pool: &ConnectionPool) -> Result<()> {
        // XXX: This code is very similar to ClientConn::run.
        // If you change this, you probably need to change that too.

        // Safety: pool is 'static, but if we mark it as such the compiler throws a fit?!?
        unsafe {
            self.pool.store(Some(change_lifetime(pool)));
        }
        self.start(&pool.config.user, &pool.config.password, pool).await?;

        let mut stream = MessageStream::new(self);
        let mut sender: Option<Arc<ClientConn>> = None;
        loop {
            // We don't want to clone the Arc everytime, so we clone() it once calling self.get_other_conn()
            // And then we cache that Arc, checking that it's still the current with self.has_other_conn()
            // Which is cheaper the the atomic-read-modify-write ops used increment and decrement and Arc.
            if sender.is_none() || !self.has_client(sender.as_ref().unwrap()) {
                sender = self.get_client();
            }
            let sender_ref = sender.as_ref().map(|arc| arc.as_ref());

            let msg = stream.next(sender_ref).await?;
            backend_message::run(self, sender.as_ref(), msg).await?;
        }
    }

    pub async fn test_auth(&self, user: &str, password: &str, pool: &ConnectionPool) -> Result<()> {
        self.start(user, password, pool).await?;

        debug_assert_eq!(self.state.get(), BackendState::Authentication);

        let mut stream = MessageStream::<Self, ClientConn>::new(self);
        loop {
            let msg = stream.next(None).await?;

            backend_message::run(self, None, msg).await?;
            if self.state.get() == BackendState::Startup {
                return Ok(())
            }
        }
    }

    async fn start(&self, user: &str, password: &str, pool: &ConnectionPool) -> Result<()> {
        let mut params = ServerParams::default();
        params.add("database", &pool.config.database);
        params.add("user", user);
        params.add("client_encoding", "UTF8");
        // We can't customize the application_name at connection, which happens once.
        // We need to do it in check_health_and_set_role which happens for each session that uses the connection.
        params.add("application_name", "riverdb");

        // Remember the user and password in the server_params, we'll need it during authentication
        // We'll overwrite them later when processing the server's startup response.
        {
            let mut server_params = self.server_params.lock().unwrap();
            server_params.add("password", password);
            server_params.add("user", user);
        }

        let cluster = pool.config.cluster.unwrap();
        match cluster.backend_tls {
            TlsMode::Disabled | TlsMode::Invalid => (),
            _ => {
                self.ssl_handshake(pool, cluster).await?;
            }
        }

        return backend_connected::run(self, &mut params).await;
    }

    pub async fn ssl_handshake(&self, pool: &ConnectionPool, cluster: &config::PostgresCluster) -> Result<()> {
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

    pub async fn check_health_and_set_role(&self, application_name: &str, role: &str) -> Result<()> {
        // TODO SET role, SET application_name
        // try_join!(
        //     self.execute(escape_query!("SET ROLE TO {}", role))
        //     self.execute(escape_query!("SET application_name TO {}", application_name))
        // ).await;
        Ok(())
    }

    pub async fn query(&self, escaped_query: &str) -> Result<()> { // TODO Result<Arc<Rows>>
        // TODO write escape_query!() formatting macro

        // TODO let run() keep pumping the message loop, and just put it in a state of
        // ReceiveResult or something, store a reference to Rows on self, and feed it the messages
        // as they're received (which it can buffer in a VecDeque until ready to process them.)
        // We can use an Arc<Rows> for that.
        todo!()
    }

    /// Returns the associated ClientConn, if any.
    pub fn get_client(&self) -> Option<Arc<ClientConn>> {
        self.client.load()
    }

    /// Returns true if client is set as the associated ClientConn.
    pub fn has_client(&self, client: &Arc<ClientConn>) -> bool {
        self.client.is(client)
    }

    /// Sets the associated ClientConn.
    pub fn set_client(&self, client: Option<Arc<ClientConn>>) {
        self.client.store(client);
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

        self.state.transition(self, BackendState::Authentication);

        backend_send_message::run(self, mb.finish()).await
    }

    pub async fn backend_message(&self, _: &mut backend_message::Event, client: Option<&Arc<ClientConn>>, msg: Message) -> Result<()> {
        match self.state.get() {
            BackendState::StateInitial | BackendState::SSLHandshake => {
                Err(Error::new(format!("unexpected message for initial state: {:?}", msg.tag())))
            },
            BackendState::Authentication => {
                backend_authenticate::run(self, msg).await
            },
            BackendState::Startup => {
                // TODO ???
                Ok(())
            },
            BackendState::InPool => {
                if msg.tag() == Tag::PARAMETER_STATUS {
                    todo!(); // TODO set param in server_params
                } else if msg.tag() == Tag::ERROR_RESPONSE {
                    todo!(); // TODO log error and close the connection
                }
                // Else ignore the message
                // TODO log that we're ignoring a message of type msg.tag()
                Ok(())
            },
            _ => {
                // Forward the message to the client, if there is one
                if let Some(client) = client {
                    return backend_send_message::run(self, msg).await
                }
                // TODO else this is part of a query workflow, do what with it???
                Ok(())
            }
        }
    }

    pub async fn backend_authenticate(&self, _: &mut backend_authenticate::Event, msg: Message) -> Result<()> {
        match msg.tag() {
            Tag::AUTHENTICATION_OK => {
                let r = MessageReader::new(&msg);
                let auth_type = r.read_i32();
                if auth_type == 0 {
                    r.error()?;
                }
                let auth_type = AuthType::from(auth_type);
                let (user, password) = {
                    let server_params = self.server_params.lock().unwrap();
                    (server_params.get("user").expect("missing user").to_string(),
                     server_params.get("password").expect("missing password").to_string())
                };

                match auth_type {
                    AuthType::Ok => {
                        // Success!
                        self.state.transition(self, BackendState::Startup)
                    },
                    AuthType::ClearText => {
                        if !self.is_tls() {
                            warn!("sending clear text password over unencrypted connection. Consider requiring TLS or using a different authentication scheme.")
                        }
                        let mut mb = MessageBuilder::new(Tag::PASSWORD_MESSAGE);
                        mb.write_str(&password);
                        backend_send_message::run(self, mb.finish()).await
                    },
                    AuthType::MD5 => {
                        let salt = r.read_i32();
                        if salt == 0 {
                            r.error()?;
                        }
                        let md5_password = hash_md5_password(&user, &password, salt);
                        let mut mb = MessageBuilder::new(Tag::PASSWORD_MESSAGE);
                        mb.write_str(&md5_password);
                        backend_send_message::run(self, mb.finish()).await
                    },
                    _ => Err(Error::new(format!("unsupported authentication scheme (pull requests welcome!) {}", auth_type)))
                }
            },
            Tag::ERROR_RESPONSE => {
                Err(Error::from(PostgresError::new(msg)?))
            },
            _ => Err(Error::new(format!("unexpected message {}", msg.tag())))
        }
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

    fn msg_is_allowed(&self, tag: Tag) -> Result<()> {
        if self.state.msg_is_allowed(tag) {
            Ok(())
        } else {
            Err(Error::new(format!("unexpected backend message {} for state {:?}", tag, self.state.get())))
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
///     backend: &BackendConn : the event source handling the backend connection
///     client: Option<&'a Arc<ClientConn>> : the associated client connection (if any)
///     msg: protocol.Message is the received protocol.Message
/// You can replace msg by creating and passing a new Message object to ev.next(...)
/// It's also possible to replace a single Message with many by calling ev.next() for each.
/// Or conversely replace many messages with fewer by buffering the Message and not immediately calling next.
/// BackendConn::backend_message is called by default and does further processing on the Message,
/// including potentially forwarding it to associated client session. Symmetric with client_message.
/// If it returns an error, the associated session is terminated.
define_event!(backend_message, (backend: &'a BackendConn, client: Option<&'a Arc<ClientConn>>, msg: Message) -> Result<()>);

/// backend_send_message is called to send a Message to a backend db connection.
///     backend: &BackendConn : the event source handling the backend connection
///     msg: protocol.Message is the protocol.Message to send
/// You can replace msg by creating and passing a new Message object to ev.next(...)
/// It's also possible to replace a single Message with many by calling ev.next() for each.
/// Or conversely replace many messages with fewer by buffering the Message and not immediately calling next.
/// BackendConn::backend_send_message is called by default and sends the Message to the db server.
/// If it returns an error, the associated session is terminated.
define_event!(backend_send_message, (backend: &'a BackendConn, msg: Message) -> Result<()>);

/// backend_authenticate is called with each Message received while in the Authentication state
define_event!(backend_authenticate, (backend: &'a BackendConn, msg: Message) -> Result<()>);