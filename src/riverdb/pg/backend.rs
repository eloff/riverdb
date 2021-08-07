use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{fence, AtomicU32, AtomicBool, AtomicPtr, AtomicI32, AtomicU8};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release, AcqRel};
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;

use chrono::{Local, DateTime};
use tokio::net::TcpStream;
use tokio::io::Interest;
use tokio::sync::mpsc::{channel, Sender};
use tracing::{debug, error, info, warn, instrument};
use bytes::Bytes;
use futures::try_join;

use crate::{define_event, query};
use crate::riverdb::{config, Error, Result};
use crate::riverdb::config::TlsMode;
use crate::riverdb::pg::{BackendConnState, ClientConn, Connection, ConnectionPool, IsolationLevel, Rows};
use crate::riverdb::server::{Transport, Connection as ServerConnection};
use crate::riverdb::server;
use crate::riverdb::pg::connection::{Backlog, read_and_flush_backlog};
use crate::riverdb::pg::backend_state::BackendState;
use crate::riverdb::common::{AtomicCell, coarse_monotonic_now, AtomicRef, change_lifetime, AtomicRefCell};
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
    tx_isolation_level: AtomicCell<IsolationLevel>, // do we need this?
    state: BackendConnState,
    pending_requests: AtomicI32,
    client: AtomicArc<ClientConn>,
    send_backlog: Backlog,
    pool: AtomicRef<'static, ConnectionPool>,
    rows: AtomicRefCell<Sender<Message>>, // forwards server messages to the Rows iterator instead of to client
    pipelined_rows: Mutex<VecDeque<Sender<Message>>>, // additional Rows iterators if there are pipelined queries
    server_params: Mutex<ServerParams>,
    pid: AtomicI32,
    secret: AtomicI32,
    created_at: DateTime<Local>,
}

impl BackendConn {
    pub async fn connect(address: &SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(address).await?;
        Ok(Self::new(stream))
    }

    #[instrument]
    pub async fn run(&self, pool: &ConnectionPool) -> Result<()> {
        // XXX: This code is very similar to ClientConn::run.
        // If you change this, you probably need to change that too.

        // Safety: pool is 'static, but if we mark it as such the compiler barfs.
        // See: https://github.com/rust-lang/rust/issues/87632
        unsafe {
            self.pool.store(Some(change_lifetime(pool)));
        }
        self.start(&pool.config.user, &pool.config.password, pool).await?;

        let mut stream = MessageStream::new(self);
        let mut sender: Option<Arc<ClientConn>> = None;
        loop {
            // We don't want to clone the Arc everytime, so we clone() it once and cache it,
            // checking that it's still the current one with has_client. That's cheaper
            // than the atomic-read-modify-write ops used increment and decrement and Arc.
            if sender.is_none() || !self.has_client(sender.as_ref().unwrap()) {
                sender = self.get_client();
            }
            let sender_ref = sender.as_ref().map(|arc| arc.as_ref());

            let msg = stream.next(sender_ref).await?;
            backend_message::run(self, sender_ref, msg).await?;
        }
    }

    #[inline]
    pub async fn send(&self, msg: Message) -> Result<usize> {
        backend_send_message::run(self, msg).await
    }

    pub async fn forward(&self, client: Option<&Arc<ClientConn>>, msg: Message) -> Result<usize> {
        let mut sent = 0;
        if let Some(client) = client {
            let tag = msg.tag();

            if self.pending_requests.load(Relaxed) > 0 {
                if tag == Tag::READY_FOR_QUERY {
                    if self.rows.is_some() {
                        // This doesn't work because a new query can see rows as full
                        // append to pipelined_rows, and then here we see pipelined_rows
                        // as empty and clear rows. That leaves an orphaned Sender in
                        // pipelined_rows that we will never check. We can fix it by holding
                        // the mutex longer in both methods to sync both operations, but
                        // that makes already fragile and dangerous code much worse.
                        // There has to be a better way of doing this without having to lock
                        // the mutex? Or do we double down on the mutex but remove the
                        // MPSC channel and replace it with just a plain write + wakeup (how?)
                        let next_rows = self.pipelined_rows.lock().unwrap().pop_front();
                        self.rows.store(next_rows);
                    } else {
                        sent = client.send(msg).await?;
                    }

                    if self.pending_requests.fetch_sub(1, Relaxed) == 1 {
                        // pending_requests has reached zero, we can maybe release the backend to the pool
                        assert!(self.rows.is_none());
                        debug_assert!(self.pipelined_rows.lock().unwrap().is_empty());
                        if let Some(backend) = client.release_backend() {
                            self.client.store(None);
                            self.pool.load().unwrap().put(backend);
                        }
                    }
                } else if let Some(rows) = self.rows.load() {
                    // If this fails, it's because the Rows was dropped, so drop the message.
                    // Don't set rows to None here, do that as normal above when handling READY_FOR_QUERY.
                    let _ = rows.send(msg).await;
                } else {
                    sent = client.send(msg).await?;
                }
            }
        }
        Ok(sent)
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
        self.send(ssl_request).await?;

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
        if self.state.get() == BackendState::InPool {
            self.state.transition(self, BackendState::Ready)?;
            self.added_to_pool.store(0, Relaxed);
        }

        // Safety: I don't know why this is required here. Rust bug?
        let role: &'static str = unsafe { change_lifetime(role) };
        let application_name: &'static str = unsafe { change_lifetime(application_name) };
        if role.is_empty() {
            self.query(query!("SET application_name TO {}", application_name)).await?;
        } else {
            try_join!(
                self.execute(query!("SET ROLE {}", role)),
                self.execute(query!("SET application_name TO {}", application_name))
            )?;
        }

        Ok(())
    }

    pub async fn query(&self, escaped_query: Message) -> Result<Rows> {
        let (tx, rx) = channel(config::ROW_CHANNEL_NUM_MESSAGES_BUFFER);
        self.add_sender(tx);
        self.send(escaped_query).await?;
        Ok(Rows::new(rx))
    }

    pub async fn execute(&self, escaped_query: Message) -> Result<i32> {
        let (tx, rx) = channel(1);
        self.add_sender(tx);
        self.send(escaped_query).await?;
        let mut rows = Rows::new(rx);
        let has_next = rows.next().await?;
        assert!(!has_next);
        Ok(rows.affected())
    }

    fn add_sender(&self, tx: Sender<Message>) {
        match self.rows.compare_exchange(None, Some(tx)) {
            Ok(_) => (),
            Err(tx) => {
                self.pipelined_rows.lock().unwrap().push_back(tx.unwrap());
            }
        }
    }

    /// Returns the associated ClientConn, if any.
    pub fn client(&self) -> Option<&Arc<ClientConn>> {
        self.client.load()
    }

    /// Sets the associated ClientConn.
    pub fn set_client(&self, client: Option<Arc<ClientConn>>) {
        self.client.store(client);
    }

    pub fn created_for_transaction(&self) -> bool {
        self.for_transaction.load(Relaxed)
    }

    pub(crate) fn set_created_for_transaction(&self, value: bool) {
        self.for_transaction.store(value, Relaxed)
    }

    pub fn isolation_level(&self) -> IsolationLevel {
        self.tx_isolation_level.load()
    }

    pub fn set_isolation_level(&self, level: IsolationLevel) {
        self.tx_isolation_level.store(level);
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
            true
        }
    }

    pub fn params(&self) -> MutexGuard<ServerParams> {
        self.server_params.lock().unwrap()
    }

    #[instrument]
    pub async fn backend_connected(&self, _: &mut backend_connected::Event, params: &mut ServerParams) -> Result<()> {
        let mut mb = MessageBuilder::new(Tag::UNTAGGED);
        mb.write_i32(PROTOCOL_VERSION);
        mb.write_params(params);

        self.state.transition(self, BackendState::Authentication);

        self.send(mb.finish()).await?;
        Ok(())
    }

    #[instrument]
    pub async fn backend_message(&self, _: &mut backend_message::Event, client: Option<&ClientConn>, msg: Message) -> Result<()> {
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
                self.forward(client, msg).await?;
                // TODO else this is part of a query workflow, do what with it???
                Ok(())
            }
        }
    }

    #[instrument]
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
                        self.send(mb.finish()).await?;
                        Ok(())
                    },
                    AuthType::MD5 => {
                        let salt = r.read_i32();
                        if salt == 0 {
                            r.error()?;
                        }
                        let md5_password = hash_md5_password(&user, &password, salt);
                        let mut mb = MessageBuilder::new(Tag::PASSWORD_MESSAGE);
                        mb.write_str(&md5_password);
                        self.send(mb.finish()).await?;
                        Ok(())
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

    #[instrument]
    pub async fn backend_send_message(&self, _: &mut backend_send_message::Event, msg: Message) -> Result<usize> {
        if msg.is_empty() {
            return Ok(0);
        }
        match msg.tag() {
            Tag::QUERY => { // TODO what other tags expect a response?
                self.pending_requests.fetch_add(msg.count() as i32, Relaxed);
            },
            _ => (),
        }
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
            tx_isolation_level: AtomicCell::default(),
            state: Default::default(),
            pending_requests: AtomicI32::new(0),
            client: AtomicRefCell::default(),
            send_backlog: Mutex::new(Default::default()),
            pool: AtomicRef::default(),
            rows: AtomicRefCell::default(),
            pipelined_rows: Mutex::new(VecDeque::new()),
            server_params: Mutex::new(ServerParams::default()),
            pid: AtomicI32::new(0),
            secret: AtomicI32::new(0),
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
///     client: Option<&'a ClientConn> : the associated client connection (if any)
///     msg: protocol.Message is the received protocol.Message
/// You can replace msg by creating and passing a new Message object to ev.next(...)
/// A Message may contain multiple wire protocol messages, see Message::next().
/// BackendConn::backend_message is called by default and does further processing on the Message,
/// including potentially forwarding it to associated client session. Symmetric with client_message.
/// If it returns an error, the associated session is terminated.
define_event!(backend_message, (backend: &'a BackendConn, client: Option<&'a ClientConn>, msg: Message) -> Result<()>);

/// backend_send_message is called to send a Message to a backend db connection.
///     backend: &BackendConn : the event source handling the backend connection
///     msg: protocol.Message is the protocol.Message to send
///     prefer_buffer: bool : passed to write_or_buffer, see docs for that method
/// You can replace msg by creating and passing a new Message object to ev.next(...)
/// A Message may contain multiple wire protocol messages, see Message::next().
/// BackendConn::backend_send_message is called by default and sends the Message to the db server.
/// If it returns an error, the associated session is terminated.
/// /// Returns the number of bytes actually written (not buffered.)
define_event!(backend_send_message, (backend: &'a BackendConn, msg: Message) -> Result<usize>);

/// backend_authenticate is called with each Message received while in the Authentication state
define_event!(backend_authenticate, (backend: &'a BackendConn, msg: Message) -> Result<()>);