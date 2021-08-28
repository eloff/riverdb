use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicU32, AtomicBool, AtomicI32, AtomicU64};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release};
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;


use chrono::{Local, DateTime};
use tokio::net::TcpStream;
use tokio::io::Interest;

use tokio::sync::Notify;
use tracing::{error, warn, instrument};
use bytes::Bytes;
use futures::try_join;

use crate::{define_event, query};
use crate::riverdb::{config, Error, Result};
use crate::riverdb::config::TlsMode;
use crate::riverdb::pg::{BackendConnState, ClientConn, Connection, ConnectionPool, Rows};
use crate::riverdb::server::{Transport, Connection as ServerConnection};
use crate::riverdb::server;
use crate::riverdb::pg::connection::{Backlog};
use crate::riverdb::pg::backend_state::BackendState;
use crate::riverdb::common::{SpscQueue, AtomicArc, AtomicRef, coarse_monotonic_now, change_lifetime};
use crate::riverdb::pg::protocol::{
    ServerParams, Messages, MessageBuilder, Tag, SSL_ALLOWED, PROTOCOL_VERSION,
    AuthType, PostgresError, hash_md5_password
};

use crate::riverdb::pg::message_stream::MessageStream;

const MAX_PENDING_REQUESTS: u32 = 32;
const CLIENT_REQUEST: u64 = 1;
const BACKEND_REQUEST: u64 = 2;
const REQUEST_TYPE_MASK: u64 = 3;

pub type MessageQueue = SpscQueue<Messages, 32>;

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
    pending_requests: AtomicU64, // a bitfield identifying client and backend (iterator) requests
    iterator_messages: MessageQueue, // messages queued for Rows iterators
    iterators: SpscQueue<Notify, 16>,
    server_params: Mutex<ServerParams>,
    pid: AtomicI32,
    secret: AtomicI32,
    #[allow(unused)]
    created_at: DateTime<Local>,
}

impl BackendConn {
    pub async fn connect(address: &SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(address).await?;
        Ok(Self::new(stream))
    }

    #[instrument]
    pub async fn run(&self) -> Result<()> {
        // XXX: This code is very similar to ClientConn::run.
        // If you change this, you probably need to change that too.

        let mut stream = MessageStream::new(self);
        let mut sender: Option<Arc<ClientConn>> = None;
        loop {
            // We don't want to clone the Arc everytime, so we clone() it once and cache it,
            // checking that it's still the current one with has_client. That's cheaper
            // than the atomic-read-modify-write ops used to increment and decrement and Arc.
            if sender.is_none() || !self.has_client(sender.as_ref().unwrap()) {
                sender = self.client();
            }
            let sender_ref = sender.as_ref().map(|arc| arc.as_ref());

            let msgs = stream.next(sender_ref).await?;
            backend_messages::run(self, sender_ref, msgs).await?;
        }
    }

    #[inline]
    pub async fn send(&self, msgs: Messages) -> Result<usize> {
        backend_send_messages::run(self, msgs, true).await
    }

    /// Dispatches msgs received from the database server to the client and/or backend requests (iterators).
    /// Safety: This can only be called from inside run(). It is not safe for use by other threads/tasks.
    #[instrument]
    pub async fn forward(&self, client: Option<&ClientConn>, mut msgs: Messages) -> Result<usize> {
        let mut sent = 0;

        let mut pending = self.pending_requests.load(Acquire);
        let pending_count = pending.count_ones();
        let mut requests_completed = 0;

    'Outer:
        while !msgs.is_empty() {
            if pending == 0 {
                // We don't have any requests in-flight, just forward the messages
                return if let Some(client) = client {
                    client.send(msgs).await
                } else {
                    warn!(?msgs, "dropping messages without client");
                    return Ok(0);
                };
            }

            let mut offset = 0;
            let mut wake = false;
            let mut pop = false;
            let request_type = pending & REQUEST_TYPE_MASK;
            println!("request type {} pending {}", request_type, pending);
            for msg in msgs.iter(0) {
                match msg.tag() {
                    Tag::ROW_DESCRIPTION => {
                        // If this is a backend request, this is a new rows result, wake the iterator
                        wake = request_type == BACKEND_REQUEST;
                    },
                    Tag::READY_FOR_QUERY => {
                        // If we didn't notify the iterator above to consume it's messages, now's the last chance
                        // This happens if the result wasn't a rows result, but just a command to execute.
                        requests_completed += 1;
                        let pending_original = pending;
                        pending >>= 2;
                        // Before we send the msgs, ensure we mark the request as processed
                        // So that if that fails we haven't done anything irreversible.
                        match self.pending_requests.compare_exchange(pending_original, pending, Release, Relaxed) {
                            Ok(_) => (),
                            Err(val) => {
                                pending = val;
                                continue 'Outer;
                            },
                        }

                        offset = msg.offset() + msg.len() as usize;
                        pop = request_type == BACKEND_REQUEST;
                        break;
                    }
                    _ => (),
                }
            }

            println!("split to {}", offset);
            let out = msgs.split_to(offset);
            if request_type == CLIENT_REQUEST {
                if let Some(client) = client {
                    sent += client.send(out).await?;
                } else {
                    warn!(msgs=?out, "dropping messages without client");
                }
            } else {
                debug_assert_eq!(request_type, BACKEND_REQUEST);

                if wake || pop {
                    println!("do wake pop={}", pop);
                    // Notify first, then put messages, otherwise put may block forever
                    if pop {
                        debug_assert!(!self.iterators.is_empty());
                        self.iterators.pop().await.notify_one();
                    } else {
                        self.iterators.peek().unwrap().notify_one();
                    }
                }
                println!("send msgs to iterators");
                self.iterator_messages.put(out).await;
            }

            if requests_completed != 0 && pending_count == requests_completed {
                // pending_requests has reached zero, we can maybe release the backend to the pool
                if let Some(client) = client {
                    self.session_idle(client).await?;
                }
            }
        }
        println!("returning from forward {} sent", sent);
        Ok(sent)
    }

    pub async fn test_auth<'a, 'b: 'a, 'c: 'a>(&'a self, user: &'b str, password: &'c str, pool: &'static ConnectionPool) -> Result<()> {
        self.start(user, password, pool).await?;

        debug_assert_eq!(self.state(), BackendState::Authentication);

        self.run_until_state(BackendState::Ready).await
    }

    pub async fn authenticate<'a>(&'a self, pool: &'static ConnectionPool) -> Result<()> {
        self.start(&pool.config.user, &pool.config.password, pool).await?;

        self.run_until_state(BackendState::Ready).await
    }

    async fn run_until_state(&self, state: BackendState) -> Result<()> {
        let mut stream = MessageStream::<Self, ClientConn>::new(self);
        while self.state() != state {
            let msgs = stream.next(None).await?;

            backend_messages::run(self, None, msgs).await?;
        }
        Ok(())
    }

    async fn start<'a, 'b: 'a, 'c: 'a>(&'a self, user: &'b str, password: &'c str, pool: &'static ConnectionPool) -> Result<()> {
        self.pool.store(Some(pool));

        let mut params = ServerParams::default();
        params.add("database".to_string(), pool.config.database.clone());
        params.add("user".to_string(), user.to_string());
        params.add("client_encoding".to_string(), "UTF8".to_string());
        // We can't customize the application_name at connection, which happens once.
        // We need to do it in check_health_and_set_role which happens for each session that uses the connection.
        params.add("application_name".to_string(), "riverdb".to_string());

        // Remember the user and password in the server_params, we'll need it during authentication
        // We'll overwrite them later when processing the server's startup response.
        {
            let mut server_params = self.server_params.lock().unwrap();
            server_params.add("password".to_string(), password.to_string());
            server_params.add("user".to_string(), user.to_string());
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
        let ssl_request = Messages::new(Bytes::from_static(SSL_REQUEST_MSG));

        self.transition(BackendState::SSLHandshake)?;
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

    pub async fn session_idle(&self, client: &ClientConn) -> Result<()> {
        if let Some(backend) = client.session_idle().await? {
            self.client.store(None);
            self.pool.load().unwrap().put(backend);
        }
        Ok(())
    }

    pub fn return_to_pool(&self, client: &ClientConn) {
        if let Some(backend) = client.release_backend() {
            self.client.store(None);
            self.pool.load().unwrap().put(backend);
        }
    }

    pub async fn check_health_and_set_role(&self, application_name: &str, role: &str) -> Result<()> {
        if self.state() == BackendState::InPool {
            self.transition(BackendState::Ready)?;
            self.added_to_pool.store(0, Relaxed);
        }

        // Safety: I don't know why this is required here. Rust bug?
        let role: &'static str = unsafe { change_lifetime(role) };
        let application_name: &'static str = unsafe { change_lifetime(application_name) };
        if role.is_empty() {
            self.execute(query!("SET application_name TO {}", application_name)).await?;
        } else {
            try_join!(
                self.execute(query!("SET ROLE {}", role)),
                self.execute(query!("SET application_name TO {}", application_name))
            )?;
            println!("after executes");
        }

        Ok(())
    }

    /// Issue a query and return a Rows iterator over the results. You must call Rows::next()
    /// until it returns false or Rows::finish() to consume the entire result, even if you
    /// don't intend to use it.
    #[must_use = "you must call Rows::next() until it returns false or Rows::finish() to consume the entire result"]
    pub async fn query<'a>(&'a self, escaped_query: Messages) -> Result<Rows<'a>> {
        if escaped_query.count() != 1 {
            return Err(Error::new("query expects exactly one Message"));
        }
        let notifier = self.iterators.put(Notify::new()).await;
        let rows = Rows::new(&self.iterator_messages, notifier);
        backend_send_messages::run(self, escaped_query, false).await?;
        Ok(rows)
    }

    /// Issue a command and wait for the result. If this is awaited with other query/execute
    /// futures then it will pipeline the queries. Returns the number of affected rows.
    pub async fn execute(&self, escaped_query: Messages) -> Result<i32> {
        if escaped_query.count() != 1 {
            return Err(Error::new("execute expects exactly one Message"));
        }
        let notifier = self.iterators.put(Notify::new()).await;
        let mut rows = Rows::new(&self.iterator_messages, notifier);
        backend_send_messages::run(self, escaped_query, false).await?;
        println!("before finish");
        rows.finish().await
    }

    pub fn state(&self) -> BackendState {
        self.state.get()
    }

    pub fn transition(&self, new_state: BackendState) -> Result<()> {
        self.state.transition(self, new_state)
    }

    /// Returns the associated ClientConn, if any.
    pub fn client(&self) -> Option<Arc<ClientConn>> {
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

    pub(crate) fn set_created_for_transaction(&self, value: bool) {
        self.for_transaction.store(value, Relaxed)
    }

    pub fn in_pool(&self) -> bool {
        if let BackendState::InPool = self.state() {
            debug_assert_ne!(self.added_to_pool.load(Relaxed), 0);
            true
        } else {
            false
        }
    }

    pub fn set_in_pool(&self) -> bool {
        // TODO if backend is in a transaction, we need to issue a ROLLBACK first

        // TODO issue RESET on connection

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

    pub fn pending_requests(&self) -> u32 {
        self.pending_requests.load(Relaxed).count_ones()
    }

    #[instrument]
    pub async fn backend_connected(&self, _: &mut backend_connected::Event, params: &mut ServerParams) -> Result<()> {
        let mut mb = MessageBuilder::new(Tag::UNTAGGED);
        mb.write_i32(PROTOCOL_VERSION);
        mb.write_params(params);
        mb.write_byte(0); // null-terminator at end of startup packet

        self.transition(BackendState::Authentication)?;

        self.send(mb.finish()).await?;
        Ok(())
    }

    #[instrument]
    pub async fn backend_messages(&self, _: &mut backend_messages::Event, client: Option<&ClientConn>, mut msgs: Messages) -> Result<()> {
        while !msgs.is_empty() {
            match self.state() {
                BackendState::StateInitial | BackendState::SSLHandshake => {
                    return Err(Error::new(format!("unexpected message for initial state: {:?}", msgs)));
                },
                BackendState::Authentication => {
                    let first = msgs.split_first();
                    assert!(!first.is_empty());
                    backend_authenticate::run(self, client, first).await?;
                },
                BackendState::Startup => {
                    let mut params = self.server_params.lock().unwrap();
                    for msg in msgs.iter(0) {
                        match msg.tag() {
                            Tag::PARAMETER_STATUS => {
                                let r = msg.reader();
                                let key = r.read_str()?;
                                let val = r.read_str()?;
                                params.set(key.to_string(), val.to_string());
                            },
                            Tag::BACKEND_KEY_DATA => {
                                let r = msg.reader();
                                // The mutex release will publish these writes
                                self.pid.store(r.read_i32(), Relaxed);
                                self.secret.store(r.read_i32(), Relaxed);
                            },
                            Tag::READY_FOR_QUERY => {
                                self.transition(BackendState::Ready)?;
                            },
                            Tag::ERROR_RESPONSE => {
                                return Err(Error::from(PostgresError::new(msgs.split_message(&msg))?));
                            },
                            _ => {
                                // Else ignore the message
                                error!(?msg, "ignoring unexpected message");
                            },
                        }
                    }
                    break;
                },
                BackendState::InPool => {
                    let mut params = self.server_params.lock().unwrap();
                    for msg in msgs.iter(0) {
                        match msg.tag() {
                            Tag::PARAMETER_STATUS => {
                                let r = msg.reader();
                                let key = r.read_str()?;
                                let val = r.read_str()?;
                                params.set(key.to_string(), val.to_string());
                            },
                            Tag::ERROR_RESPONSE => {
                                return Err(Error::from(PostgresError::new(msgs.split_message(&msg))?));
                            },
                            _ => {
                                // Else ignore the message
                                error!(?msg, "ignoring unexpected message");
                            },
                        }
                    }
                    break;
                },
                _ => {
                    // Forward the message to the client, if there is one
                    // Safety: this is safe to call from the run() thread, and backend_messages is called by run().
                    self.forward(client, msgs).await?;
                    // TODO else this is part of a query workflow, do what with it???
                    break;
                }
            }
        }
        Ok(())
    }

    #[instrument]
    pub async fn backend_authenticate(&self, _: &mut backend_authenticate::Event, client: Option<&ClientConn>, msgs: Messages) -> Result<()> {
        assert_eq!(msgs.count(), 1);

        let msg = msgs.first().unwrap(); // see assert above
        match msg.tag() {
            Tag::AUTHENTICATION_OK => {
                let (auth_type, salt) = {
                    let r = msg.reader();
                    let auth_type = r.read_i32();
                    if auth_type == 0 {
                        r.error()?;
                    }
                    let auth_type = AuthType::from(auth_type);
                    let salt = r.read_i32();
                    if salt == 0 && auth_type == AuthType::MD5 {
                        r.error()?;
                    }
                    (auth_type, salt)
                };
                let (user, password) = {
                    let server_params = self.server_params.lock().unwrap();
                    (server_params.get("user").expect("missing user").to_string(),
                     server_params.get("password").expect("missing password").to_string())
                };

                match auth_type {
                    AuthType::Ok => {
                        // Success!
                        self.transition(BackendState::Startup)
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
                Err(Error::from(PostgresError::new(msgs)?))
            },
            _ => Err(Error::new(format!("unexpected message {}", msg.tag())))
        }
    }

    #[instrument]
    pub async fn backend_send_messages(&self, _: &mut backend_send_messages::Event, msgs: Messages, from_client: bool) -> Result<usize> {
        if msgs.is_empty() {
            return Ok(0);
        }
        for msg in msgs.iter(0) {
            match msg.tag() {
                Tag::QUERY => { // TODO what other tags expect a response?
                    let request_flag = if from_client {
                        CLIENT_REQUEST
                    } else {
                        BACKEND_REQUEST
                    };
                    println!("queue {} request", request_flag);
                    let mut pending = self.pending_requests.load(Relaxed);
                    loop {
                        let pending_count = pending.count_ones();
                        if pending_count == MAX_PENDING_REQUESTS {
                            return Err(Error::new(format!("reached maximum number of pipelined requests {}", MAX_PENDING_REQUESTS)));
                        }
                        let val = pending | (request_flag << (pending_count*2));
                        match self.pending_requests.compare_exchange_weak(pending, val, Release, Relaxed) {
                            Ok(_) => break,
                            Err(val) => pending = val,
                        }
                    }
                },
                _ => (),
            }
        }
        self.write_or_buffer(msgs.into_bytes())
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
            client: AtomicArc::default(),
            send_backlog: Mutex::new(Default::default()),
            pool: AtomicRef::default(),
            pending_requests: AtomicU64::new(0),
            iterator_messages: MessageQueue::new(),
            iterators: SpscQueue::new(),
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
        if let BackendState::Closed = self.state() {
            true
        } else {
            false
        }
    }

    fn msg_is_allowed(&self, tag: Tag) -> Result<()> {
        if self.state.msg_is_allowed(tag) {
            Ok(())
        } else {
            Err(Error::new(format!("unexpected backend message {} for state {:?}", tag, self.state())))
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


define_event! {
    /// backend_connected is called when a new backend session is being established.
    ///     backend: &BackendConn : the event source handling the backend connection
    ///     params: &ServerParams : key-value pairs that will be passed to the connected backend in the startup message (including database and user)
    /// BackendConn::backend_connected is called by default and sends ServerParams in the startup message.
    /// If it returns an error, the associated session is terminated.
    backend_connected,
    (backend: &'a BackendConn, params: &'a mut ServerParams) -> Result<()>
}

define_event! {
    /// backend_message is called when Postgres message(s) are received in a backend db connection.
    ///     backend: &BackendConn : the event source handling the backend connection
    ///     client: Option<&'a ClientConn> : the associated client connection (if any)
    ///     msgs: protocol.Messages : the received message(s)
    /// BackendConn::backend_message is called by default and does further processing on the Message,
    /// including potentially forwarding it to associated client session. Symmetric with client_message.
    /// If it returns an error, the associated session is terminated.
    backend_messages,
    (backend: &'a BackendConn, client: Option<&'a ClientConn>, msgs: Messages) -> Result<()>
}


define_event! {
    /// backend_send_message is called to send a Message to a backend db connection.
    ///     backend: &BackendConn : the event source handling the backend connection
    ///     msgs: protocol.Messages : the message(s) to send
    ///     from_client: bool : true if any requests are from the client, false if from the "backend"
    ///                         (e.g. from query or execute methods on BackendConn)
    /// BackendConn::backend_send_message is called by default and sends the Messages to the db server.
    /// If it returns an error, the associated session is terminated.
    /// Returns the number of bytes actually written (not buffered.)
    backend_send_messages,
    (backend: &'a BackendConn, msgs: Messages, from_client: bool) -> Result<usize>
}


define_event! {
    /// backend_authenticate is called with each message(s) received from Postgres while in the Authentication state
    ///     backend: &BackendConn : the event source handling the backend connection
    ///     msgs: protocol.Messages : the message(s) received
    /// This may be invoked multiple times during the authentication process to support multi-step auth workflows.
    /// Call self.transition to BackendState::Startup when authentication has completed successfully or
    /// return an error.
    backend_authenticate,
    (backend: &'a BackendConn, client: Option<&ClientConn>, msgs: Messages) -> Result<()>
}