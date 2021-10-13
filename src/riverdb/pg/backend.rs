use std::sync::{Mutex, MutexGuard};
use std::sync::atomic::{AtomicU32, AtomicBool, AtomicI32, AtomicU64};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release};
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::pin::Pin;
use std::cell::UnsafeCell;
use std::convert::TryFrom;

use chrono::{Local, DateTime};
use tokio::net::TcpStream;
use tokio::io::Interest;
use tokio::sync::Notify;
use tracing::{error, warn, debug, instrument};
use bytes::Bytes;

use crate::{define_event, query};
use crate::riverdb::{config, Error, Result};
use crate::riverdb::config::TlsMode;
use crate::riverdb::pg::{BackendConnState, ClientConn, Connection, ConnectionPool, Rows, parse_messages};
use crate::riverdb::server::{Transport, Connection as ServerConnection, Connections};
use crate::riverdb::server;
use crate::riverdb::pg::connection::{Backlog, RefcountAndFlags};
use crate::riverdb::pg::backend_state::{BackendState, StateEnum};
use crate::riverdb::common::{SpscQueue, AtomicRef, coarse_monotonic_now, change_lifetime, AtomicRefCounted, Ark};
use crate::riverdb::pg::protocol::{
    ServerParams, Messages, MessageBuilder, MessageParser, Tag, SSL_ALLOWED, PROTOCOL_VERSION,
    AuthType, PostgresError, hash_md5_password, Message, sasl,
};


const MAX_PENDING_REQUESTS: u32 = 32;
const CLIENT_REQUEST: u64 = 1;
const BACKEND_REQUEST: u64 = 2;
const REQUEST_TYPE_MASK: u64 = 3;

pub type MessageQueue = SpscQueue<Messages, 32>;

pub struct BackendConn {
    stream: Transport,
    parser: UnsafeCell<MessageParser>,
    /// id is set once and then read-only. Starts as 0.
    id: AtomicU32,
    /// added_to_pool is a course-grained monotonic clock that is 0, or records when this was returned to the pool
    added_to_pool: AtomicU32,
    refcount_and_flags: RefcountAndFlags,
    for_transaction: AtomicBool,
    state: BackendConnState,
    client: Ark<ClientConn>,
    send_backlog: Backlog,
    pool: AtomicRef<'static, ConnectionPool>,
    pending_requests: AtomicU64, // a bitfield identifying client and backend (iterator) requests
    iterator_messages: MessageQueue, // messages queued for Rows iterators
    iterators: SpscQueue<usize, 16>, // rust doesn't allow a pointer type here (*const Notify is not Send, despite Send being implemented for SPSC)
    server_params: Mutex<ServerParams>,
    pid: AtomicI32,
    secret: AtomicI32,
    #[allow(unused)]
    created_at: DateTime<Local>,
    connections: &'static Connections<BackendConn>,
}

impl BackendConn {
    pub async fn connect(address: &SocketAddr, connections: &'static Connections<Self>) -> Result<Self> {
        let stream = TcpStream::connect(address).await?;
        Ok(Self::new(stream, connections))
    }

    #[instrument]
    pub async fn run(&self) -> Result<()> {
        // XXX: This code is very similar to ClientConn::run_inner.
        // If you change this, you probably need to change that too.

        loop {
            // Safety: we only access self.stream from this thread
            // Safety: we only access self.stream from this thread
            let msgs = unsafe { self.recv().await? };
            backend_messages::run(self, msgs).await?;
        }
    }

    unsafe fn parser(&self) -> &mut MessageParser {
        &mut *self.parser.get()
    }

    /// recv parses some Messages from the stream.
    /// Safety: recv can only be called from the run thread, only from inside
    /// methods called directly or indirectly by self.run(). Marked as unsafe
    /// because the programmer must enforce that constraint.
    #[inline]
    pub async unsafe fn recv(&self) -> Result<Messages> {
        let parser = self.parser();
        parse_messages(parser, self, self.client(), false).await
    }

    /// recv_one parses a single Message from the stream.
    /// Safety: recv_one can only be called from the run thread, only from inside
    /// methods called directly or indirectly by self.run(). Marked as unsafe
    /// because the programmer must enforce that constraint.
    #[inline]
    pub async unsafe fn recv_one(&self) -> Result<Messages> {
        let parser = self.parser();
        parse_messages(parser, self, self.client(), true).await
    }

    #[inline]
    pub async fn send(&self, msgs: Messages) -> Result<usize> {
        backend_send_messages::run(self, msgs, true).await
    }

    /// Dispatches msgs received from the database server to the client and/or backend requests (iterators).
    /// Safety: This can only be called from inside run(). It is not safe for use by other threads/tasks.
    #[instrument]
    pub async fn forward(&self, mut msgs: Messages) -> Result<usize> {
        let mut sent = 0;
        let client = self.client();
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

            // If we don't find READY_FOR_QUERY, take all messages
            let mut offset = msgs.len() as usize;
            let mut wake = false;
            let mut pop = false;
            let request_type = pending & REQUEST_TYPE_MASK;
            for msg in msgs.iter(0) {
                match msg.tag() {
                    Tag::ROW_DESCRIPTION => {
                        debug!("forward ROW_DESCRIPTION");
                        // If this is a backend request, this is a new rows result, wake the iterator
                        wake = request_type == BACKEND_REQUEST;
                    },
                    Tag::READY_FOR_QUERY => {
                        debug!("forward READY_FOR_QUERY");
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
                        // If we didn't notify the iterator above to consume it's messages, now's the last chance
                        pop = request_type == BACKEND_REQUEST;
                        wake = pop;
                        break;
                    }
                    _ => (),
                }
            }

            debug!("split to {} out of {} for {}", offset, msgs.len(), if request_type == CLIENT_REQUEST {"client request"} else {"backend request"});
            let out = msgs.split_to(offset);
            if request_type == CLIENT_REQUEST {
                if let Some(client) = client {
                    sent += client.send(out).await?;
                } else {
                    warn!(msgs=?out, "dropping messages without client");
                }
            } else {
                debug_assert_eq!(request_type, BACKEND_REQUEST);

                // Notify first, then put messages, otherwise put may block forever
                if wake {
                    debug_assert!(!self.iterators.is_empty());
                    let notifier = if pop {
                        self.iterators.pop_now()
                    } else {
                        *self.iterators.peek().unwrap()
                    } as *const Notify;
                    // Safety: dereferencing a valid pointer, if the Rows object was dropped it would have panicked
                    unsafe { &*notifier }.notify_one();
                }
                self.iterator_messages.put(out).await;
            }

            if requests_completed != 0 && pending_count == requests_completed {
                // pending_requests has reached zero, we can maybe release the backend to the pool
                if let Some(client) = client {
                    self.session_idle(client).await?;
                }
            }
        }
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
        while self.state() != state {
            // Safety: This method is not called concurrently with run()
            let msgs = unsafe { self.recv().await? };
            backend_messages::run(self, msgs).await?;
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

        self.stream.ready(Interest::READABLE).await?;
        let mut buf: [u8; 1] = [0];
        let n = self.stream.try_read(&mut buf[..])?;
        if n == 1 {
            if buf[0] == SSL_ALLOWED {
                let tls_config = cluster.backend_tls_config.clone().unwrap();
                self.stream.upgrade_client(tls_config, cluster.backend_tls, pool.config.tls_host.as_str()).await
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
        let conn = client.session_idle().await?;
        Self::return_to_pool(conn).await;
        Ok(())
    }

    pub async fn return_to_pool(this: Ark<Self>) {
        if let Some(backend) = this.load() {
            backend.client.store(Ark::default());
            backend.pool.load().unwrap().put(this).await;
        }
    }

    /// Reset the connection prior to returning it to the pool
    pub async fn reset(&self) -> Result<()> {
        // TODO(optimization) track how SET was used and if there's nothing to reset, no need to call RESET ALL

        let reset = if self.state().is_transaction() {
            query!("ROLLBACK; RESET ROLE; RESET ALL",)
        } else {
            query!("RESET ROLE; RESET ALL",)
        };

        self.execute(reset).await?;
        Ok(())
    }

    pub async fn check_health_and_set_role(&self, application_name: &str, role: &str) -> Result<()> {
        if self.state() == BackendState::InPool {
            self.transition(BackendState::Ready)?;
            self.added_to_pool.store(0, Relaxed);
        }

        // Safety: I don't know why this is required here. Rust bug?
        let role: &'static str = unsafe { change_lifetime(role) };
        let application_name: &'static str = unsafe { change_lifetime(application_name) };
        let check = if role.is_empty() {
            query!("SET application_name TO {}", application_name)
        } else {
            query!("SET ROLE {}; SET application_name TO {}", role, application_name)
        };

        self.execute(check).await?;
        Ok(())
    }

    /// Issue a query and return a Rows iterator over the results. You must call Rows::next()
    /// until it returns false or Rows::finish() to consume the entire result, even if you
    /// don't intend to use it.
    #[must_use = "you must call Rows::next() until it returns false or Rows::finish() to consume the entire result"]
    pub async fn query<'a>(&'a self, escaped_query: Messages) -> Result<Pin<Box<Rows<'a>>>> {
        if escaped_query.count() != 1 {
            return Err(Error::new("query expects exactly one Message"));
        }
        let rows = Box::pin(Rows::new(self));
        let notifier = rows.as_ref().notifier() as usize;
        self.iterators.put(notifier as usize).await;
        backend_send_messages::run(self, escaped_query, false).await?;
        Ok(rows)
    }

    /// Issue a command and wait for the result. If this is awaited with other query/execute
    /// futures then it will pipeline the queries. Returns the number of affected rows.
    pub async fn execute(&self, escaped_query: Messages) -> Result<i32> {
        let mut rows = self.query(escaped_query).await?;
        rows.finish().await
    }

    pub fn state(&self) -> BackendState {
        self.state.get()
    }

    pub fn transition(&self, new_state: BackendState) -> Result<()> {
        self.state.transition(self, new_state)
    }

    /// Returns the associated ClientConn, if any.
    pub fn client(&self) -> Option<&ClientConn> {
        self.client.load()
    }

    /// Sets the associated ClientConn.
    pub fn set_client(&self, client: Ark<ClientConn>) {
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
        // See ConnectionPool::put, which calls reset() before this.
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

    pub(crate) async fn iterator_messages(&self) -> Messages {
        self.iterator_messages.pop().await
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
    pub async fn backend_messages(&self, _: &mut backend_messages::Event, mut msgs: Messages) -> Result<()> {
        while !msgs.is_empty() {
            match self.state() {
                BackendState::StateInitial | BackendState::SSLHandshake => {
                    return Err(Error::new(format!("unexpected message for initial state: {:?}", msgs)));
                },
                BackendState::Authentication => {
                    let first = msgs.split_first();
                    assert!(!first.is_empty());
                    backend_authenticate::run(self, first).await?;
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
                    self.forward(msgs).await?;
                    // TODO else this is part of a query workflow, do what with it???
                    break;
                }
            }
        }
        Ok(())
    }

    #[instrument]
    pub async fn backend_authenticate(&self, _: &mut backend_authenticate::Event, msgs: Messages) -> Result<()> {
        assert_eq!(msgs.count(), 1);

        let msg = msgs.first().unwrap(); // see assert above
        match msg.tag() {
            Tag::AUTHENTICATION_OK => {
                let auth_type = {
                    let r = msg.reader();
                    let auth_type = r.read_i32();
                    if auth_type == 0 {
                        r.error()?;
                    }
                    AuthType::try_from(auth_type)?
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
                        let password_msg = {
                            let r = msg.reader();
                            r.advance(4)?; // skip over auth_type
                            let salt = r.read_i32();
                            if salt == 0 {
                                r.error()?;
                            }
                            let md5_password = hash_md5_password(&user, &password, salt);
                            let mut mb = MessageBuilder::new(Tag::PASSWORD_MESSAGE);
                            mb.write_str(&md5_password);
                            mb.finish()
                        };
                        self.send(password_msg).await?;
                        Ok(())
                    },
                    AuthType::SASL => {
                        self.sasl_auth(msg, user, password).await
                    },
                    _ => Err(Error::new(format!("unsupported authentication scheme (use SASL, MD5, or plaintext over SSL) {}", auth_type)))
                }
            },
            Tag::ERROR_RESPONSE => {
                Err(Error::from(PostgresError::new(msgs)?))
            },
            _ => Err(Error::new(format!("unexpected message {}", msg.tag())))
        }
    }

    pub async fn sasl_auth(&self, msg: Message<'_>, _user: String, password: String) -> Result<()> {
        let mut have_scram_256 = false;
        let mut have_scram_256_plus = false;

        {
            let r = msg.reader();
            r.advance(4)?; // skip auth_type
            loop {
                let mechanism = r.read_str()?;
                if mechanism.is_empty() {
                    break;
                }
                match mechanism {
                    sasl::SCRAM_SHA_256 => have_scram_256 = true,
                    sasl::SCRAM_SHA_256_PLUS => have_scram_256_plus = true,
                    _ => (),
                }
            }
        }

        // TODO support channel binding for better security when possible
        let tls_endpoint = vec![];

        let (channel_binding, mechanism) = if have_scram_256_plus {
            if tls_endpoint.is_empty() {
                (sasl::ChannelBinding::unsupported(), sasl::SCRAM_SHA_256)
            } else {
                (sasl::ChannelBinding::tls_server_end_point(tls_endpoint), sasl::SCRAM_SHA_256_PLUS)
            }
        } else if have_scram_256 {
            (sasl::ChannelBinding::unrequested(), sasl::SCRAM_SHA_256)
        } else {
            return Err(Error::new("unsupported SASL mechanism"));
        };

        let mut scram = sasl::ScramSha256::new(password.as_bytes(), channel_binding);
        let sasl_initial = {
            let message_data = scram.message();
            let mut mb = MessageBuilder::new(Tag::PASSWORD_MESSAGE);
            mb.write_str(mechanism);
            mb.write_i32(message_data.len() as i32);
            mb.write_bytes(message_data);
            mb.finish()
        };
        self.send(sasl_initial).await?;

        // Safety: this is called indirectly from inside run()
        let msgs = unsafe { self.recv_one() }.await?;
        scram.update_from_message(msgs)?;

        let sasl_continue = {
            let mut mb = MessageBuilder::new(Tag::PASSWORD_MESSAGE);
            mb.write_bytes(scram.message());
            mb.finish()
        };
        self.send(sasl_continue).await?;

        let mut msgs = unsafe { self.recv_one() }.await?;
        let final_msg = msgs.split_first();
        scram.update_from_message(final_msg)?;

        Ok(())
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

impl AtomicRefCounted for BackendConn {
    fn refcount(&self) -> u32 {
        self.refcount_and_flags.refcount()
    }

    fn incref(&self) {
        self.refcount_and_flags.incref();
    }

    fn decref(&self) -> bool {
        if self.refcount_and_flags.decref() {
            self.connections.remove(self, self.id());
            true
        } else {
            false
        }
    }
}

impl server::Connection for BackendConn {
    fn new(stream: TcpStream, connections: &'static Connections<Self>) -> Self {
        BackendConn {
            stream: Transport::new(stream),
            parser: UnsafeCell::new(MessageParser::new()),
            id: Default::default(),
            added_to_pool: Default::default(),
            refcount_and_flags: RefcountAndFlags::new(),
            for_transaction: Default::default(),
            state: Default::default(),
            client: Ark::default(),
            send_backlog: Mutex::new(Default::default()),
            pool: AtomicRef::default(),
            pending_requests: AtomicU64::new(0),
            iterator_messages: MessageQueue::new(),
            iterators: SpscQueue::new(),
            server_params: Mutex::new(ServerParams::default()),
            pid: AtomicI32::new(0),
            secret: AtomicI32::new(0),
            created_at: Local::now(),
            connections,
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
        self.stream.close();
    }
}

impl Connection for BackendConn {
    fn has_backlog(&self) -> bool {
        self.refcount_and_flags.has(RefcountAndFlags::HAS_BACKLOG)
    }

    fn set_has_backlog(&self, value: bool) {
        self.refcount_and_flags.set(RefcountAndFlags::HAS_BACKLOG, value);
    }

    fn backlog(&self) -> &Mutex<VecDeque<Bytes>> {
        &self.send_backlog
    }

    fn transport(&self) -> &Transport {
        &self.stream
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

// Safety: we use an UnsafeCell, but access is controlled safely, see recv method for details.
unsafe impl Send for BackendConn {}
unsafe impl Sync for BackendConn {}


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
    ///     msgs: protocol.Messages : the received message(s)
    /// BackendConn::backend_message is called by default and does further processing on the Message,
    /// including potentially forwarding it to associated client session. Symmetric with client_message.
    /// If it returns an error, the associated session is terminated.
    backend_messages,
    (backend: &'a BackendConn, msgs: Messages) -> Result<()>
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
    (backend: &'a BackendConn, msgs: Messages) -> Result<()>
}