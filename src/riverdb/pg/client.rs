use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicPtr, fence};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release, AcqRel};
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

use bytes::Bytes;
use fnv::FnvHashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::net::TcpStream;
use tracing::{debug, error, info, instrument};
use rustls::{ClientConnection};

use crate::define_event;
use crate::riverdb::{Error, Result, common};
use crate::riverdb::worker::{Worker};
use crate::riverdb::pg::protocol::{
    Message, MessageReader, MessageParser, ServerParams, Tag, PostgresError,
    PROTOCOL_VERSION, SSL_REQUEST, AuthType, MessageBuilder, MessageErrorBuilder,
    error_codes, ErrorSeverity, SSL_ALLOWED, SSL_NOT_ALLOWED
};
use crate::riverdb::pg::{ClientConnState, BackendConn, Connection, TransactionType};
use crate::riverdb::server::Transport;
use crate::riverdb::server;
use crate::riverdb::pg::{PostgresCluster, ConnectionPool};
use crate::riverdb::pg::connection::{read_and_flush_backlog, Backlog};
use crate::riverdb::pg::backend_state::BackendState;
use crate::riverdb::pg::message_stream::MessageStream;
use crate::riverdb::pg::client_state::ClientState;
use crate::riverdb::pg::sql::{Query, QueryType};
use crate::riverdb::pg::PostgresReplicationGroup;
use crate::riverdb::common::{AtomicCell, AtomicRefCell, AtomicRef};
use crate::riverdb::config::{conf, TlsMode};


pub struct ClientConn {
    /// client_stream is a possibly uninitialized Transport, may check if client_id != 0 first
    transport: Transport,
    /// id is set once and then read-only. Starts as 0.
    id: AtomicU32,
    /// last-active is a course-grained monotonic clock that is advanced when data is received from the client
    last_active: AtomicU32,
    auth_type: AtomicCell<AuthType>,
    has_send_backlog: AtomicBool,
    state: ClientConnState,
    tx_type: AtomicCell<TransactionType>,
    backend: AtomicArc<BackendConn>,
    send_backlog: Backlog,
    cluster: AtomicRef<'static, PostgresCluster>,
    replication_group: AtomicRef<'static, PostgresReplicationGroup>, // the last PostgresReplicationGroup used
    pool: AtomicRef<'static, ConnectionPool>, // the last ConnectionPool used
    buffered: Mutex<Option<Message>>,
    connect_params: UnsafeCell<ServerParams>,
    salt: i32,
}

impl ClientConn {
    #[instrument]
    pub async fn run(&self) -> Result<()> {
        // XXX: This code is very similar to BackendConn::run.
        // If you change this, you probably need to change that too.

        let mut stream = MessageStream::new(self);
        let mut sender: Option<Arc<BackendConn>> = None;
        loop {
            // We don't want to clone the Arc everytime, so we clone() it once and cache it,
            // checking that it's still the current one with has_backend. That's cheaper
            // than the atomic-read-modify-write ops used increment and decrement and Arc.
            if sender.is_none() || !self.has_backend(sender.as_ref().unwrap()) {
                sender = self.backend();
            }
            let sender_ref = sender.as_ref().map(|arc| arc.as_ref());

            let msg = stream.next(sender_ref).await?;
            client_message::run(self, sender_ref, msg).await?;
        }
    }

    #[inline]
    pub async fn send(&self, msg: Message) -> Result<usize> {
        if msg.is_empty() {
            return Ok(0);
        }
        client_send_message::run(self, msg).await
    }

    /// Returns the associated BackendConn, if any.
    pub fn backend(&self) -> Option<Arc<BackendConn>> { self.backend.load() }

    /// Sets the associated BackendConn. Panics if called on a BackendConn.
    pub fn set_backend(&self, backend: Option<Arc<BackendConn>>) {
        self.backend.store(backend);
    }

    pub fn cluster(&self) -> Option<&'static PostgresCluster> {
        self.cluster.load()
    }

    pub fn set_cluster(&self, cluster: Option<&'static PostgresCluster>) {
        self.cluster.store(cluster);
    }

    pub fn replication_group(&self) -> Option<&'static PostgresReplicationGroup> {
        self.replication_group.load()
    }

    pub fn set_replication_group(&self, replication_group: Option<&'static PostgresReplicationGroup>) {
        self.replication_group.store(replication_group);
    }

    pub fn pool(&self) -> Option<&'static ConnectionPool> {
        self.pool.load()
    }

    pub fn set_pool(&self, pool: Option<&'static ConnectionPool>) {
        self.pool.store(pool);
    }

    pub fn connection_params(&self) -> &ServerParams {
        match self.state.get() {
            ClientState::StateInitial | ClientState::SSLHandshake => {
                panic!("can only access connection_params once in the Authentication or later states");
            },
            _ => (),
        }
        // Safety: we don't allow accessing params (we panic) if ClientState < ClientState::Authentication
        unsafe {
            &*self.connect_params.get()
        }
    }

    /// forwards msg to the backend via backend.send. If backend is None, runs client_connect_backend
    /// to acquire a backend connection. Panics unless in Ready, Transaction, or FailedTransaction states.
    pub async fn forward(&self, backend: Option<&Arc<BackendConn>>, msg: Message) -> Result<usize> {
        let query = Query::new(msg);
        client_query::run(self, backend, query).await
    }

    pub fn release_backend(&self) -> Option<Arc<BackendConn>> {
        match self.state.get() {
            ClientState::Ready => {
                self.backend.swap(None)
            },
            ClientState::Transaction => {
                // If we're in a transaction, we can only release the backend
                // if defer_begin is enabled and we still have the begin statement buffered.
                if conf().postgres.defer_begin && self.buffered.lock().unwrap().is_some() {
                    self.backend.swap(None)
                } else {
                    None
                }
            },
            _ => None,
        }
    }

    fn begins_transaction(&self, query: &Query) -> Result<bool> {
        match query.query_type {
            QueryType::Begin | QueryType::SetTransaction => {
                let tx_type = TransactionType::parse_from_query(query.normalized());
                if tx_type == TransactionType::Default {
                    // TODO use the highest default isolation level for the master nodes of the cluster
                }
                self.tx_type.store(tx_type);
                self.state.transition(self, ClientState::Transaction)?;
                Ok(true)
            },
            QueryType::Commit => {
                if query.normalized().contains("AND CHAIN") {
                    // Stay in the Transaction state
                    Ok(true)
                } else {
                    self.tx_type.store(TransactionType::None);
                    self.state.transition(self, ClientState::Ready)?;
                    Ok(false)
                }
            }
            _ => Ok(false),
        }
    }

    /// Sends a COMMAND_COMPLETE message. Command should usually be a single word that identifies the completed SQL command.
    /// For an INSERT command, the tag is INSERT 0 rows, where rows is the number of rows inserted.
    /// For a DELETE command, the tag is DELETE rows where rows is the number of rows deleted.
    /// For an UPDATE command, the tag is UPDATE rows where rows is the number of rows updated.
    /// For a SELECT or CREATE TABLE AS command, the tag is SELECT rows where rows is the number of rows retrieved.
    /// For a MOVE command, the tag is MOVE rows where rows is the number of rows the cursor's position has been changed by.
    /// For a FETCH command, the tag is FETCH rows where rows is the number of rows that have been retrieved from the cursor.
    /// For a COPY command, the tag is COPY rows where rows is the number of rows copied.
    async fn send_command_successful(&self, command: &str, tx_status: char) -> Result<usize> {
        let mut mb = MessageBuilder::new(Tag::COMMAND_COMPLETE);
        mb.write_str(command);
        mb.add_new(Tag::READY_FOR_QUERY);
        mb.write_byte(tx_status as u8);
        self.send(mb.finish()).await
    }

    #[instrument]
    async fn startup(&self, msg: Message) -> Result<()> {
        assert_eq!(msg.tag(), Tag::UNTAGGED); // was previously checked by msg_is_allowed
        let r = MessageReader::new(&msg);
        let protocol_version = r.read_i32();
        match protocol_version {
            PROTOCOL_VERSION => {
                let mut params= ServerParams::from_startup_message(&msg)?;
                let cluster = client_connected::run(self, params).await?;
                self.cluster.store(Some(cluster));
                Ok(())
            },
            SSL_REQUEST => self.ssl_handshake().await,
            _ => Err(Error::new(format!("{:?}: unsupported protocol {}", self, protocol_version)))
        }
    }

    #[instrument]
    async fn ssl_handshake(&self) -> Result<()> {
        let tls_mode = conf().postgres.client_tls;
        match tls_mode {
            TlsMode::Disabled | TlsMode::Invalid => {
                let n = self.write_or_buffer(Bytes::from_static(&[SSL_NOT_ALLOWED]))?;
                debug_assert_eq!(n, 1);
                Ok(())
            },
            _ => {
                let n = self.write_or_buffer(Bytes::from_static(&[SSL_ALLOWED]))?;
                debug_assert_eq!(n, 1);
                self.state.transition(self, ClientState::SSLHandshake)?;
                let tls_config = conf().postgres.tls_config.clone().unwrap();
                self.transport.upgrade_server(tls_config, tls_mode).await
            }
        }
    }

    #[instrument]
    pub async fn client_query(&self, _: &mut client_query::Event, backend: Option<&Arc<BackendConn>>, mut query: Query) -> Result<usize> {
        let begins_tx = self.begins_transaction(&query)?;

        let state = self.state.get();
        match state {
            ClientState::Transaction | ClientState::Ready => {
                if backend.is_none() && begins_tx {
                    {
                        let mut buffered = self.buffered.lock().unwrap();
                        if buffered.is_none() {
                            *buffered = Some(query.into_message());
                            return Ok(0);
                        }
                    }

                    // There shouldn't have been anything buffered, we received two begin statements back to back
                    // Behave the same as Postgres, give a warning and ignore the second one.
                    let msg = Message::new_warning(error_codes::ACTIVE_SQL_TRANSACTION, "there is already a transaction in progress");
                    self.send(msg).await?;
                    self.send_command_successful("BEGIN", 'T').await?;
                    return Ok(0);
                }
            },
            ClientState::FailedTransaction => {
                // Only ROLLBACK is permitted
                if query.query_type == QueryType::Rollback {
                    // We already rolled back the backend and returned it to the pool
                    self.state.transition(self, ClientState::Ready)?;

                    // Tell the client the command succeeded
                    self.send_command_successful("ROLLBACK", 'I').await?;
                } else {
                    let error_msg = "current transaction is aborted, commands ignored until end of transaction block";
                    let msg = Message::new_error(error_codes::IN_FAILED_SQL_TRANSACTION, error_msg);
                    self.send(msg).await?;
                }
                return Ok(0);
            }
            _ => panic!("forward called in unexpected state {:?}", state)
        };

        if backend.is_none() {
            let cluster = self.cluster.load().expect("missing cluster");
            let params = self.connection_params();
            let user = params.get("user").expect("missing user");
            let database = params.get("database").expect("missing database");
            let application_name = params.get("application_name").unwrap_or("riverdb");
            let tx_type = self.tx_type.load();
            let backend = client_connect_backend::run(self, cluster, application_name, user, database, tx_type, &mut query).await?;

            // If we have buffered messages, flush them now
            // TODO not necessarily if defer_begin is enabled
            let msg = self.buffered.lock().unwrap().take();
            if let Some(msg) = msg {
                backend.send(msg).await?;
            }

            let n = backend.send(query.into_message()).await?;
            self.set_backend(Some(backend));
            Ok(n)
        } else {
            backend.unwrap().send(query.into_message()).await
        }
    }

    #[instrument]
    pub async fn client_connect_backend<'a>(&'a self, _: &'a mut client_connect_backend::Event, cluster: &'static PostgresCluster, application_name: &'a str, user: &'a str, database: &'a str, tx_type: TransactionType, query: &'a mut Query) -> Result<Arc<BackendConn>> {
        let mut error_code = error_codes::CANNOT_CONNECT_NOW;
        let group = client_partition::run(self, cluster, application_name, user, database, tx_type, query).await?;
        if let Some(group) = group {
            self.set_replication_group(Some(group));
            let pool = if !group.has_query_replica() || tx_type != TransactionType::ReadOnly {
                group.master.load()
            } else {
                client_route_query::run(self, group, tx_type, query).await?
            };
            if let Some(pool) = pool {
                self.set_pool(Some(pool));
                let backend = pool.get(application_name, user, tx_type).await?;
                if let Some(backend) = backend {
                    return Ok(backend);
                }
                error_code = error_codes::CONFIGURATION_LIMIT_EXCEEDED;
            }
        }

        let error_msg = "no database available for query";
        self.send(Message::new_error(error_code, error_msg)).await?;
        Err(Error::new(error_msg))
    }

    #[instrument]
    pub async fn client_partition<'a>(&'a self, _: &'a mut client_partition::Event, cluster: &'static PostgresCluster, application_name: &'a str, user: &'a str, database: &'a str, tx_type: TransactionType, query: &'a mut Query) -> Result<Option<&'static PostgresReplicationGroup>> {
        Ok(cluster.get_by_database(database))
    }

    #[instrument]
    pub async fn client_route_query<'a>(&'a self, _: &'a mut client_route_query::Event, group: &'static PostgresReplicationGroup, _tx_type: TransactionType, _query: &'a mut Query) -> Result<Option<&'static ConnectionPool>> {
        Ok(group.master.load())
    }

    #[instrument]
    pub async fn client_auth_challenge(&self, _: &mut client_auth_challenge::Event, params: ServerParams) -> Result<AuthType> {
        let auth_type = if self.is_tls() {
            AuthType::ClearText
        } else {
            AuthType::MD5
        };

        // Safety: we don't allow accessing params (we panic) if ClientState < ClientState::Authentication
        unsafe {
            *self.connect_params.get() = params
        };
        self.state.transition(self, ClientState::Authentication)?;

        let mut mb = MessageBuilder::new(Tag::AUTHENTICATION_OK);
        mb.write_i32(auth_type.as_i32());
        if let AuthType::MD5 = auth_type {
            mb.write_i32(self.salt);
        }
        self.send(mb.finish()).await?;

        Ok(auth_type)
    }

    #[instrument]
    pub async fn client_authenticate(&self, _: &mut client_authenticate::Event, auth_type: AuthType, msg: Message) -> Result<()> {
        let params = self.connection_params();
        let cluster = self.cluster.load().expect("expected db_cluster to be set");

        match msg.tag() {
            Tag::PASSWORD_MESSAGE => {
                // user and database exist, see ServerParams::from_startup_message
                let user = params.get("user").expect("missing user");
                let database = params.get("database").expect("missing database");

                let group = cluster.get_by_database(database);
                if let Some(group) = group {
                    let pool = group.master.load();
                    if let Some(pool) = pool {
                        let password = if auth_type == AuthType::ClearText {
                            let r = MessageReader::new(&msg);
                            r.read_str()?
                        } else if user == pool.config.user {
                            pool.config.password.as_str()
                        } else {
                            // TODO confirm this is the right error code
                            let error_msg = format!("unless the user is the configured user, only clear text authentication is supported: {}@{}", user, database);
                            self.send(Message::new_error(error_codes::INVALID_AUTHORIZATION_SPECIFICATION, &error_msg)).await?;
                            return Err(Error::new(error_msg))
                        };

                        return if cluster.authenticate(user, password, pool).await? {
                            client_complete_startup::run(self, cluster).await?;
                            self.state.transition(self, ClientState::Ready)
                        } else {
                            let error_msg = format!("password authentication failed for user \"{}\"", user);
                            self.send(Message::new_error(error_codes::INVALID_PASSWORD, &error_msg)).await?;
                            Err(Error::new(error_msg))
                        };
                    }
                }

                let error_msg = format!("database \"{}\" does not exist", database);
                self.send(Message::new_error(error_codes::INVALID_CATALOG_NAME, &error_msg)).await?;
                Err(Error::new(error_msg))
            },
            _ => {
                Err(Error::new(format!("unexpected message {}", msg.tag())))
            }
        }
    }

    #[instrument]
    pub async fn client_complete_startup(&self, _: &mut client_complete_startup::Event, cluster: &PostgresCluster) -> Result<()> {
        let startup_params = cluster.get_startup_params();

        let mut mb = MessageBuilder::new(Tag::AUTHENTICATION_OK);
        mb.write_i32(AuthType::Ok.as_i32());

        for (key, value) in startup_params.iter() {
            mb.add_new(Tag::PARAMETER_STATUS);
            mb.write_str(key);
            mb.write_str(value);
        }

        mb.add_new(Tag::BACKEND_KEY_DATA);
        mb.write_i32(self.id.load(Relaxed) as i32);
        mb.write_i32(self.salt);

        mb.add_new(Tag::READY_FOR_QUERY);
        mb.write_byte('I' as u8);
        self.send(mb.finish()).await?;
        Ok(())
    }

    #[instrument]
    pub async fn client_connected(&self, _: &mut client_connected::Event, params: ServerParams) -> Result<&'static PostgresCluster> {
        if let Some(encoding) = params.get("client_encoding") {
            let enc = encoding.to_ascii_uppercase();
            if enc != "UTF8" && enc != "UTF-8" {
                error!(encoding, "client_encoding must be set to UTF8");
            }
        }

        let auth_type = client_auth_challenge::run(self, params).await?;
        self.auth_type.store(auth_type);

        Ok(PostgresCluster::singleton())
    }

    #[instrument]
    pub async fn client_message(&self, _: &mut client_message::Event, backend: Option<&BackendConn>, msg: Message) -> Result<()> {
        let state = self.state.get();
        match state {
            ClientState::StateInitial => {
                self.startup(msg).await
            },
            ClientState::Authentication => {
                let auth_type = self.auth_type.load();
                client_authenticate::run(self, auth_type, msg).await
            },
            ClientState::Ready | ClientState::Transaction | ClientState::FailedTransaction => {
                self.forward(backend, msg).await;
                Ok(())
            },
            ClientState::Closed => {
                Err(Error::closed())
            },
            _ => {
                let error_msg = format!("received unexpected {:?} message while in {:?}", msg.tag(), state);
                self.send(Message::new_error(error_codes::PROTOCOL_VIOLATION, &error_msg)).await?;
                Err(Error::new(error_msg))
            }
        }
    }

    #[instrument]
    pub async fn client_send_message(&self, _: &mut client_send_message::Event, msg: Message) -> Result<usize> {
        self.write_or_buffer(msg.into_bytes())
    }
}

impl server::Connection for ClientConn {
    fn new(stream: TcpStream) -> Self {
        ClientConn {
            transport: Transport::new(stream),
            id: Default::default(),
            last_active: Default::default(),
            auth_type: AtomicCell::default(),
            has_send_backlog: Default::default(),
            state: Default::default(),
            tx_type: AtomicCell::default(),
            backend: AtomicRefCell::default(),
            send_backlog: Mutex::new(VecDeque::new()),
            cluster: AtomicRef::default(),
            replication_group: AtomicRef::default(),
            pool: AtomicRef::default(),
            buffered: Mutex::new(None),
            connect_params: UnsafeCell::new(ServerParams::new()),
            salt: Worker::get().rand32() as i32
        }
    }

    fn id(&self) -> u32 {
        self.id.load(Relaxed)
    }

    fn set_id(&self, id: u32) {
        self.id.store(id, Relaxed);
    }

    fn last_active(&self) -> u32 {
        self.last_active.load(Relaxed)
    }

    fn close(&self) {
        self.state.transition(self, ClientState::Closed);
        self.transport.close();
    }
}

impl Connection for ClientConn {
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
        if let ClientState::Closed = self.state.get() {
            true
        } else {
            false
        }
    }

    fn msg_is_allowed(&self, tag: Tag) -> Result<()> {
        if self.state.msg_is_allowed(tag) {
            Ok(())
        } else {
            Err(Error::new(format!("unexpected client message {} for state {:?}", tag, self.state.get())))
        }
    }
}

impl Debug for ClientConn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "pg::ClientConn{{id: {}, state: {:?}}}",
             self.id.load(Relaxed),
             self.state))
    }
}

// Safety: we use an UnsafeCell, but access is controlled safely, see connection_params method for details.
unsafe impl Sync for ClientConn {}


/// client_connected is called when a new client session is being established.
///     client: &ClientConn : the event source handling the client connection
///     params: &ServerParams : key-value pairs passed by the connected client in the startup message (including database and user)
/// Returns the database cluster where the BackendConn will later be established (usually pool.get_cluster()).
/// ClientConn::client_connected is called by default and sends the authentication challenge in response.
/// If it returns an error, the associated session is terminated.
define_event!(client_connected, (client: &'a ClientConn, params: ServerParams) -> Result<&'static PostgresCluster>);

/// client_message is called when a Postgres protocol.Message is received in a client session.
///     client: &ClientConn : the event source handling the client connection
///     backend: Option<&'a Arc<BackendConn>> : the associated backend connection (if any)
///     msg: protocol.Message is the received protocol.Message
/// You can replace msg by creating and passing a new Message object to ev.next(...)
/// A Message may contain multiple wire protocol messages, see Message::next().
/// ClientConn::client_message is called by default and does further processing on the Message,
/// including potentially calling the higher-level client_query. Symmetric with backend_message.
/// If it returns an error, the associated session is terminated.
define_event!(client_message, (client: &'a ClientConn, backend: Option<&'a BackendConn>, msg: Message) -> Result<()>);

define_event!(client_query, (client: &'a ClientConn, backend: Option<&'a BackendConn>, query: Query) -> Result<usize>);

/// client_send_message is called to send a Message to a backend db connection.
///     client: &ClientConn : the event source handling the client connection
///     msg : protocol.Message is the protocol.Message to send
///     prefer_buffer: bool : passed to write_or_buffer, see docs for that method
/// You can replace msg by creating and passing a new Message object to ev.next(...)
/// A Message may contain multiple wire protocol messages, see Message::next().
/// ClientConn::client_send_message is called by default and sends the Message to the connected client.
/// If it returns an error, the associated session is terminated.
/// Returns the number of bytes actually written (not buffered.)
define_event!(client_send_message, (client: &'a ClientConn, msg: Message) -> Result<usize>);

define_event!(client_auth_challenge, (client: &'a ClientConn, params: ServerParams) -> Result<AuthType>);

define_event!(client_authenticate, (client: &'a ClientConn, auth_type: AuthType, msg: Message) -> Result<()>);

define_event!(client_complete_startup, (client: &'a ClientConn, cluster: &'static PostgresCluster) -> Result<()>);

define_event!(client_connect_backend, (
    client: &'a ClientConn,
    cluster: &'static PostgresCluster,
    application_name: &'a str,
    user: &'a str,
    database: &'a str,
    tx_type: TransactionType,
    query: &'a mut Query) -> Result<Arc<BackendConn>>);

define_event!(client_partition, (
    client: &'a ClientConn,
    cluster: &'static PostgresCluster,
    application_name: &'a str,
    user: &'a str,
    database: &'a str,
    tx_type: TransactionType,
    query: &'a mut Query) -> Result<Option<&'static PostgresReplicationGroup>>);

define_event!(client_route_query, (
    client: &'a ClientConn,
    group: &'static PostgresReplicationGroup,
    tx_type: TransactionType,
    query: &'a mut Query) -> Result<Option<&'static ConnectionPool>>);