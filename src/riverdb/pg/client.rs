use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicPtr};
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
use crate::riverdb::pg::sql::Query;
use crate::riverdb::pg::PostgresReplicationGroup;
use crate::riverdb::common::{AtomicCell, AtomicArc, AtomicRef};
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
    db_cluster: AtomicRef<'static, PostgresCluster>,
    db_replication_group: AtomicRef<'static, PostgresReplicationGroup>, // the last PostgresReplicationGroup used
    db_pool: AtomicRef<'static, ConnectionPool>, // the last ConnectionPool used
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
            // We don't want to clone the Arc everytime, so we clone() it once calling self.get_other_conn()
            // And then we cache that Arc, checking that it's still the current with self.has_other_conn()
            // Which is cheaper the the atomic-read-modify-write ops used increment and decrement and Arc.
            if sender.is_none() || !self.has_backend(sender.as_ref().unwrap()) {
                sender = self.get_backend();
            }
            let sender_ref = sender.as_ref().map(|arc| arc.as_ref());

            let msg = stream.next(sender_ref).await?;
            client_message::run(self, sender.as_ref(), msg).await?;
        }
    }

    #[inline]
    pub async fn send(&self, msg: Message, prefer_buffer: bool) -> Result<usize> {
        if msg.is_empty() {
            return Ok(0);
        }
        client_send_message::run(self, msg, prefer_buffer).await
    }

    /// Returns the associated BackendConn, if any.
    pub fn get_backend(&self) -> Option<Arc<BackendConn>> {
        self.backend.load()
    }

    /// Returns true if backend is set as the associated BackendConn.
    pub fn has_backend(&self, backend: &Arc<BackendConn>) -> bool {
        self.backend.is(backend)
    }

    /// Sets the associated BackendConn. Panics if called on a BackendConn.
    pub fn set_backend(&self, backend: Option<Arc<BackendConn>>) {
        self.backend.store(backend);
    }

    pub fn cluster(&self) -> Option<&'static PostgresCluster> {
        self.db_cluster.load()
    }

    pub fn set_cluster(&self, cluster: Option<&'static PostgresCluster>) {
        self.db_cluster.store(cluster);
    }

    pub fn replication_group(&self) -> Option<&'static PostgresReplicationGroup> {
        self.db_replication_group.load()
    }

    pub fn set_replication_group(&self, replication_group: Option<&'static PostgresReplicationGroup>) {
        self.db_replication_group.store(replication_group);
    }

    pub fn pool(&self) -> Option<&'static ConnectionPool> {
        self.db_pool.load()
    }

    pub fn set_pool(&self, pool: Option<&'static ConnectionPool>) {
        self.db_pool.store(pool);
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
        client_query::run(self, backend, Query::new(msg)).await
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
                self.db_cluster.store(Some(cluster));
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
                let n = self.write_or_buffer(Bytes::from_static(&[SSL_NOT_ALLOWED]), false)?;
                debug_assert_eq!(n, 1);
                Ok(())
            },
            _ => {
                let n = self.write_or_buffer(Bytes::from_static(&[SSL_ALLOWED]), false)?;
                debug_assert_eq!(n, 1);
                self.state.transition(self, ClientState::SSLHandshake)?;
                let tls_config = conf().postgres.tls_config.clone().unwrap();
                self.transport.upgrade_server(tls_config, tls_mode).await
            }
        }
    }

    #[instrument]
    pub async fn client_query(&self, _: &mut client_query::Event, backend: Option<&Arc<BackendConn>>, mut query: Query) -> Result<usize> {
        let state = self.state.get();
        let tx_type = match state {
            ClientState::Transaction => self.tx_type.load(),
            ClientState::Ready | ClientState::FailedTransaction => {
                // TODO does msg start a transaction?
                todo!()
            },
            _ => panic!("forward called in unexpected state {:?}", state)
        };

        if backend.is_none() {
            let cluster = self.db_cluster.load().expect("missing cluster");
            let params = self.connection_params();
            let user = params.get("user").expect("missing user");
            let database = params.get("database").expect("missing database");
            let application_name = params.get("application_name").unwrap_or("riverdb");
            let backend = client_connect_backend::run(self, cluster,application_name, user, database, tx_type, &mut query).await?;
            let n = backend.send(query.into_message(), false).await?;
            self.set_backend(Some(backend));
            Ok(n)
        } else {
            backend.unwrap().send(query.into_message(), false).await
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
        self.send(Message::new_error(error_code, error_msg), false).await?;
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
        self.send(mb.finish(), false).await?;

        Ok(auth_type)
    }

    #[instrument]
    pub async fn client_authenticate(&self, _: &mut client_authenticate::Event, auth_type: AuthType, msg: Message) -> Result<()> {
        let params = self.connection_params();
        let cluster = self.db_cluster.load().expect("expected db_cluster to be set");

        match msg.tag() {
            Tag::PASSWORD_MESSAGE => {
                // user and database exist, see ServerParams::from_startup_message
                let user = params.get("user").expect("missing user");
                let database = params.get("database").expect("missing database");

                let group = cluster.get_by_database(database);
                if let Some(group) = group {
                    let pool = group.master.load();
                    if let Some(pool) = pool {
                        let password = {
                            let r = MessageReader::new(&msg);
                            r.read_str()?
                        };
                        return if cluster.authenticate(user, password, pool).await? {
                            client_complete_startup::run(self, cluster).await?;
                            self.state.transition(self, ClientState::Ready)
                        } else {
                            let error_msg = format!("password authentication failed for user \"{}\"", user);
                            self.send(Message::new_error(error_codes::INVALID_PASSWORD, &error_msg), false).await?;
                            Err(Error::new(error_msg))
                        };
                    }
                }

                let error_msg = format!("database \"{}\" does not exist", database);
                self.send(Message::new_error(error_codes::INVALID_CATALOG_NAME, &error_msg), false).await?;
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
        self.send(mb.finish(), false).await?;
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
    pub async fn client_message(&self, _: &mut client_message::Event, backend: Option<&Arc<BackendConn>>, msg: Message) -> Result<()> {
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
                self.send(Message::new_error(error_codes::PROTOCOL_VIOLATION, &error_msg), false).await?;
                Err(Error::new(error_msg))
            }
        }
    }

    #[instrument]
    pub async fn client_send_message(&self, _: &mut client_send_message::Event, msg: Message, prefer_buffer: bool) -> Result<usize> {
        self.write_or_buffer(msg.into_bytes(), prefer_buffer)
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
            backend: Default::default(),
            send_backlog: Mutex::new(VecDeque::new()),
            db_cluster: AtomicRef::default(),
            db_replication_group: AtomicRef::default(),
            db_pool: AtomicRef::default(),
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
define_event!(client_message, (client: &'a ClientConn, backend: Option<&'a Arc<BackendConn>>, msg: Message) -> Result<()>);

define_event!(client_query, (client: &'a ClientConn, backend: Option<&'a Arc<BackendConn>>, query: Query) -> Result<usize>);

/// client_send_message is called to send a Message to a backend db connection.
///     client: &ClientConn : the event source handling the client connection
///     msg : protocol.Message is the protocol.Message to send
///     prefer_buffer: bool : passed to write_or_buffer, see docs for that method
/// You can replace msg by creating and passing a new Message object to ev.next(...)
/// A Message may contain multiple wire protocol messages, see Message::next().
/// ClientConn::client_send_message is called by default and sends the Message to the connected client.
/// If it returns an error, the associated session is terminated.
/// Returns the number of bytes actually written (not buffered.)
define_event!(client_send_message, (client: &'a ClientConn, msg: Message, prefer_buffer: bool) -> Result<usize>);

define_event!(client_auth_challenge, (client: &'a ClientConn, params: ServerParams) -> Result<AuthType>);

define_event!(client_authenticate, (client: &'a ClientConn, auth_type: AuthType, msg: Message) -> Result<()>);

define_event!(client_complete_startup, (client: &'a ClientConn, cluster: &'static PostgresCluster) -> Result<()>);

define_event!(client_connect_backend, (client: &'a ClientConn, cluster: &'static PostgresCluster, application_name: &'a str, user: &'a str, database: &'a str, tx_type: TransactionType, query: &'a mut Query) -> Result<Arc<BackendConn>>);

define_event!(client_partition, (client: &'a ClientConn, cluster: &'static PostgresCluster, application_name: &'a str, user: &'a str, database: &'a str, tx_type: TransactionType, query: &'a mut Query) -> Result<Option<&'static PostgresReplicationGroup>>);

define_event!(client_route_query, (client: &'a ClientConn, group: &'static PostgresReplicationGroup, tx_type: TransactionType, query: &'a mut Query) -> Result<Option<&'static ConnectionPool>>);