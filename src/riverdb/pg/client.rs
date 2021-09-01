use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU32};
use std::sync::atomic::Ordering::{Relaxed};
use std::fmt::{Debug, Formatter};
use std::sync::{Mutex};
use std::collections::VecDeque;

use bytes::Bytes;

use tokio::net::TcpStream;
use tracing::{error, instrument};


use crate::define_event;
use crate::riverdb::{Error, Result};
use crate::riverdb::worker::{Worker};
use crate::riverdb::pg::protocol::{
    Messages, ServerParams, Tag,
    PROTOCOL_VERSION, SSL_REQUEST, AuthType, MessageBuilder,
    error_codes, SSL_ALLOWED, SSL_NOT_ALLOWED
};
use crate::riverdb::pg::{ClientConnState, BackendConn, Connection, TransactionType};
use crate::riverdb::server::{Transport, Connections, Connection as ServerConnection};

use crate::riverdb::pg::{PostgresCluster, ConnectionPool};
use crate::riverdb::pg::connection::{Backlog, RefcountAndFlags};

use crate::riverdb::pg::message_stream::MessageStream;
use crate::riverdb::pg::client_state::ClientState;
use crate::riverdb::pg::sql::{Query, QueryType};
use crate::riverdb::pg::PostgresReplicationGroup;
use crate::riverdb::common::{AtomicCell, AtomicRef, Ark, AtomicRefCounted};
use crate::riverdb::config::{conf, TlsMode};


pub struct ClientConn {
    /// client_stream is a possibly uninitialized Transport, may check if client_id != 0 first
    transport: Transport,
    /// id is set once and then read-only. Starts as 0.
    id: AtomicU32,
    /// last-active is a course-grained monotonic clock that is advanced when data is received from the client
    last_active: AtomicU32,
    auth_type: AtomicCell<AuthType>,
    refcount_and_flags: RefcountAndFlags,
    state: ClientConnState,
    tx_type: AtomicCell<TransactionType>,
    backend: Ark<BackendConn>,
    send_backlog: Backlog,
    cluster: AtomicRef<'static, PostgresCluster>,
    replication_group: AtomicRef<'static, PostgresReplicationGroup>, // the last PostgresReplicationGroup used
    pool: AtomicRef<'static, ConnectionPool>, // the last ConnectionPool used
    buffered: Mutex<Option<Messages>>,
    connect_params: UnsafeCell<ServerParams>,
    salt: i32,
    connections: &'static Connections<ClientConn>,
}

impl ClientConn {
    #[instrument]
    pub async fn run(&self) -> Result<()> {
        // XXX: This code is very similar to BackendConn::run.
        // If you change this, you probably need to change that too.

        let mut stream = MessageStream::new(self);
        loop {
            let msgs = stream.next(self.backend()).await?;
            client_messages::run(self, msgs).await?;
        }
    }

    #[inline]
    pub async fn send(&self, msgs: Messages) -> Result<usize> {
        if msgs.is_empty() {
            return Ok(0);
        }
        client_send_messages::run(self, msgs).await
    }

    pub fn state(&self) -> ClientState {
        self.state.get()
    }

    pub fn transition(&self, new_state: ClientState) -> Result<()> {
        self.state.transition(self, new_state)
    }

    /// Returns the associated BackendConn, if any.
    pub fn backend(&self) -> Option<&BackendConn> { self.backend.load() }

    /// Sets the associated BackendConn. Panics if called on a BackendConn.
    pub fn set_backend(&self, backend: Ark<BackendConn>) {
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

    /// For each Message in msgs, constructs a Query object and runs client_query.
    /// Which forwards the Query or Message to the backend via backend.send.
    /// If backend is None, runs client_connect_backend to acquire a backend connection.
    /// Panics unless in Ready, Transaction, or FailedTransaction states.
    #[instrument]
    pub async fn forward(&self, msgs: Messages) -> Result<()> {
        for msg in msgs.iter(0) {
            match msg.tag() {
                Tag::QUERY => {
                    // TODO we could implement Query<'a> with Message instead?
                    // TODO can we still issue a bulk send here if Query is unaltered? This is the performance sensitive part
                    let query = Query::new(msgs.split_message(&msg));
                    client_query::run(self, query).await?;
                },
                Tag::TERMINATE => {
                    self.transition(ClientState::Closed)?;
                    let backend = self.backend();
                    if backend.is_some() {
                        BackendConn::return_to_pool(self.release_backend());
                    }
                    self.transport.close();
                    break;
                },
                _ => {
                    todo!();
                }
            }
        }
        Ok(())
    }

    pub async fn session_idle(&self) -> Result<Ark<BackendConn>> {
        client_idle::run(self).await
    }

    #[instrument]
    pub fn release_backend(&self) -> Ark<BackendConn> {
        match self.state.get() {
            ClientState::Ready | ClientState::Closed => {
                return self.backend.swap(Ark::default());
            },
            ClientState::Transaction => {
                // If we're in a transaction, we can only release the backend
                // if defer_begin is enabled and we still have the begin statement buffered.
                if conf().postgres.defer_begin && self.buffered.lock().unwrap().is_some() {
                    return self.backend.swap(Ark::default());
                }
            },
            _ => (),
        }
        Ark::default()
    }

    fn begins_transaction(&self, query: &Query) -> Result<bool> {
        match query.query_type() {
            QueryType::Begin | QueryType::SetTransaction => {
                let tx_type = TransactionType::parse_from_query(query.normalized());
                if tx_type == TransactionType::Default {
                    // TODO use the highest default isolation level for the master nodes of the cluster
                }
                self.tx_type.store(tx_type);
                self.transition(ClientState::Transaction)?;
                Ok(true)
            },
            QueryType::Commit => {
                if query.normalized().contains("AND CHAIN") {
                    // Stay in the Transaction state
                    Ok(true)
                } else {
                    self.tx_type.store(TransactionType::None);
                    self.transition(ClientState::Ready)?;
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
    async fn startup(&self, msgs: Messages) -> Result<()> {
        if msgs.count() != 1 {
            return Err(Error::new("startup expects exactly one Message"));
        }

        let msg = msgs.first().unwrap(); // see msgs.count() condition above
        assert_eq!(msg.tag(), Tag::UNTAGGED); // was previously checked by msg_is_allowed
        let r = msg.reader();
        let protocol_version = r.read_i32();
        match protocol_version {
            PROTOCOL_VERSION => {
                let params= ServerParams::from_startup_message(&msg)?;
                let cluster = client_connected::run(self, params).await?;
                self.set_cluster(Some(cluster));
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
                self.transition(ClientState::SSLHandshake)?;
                let tls_config = conf().postgres.tls_config.clone().unwrap();
                self.transport.upgrade_server(tls_config, tls_mode).await
            }
        }
    }

    #[instrument]
    pub async fn client_query(&self, _: &mut client_query::Event, mut query: Query) -> Result<()> {
        let begins_tx = self.begins_transaction(&query)?;
        let backend = self.backend();

        let state = self.state.get();
        match state {
            ClientState::Transaction | ClientState::Ready => {
                if backend.is_none() && begins_tx {
                    {
                        let mut buffered = self.buffered.lock().unwrap();
                        if buffered.is_none() {
                            *buffered = Some(query.into_messages());
                            return Ok(());
                        }
                    }

                    // There shouldn't have been anything buffered, we received two begin statements back to back
                    // Behave the same as Postgres, give a warning and ignore the second one.
                    let msg = Messages::new_warning(error_codes::ACTIVE_SQL_TRANSACTION, "there is already a transaction in progress");
                    self.send(msg).await?;
                    self.send_command_successful("BEGIN", 'T').await?;
                    return Ok(());
                }
            },
            ClientState::FailedTransaction => {
                // Only ROLLBACK is permitted
                if query.query_type() == QueryType::Rollback {
                    // We already rolled back the backend and returned it to the pool
                    self.transition(ClientState::Ready)?;

                    // Tell the client the command succeeded
                    self.send_command_successful("ROLLBACK", 'I').await?;
                } else {
                    let error_msg = "current transaction is aborted, commands ignored until end of transaction block";
                    let msg = Messages::new_error(error_codes::IN_FAILED_SQL_TRANSACTION, error_msg);
                    self.send(msg).await?;
                }
                return Ok(());
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
            let backend_ark = client_connect_backend::run(self, cluster, application_name, user, database, tx_type, &mut query).await?;
            // If we have buffered messages, flush them now
            // TODO not necessarily if defer_begin is enabled
            let msgs = self.buffered.lock().unwrap().take();
            if let Some(msgs) = msgs {
                backend_ark.send(msgs).await?;
            }

            backend_ark.send(query.into_messages()).await?;
            self.set_backend(backend_ark);
        } else {
            backend.unwrap().send(query.into_messages()).await?;
        }
        Ok(())
    }

    #[instrument]
    pub async fn client_connect_backend<'a>(&'a self, _: &'a mut client_connect_backend::Event, cluster: &'static PostgresCluster, application_name: &'a str, user: &'a str, database: &'a str, tx_type: TransactionType, query: &'a mut Query) -> Result<Ark<BackendConn>> {
        let mut error_code = error_codes::CANNOT_CONNECT_NOW;
        let group = client_partition::run(self, cluster, application_name, user, database, tx_type, query).await?;
        if let Some(group) = group {
            self.set_replication_group(Some(group));
            let pool = if !group.has_query_replica() || tx_type != TransactionType::ReadOnly {
                group.master()
            } else {
                client_route_query::run(self, group, tx_type, query).await?
            };
            if let Some(pool) = pool {
                self.set_pool(Some(pool));
                let backend = pool.get(application_name, user, tx_type).await?;
                if let Some(backend_ref) = backend.load() {
                    let client = Ark::from(self);
                    backend_ref.set_client(client);
                    return Ok(backend);
                }
                error_code = error_codes::CONFIGURATION_LIMIT_EXCEEDED;
            }
        }

        let error_msg = "no database available for query";
        self.send(Messages::new_error(error_code, error_msg)).await?;
        Err(Error::new(error_msg))
    }

    #[instrument]
    pub async fn client_partition<'a>(&'a self, _: &'a mut client_partition::Event, cluster: &'static PostgresCluster, _application_name: &'a str, _user: &'a str, database: &'a str, _tx_type: TransactionType, _query: &'a mut Query) -> Result<Option<&'static PostgresReplicationGroup>> {
        Ok(cluster.get_by_database(database))
    }

    #[instrument]
    pub async fn client_route_query<'a>(&'a self, _: &'a mut client_route_query::Event, group: &'static PostgresReplicationGroup, _tx_type: TransactionType, _query: &'a mut Query) -> Result<Option<&'static ConnectionPool>> {
        Ok(group.master())
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
        self.transition(ClientState::Authentication)?;

        let mut mb = MessageBuilder::new(Tag::AUTHENTICATION_OK);
        mb.write_i32(auth_type.as_i32());
        if let AuthType::MD5 = auth_type {
            mb.write_i32(self.salt);
        }
        self.send(mb.finish()).await?;

        Ok(auth_type)
    }

    #[instrument]
    pub async fn client_authenticate(&self, _: &mut client_authenticate::Event, auth_type: AuthType, msgs: Messages) -> Result<()> {
        let params = self.connection_params();
        let cluster = self.cluster.load().expect("expected db_cluster to be set");

        if msgs.count() != 1 {
            return Err(Error::new("client_authenticate expects exactly one Message"));
        }

        let msg = msgs.first().unwrap(); // see msgs.count() condition above
        match msg.tag() {
            Tag::PASSWORD_MESSAGE => {
                // user and database exist, see ServerParams::from_startup_message
                let user = params.get("user").expect("missing user");
                let database = params.get("database").expect("missing database");

                let group = cluster.get_by_database(database);
                if let Some(group) = group {
                    let pool = group.master();
                    if let Some(pool) = pool {
                        let password = if auth_type == AuthType::ClearText {
                            msg.reader().read_str()?
                        } else if user == pool.config.user {
                            pool.config.password.as_str()
                        } else {
                            // TODO confirm this is the right error code
                            let error_msg = format!("unless the user is the configured user, only clear text authentication is supported: {}@{}", user, database);
                            self.send(Messages::new_error(error_codes::INVALID_AUTHORIZATION_SPECIFICATION, &error_msg)).await?;
                            return Err(Error::new(error_msg))
                        };

                        return if cluster.authenticate(user, password, pool).await? {
                            client_complete_startup::run(self, cluster).await
                        } else {
                            let error_msg = format!("password authentication failed for user \"{}\"", user);
                            self.send(Messages::new_error(error_codes::INVALID_PASSWORD, &error_msg)).await?;
                            Err(Error::new(error_msg))
                        };
                    }
                }

                let error_msg = format!("database \"{}\" does not exist", database);
                self.send(Messages::new_error(error_codes::INVALID_CATALOG_NAME, &error_msg)).await?;
                Err(Error::new(error_msg))
            },
            _ => {
                Err(Error::new(format!("unexpected {}", msgs)))
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
        let msgs = mb.finish();
        self.send(msgs).await?;
        self.transition(ClientState::Ready)
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

        Ok(self.cluster().unwrap_or_else(PostgresCluster::singleton))
    }

    #[instrument]
    pub async fn client_messages(&self, _: &mut client_messages::Event, msgs: Messages) -> Result<()> {
        let state = self.state.get();
        match state {
            ClientState::StateInitial => {
                self.startup(msgs).await
            },
            ClientState::Authentication => {
                let auth_type = self.auth_type.load();
                client_authenticate::run(self, auth_type, msgs).await
            },
            ClientState::Ready | ClientState::Transaction | ClientState::FailedTransaction => {
                self.forward(msgs).await
            },
            ClientState::Closed => {
                Err(Error::closed())
            },
            _ => {
                let error_msg = format!("received unexpected {:?} while in {:?}", msgs, state);
                self.send(Messages::new_error(error_codes::PROTOCOL_VIOLATION, &error_msg)).await?;
                Err(Error::new(error_msg))
            }
        }
    }

    #[instrument]
    pub async fn client_send_messages(&self, _: &mut client_send_messages::Event, msgs: Messages) -> Result<usize> {
        for msg in msgs.iter(0) {
            if msg.tag() == Tag::READY_FOR_QUERY {

            }
        }
        self.write_or_buffer(msgs.into_bytes())
    }

    #[instrument]
    pub async fn client_idle(&self, _: &mut client_idle::Event) -> Result<Ark<BackendConn>> {
        Ok(self.release_backend())
    }
}

impl AtomicRefCounted for ClientConn {
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

impl ServerConnection for ClientConn {
    fn new(stream: TcpStream, connections: &'static Connections<Self>) -> Self {
        ClientConn {
            transport: Transport::new(stream),
            id: Default::default(),
            last_active: Default::default(),
            auth_type: AtomicCell::default(),
            refcount_and_flags: RefcountAndFlags::new(),
            state: Default::default(),
            tx_type: AtomicCell::default(),
            backend: Ark::default(),
            send_backlog: Mutex::new(VecDeque::new()),
            cluster: AtomicRef::default(),
            replication_group: AtomicRef::default(),
            pool: AtomicRef::default(),
            buffered: Mutex::new(None),
            connect_params: UnsafeCell::new(ServerParams::new()),
            salt: Worker::get().rand32() as i32,
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
        self.last_active.load(Relaxed)
    }

    fn close(&self) {
        self.transition(ClientState::Closed).unwrap(); // does not fail
        if self.backend.is_some() {
            BackendConn::return_to_pool(self.release_backend());
        }
        self.transport.close();
    }
}

impl Connection for ClientConn {
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


define_event! {
    /// client_connected is called when a new client session is being established.
    ///     client: &ClientConn : the event source handling the client connection
    ///     params: &ServerParams : key-value pairs passed by the connected client in the startup message (including database and user)
    /// Returns the database cluster where the BackendConn will later be established (usually pool.get_cluster()).
    /// ClientConn::client_connected is called by default and sends the authentication challenge in response.
    /// If it returns an error, the associated session is terminated.
    client_connected,
    (client: &'a ClientConn, params: ServerParams) -> Result<&'static PostgresCluster>
}


define_event! {
    /// client_message is called when a Postgres protocol.Message is received in a client session.
    ///     client: &ClientConn : the event source handling the client connection
    ///     backend: Option<&'a BackendConn> : the associated backend connection (if any)
    ///     msg: protocol.Message is the received protocol.Message
    /// ClientConn::client_message is called by default and does further processing on the Message,
    /// including potentially calling the higher-level client_query. Symmetric with backend_message.
    /// If it returns an error, the associated session is terminated.
    client_messages,
    (client: &'a ClientConn, msgs: Messages) -> Result<()>
}

define_event! {
    /// TODO
    client_query,
    (client: &'a ClientConn, query: Query) -> Result<()>
}

define_event! {
    /// client_send_message is called to send a Message to the connected client.
    ///     client: &ClientConn : the event source handling the client connection
    ///     msgs : protocol.Messages is the messages to send
    /// Returns the number of bytes actually written (not buffered.)
    /// If it returns an error, the associated session is terminated.
    client_send_messages,
    (client: &'a ClientConn, msgs: Messages) -> Result<usize>
}

define_event! {
    /// TODO
    client_auth_challenge,
    (client: &'a ClientConn, params: ServerParams) -> Result<AuthType>
}

define_event! {
    /// TODO
    client_authenticate,
    (client: &'a ClientConn, auth_type: AuthType, msgs: Messages) -> Result<()>
}

define_event! {
    /// client_complete_startup is called to after authentication to send the
    /// authentication ok messages, parameter status messages, backend key data, and ready for query
    /// messages that a Postgres server sends when the startup phase is completed.
    ///     client: &ClientConn : the event source handling the client connection
    ///     cluster: &'static PostgresCluster : the Postgres cluster this connection belongs to.
    /// If it returns an error, the associated session is terminated.
    /// After calling ev.next() or equivalently sending this series of startup messages,
    /// the newly established connection is ready to receive queries.
    client_complete_startup,
    (client: &'a ClientConn, cluster: &'static PostgresCluster) -> Result<()>
}

define_event! {
    /// TODO
    client_connect_backend,
    (
        client: &'a ClientConn,
        cluster: &'static PostgresCluster,
        application_name: &'a str,
        user: &'a str,
        database: &'a str,
        tx_type: TransactionType,
        query: &'a mut Query
    ) -> Result<Ark<BackendConn>>
}

define_event! {
    /// TODO
    client_partition,
    (
        client: &'a ClientConn,
        cluster: &'static PostgresCluster,
        application_name: &'a str,
        user: &'a str,
        database: &'a str,
        tx_type: TransactionType,
        query: &'a mut Query
    ) -> Result<Option<&'static PostgresReplicationGroup>>
}

define_event! {
    /// TODO
    client_route_query,
    (
        client: &'a ClientConn,
        group: &'static PostgresReplicationGroup,
        tx_type: TransactionType,
        query: &'a mut Query
    ) -> Result<Option<&'static ConnectionPool>>
}

define_event! {
    /// client_idle is called when the connection is ready for a query, and not waiting for a response,
    /// and is not inside a transaction.
    ///     client: &ClientConn : the event source handling the client connection
    /// Optionally dissociates and returns the BackendConn. By default, if there is a BackendConn,
    /// ClientConn::client_idle will remove it from this session and return it. The caller
    /// then typically returns that BackendConn to the connection pool.
    /// If it returns an error, the associated session is terminated.
    client_idle,
    (client: &'a ClientConn) -> Result<Ark<BackendConn>>
}