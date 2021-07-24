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
use crate::riverdb::pg::protocol::{Message, MessageParser, ServerParams};
use crate::riverdb::pg::{ClientConnState, BackendConn, Connection};
use crate::riverdb::server::Transport;
use crate::riverdb::server;
use crate::riverdb::pg::{PostgresCluster, ConnectionPool};
use crate::riverdb::pg::connection::{read_and_flush_backlog, Backlog};
use crate::riverdb::pg::backend_state::BackendState;
use crate::riverdb::pg::client_state::ClientState;
use crate::riverdb::common::{AtomicCell, AtomicArc};


pub struct ClientConn {
    /// client_stream is a possibly uninitialized Transport, may check if client_id != 0 first
    transport: Transport,
    /// id is set once and then read-only. Starts as 0.
    id: AtomicU32,
    /// last-active is a course-grained monotonic clock that is advanced when data is received from the client
    last_active: AtomicU32,
    has_send_backlog: AtomicBool,
    state: ClientConnState,
    backend: AtomicArc<BackendConn>,
    pool: AtomicCell<Option<&'static ConnectionPool>>,
    send_backlog: Backlog,
    salt: u32,
}

impl ClientConn {
    #[instrument]
    pub async fn run(&self) -> Result<()> {
        // XXX: This code is very similar to BackendConn::run.
        // If you change this, you probably need to change that too.

        let mut parser = MessageParser::new();
        let mut backend: Option<Arc<BackendConn>> = None; // keep
        loop {
            // Check first if we have another message in the parser
            if let Some(result) = parser.next() {
                let msg = result?;
                let tag = msg.tag();
                debug!(%tag, "received message from client");
                if !self.state.msg_is_allowed(tag) {
                    return Err(Error::new(format!("unexpected message {} for state {:?}", tag, self.state)));
                }

                // TODO run client_message
            } else {
                // We don't want to clone the Arc everytime, so we clone() it once calling self.get_backend()
                // And then we cache that Arc, checking that it's still the current backend with self.has_backend()
                // Which is cheaper the the atomic-read-modify-write ops used increment and decrement and Arc.
                if backend.is_none() || !self.has_backend(backend.as_ref().unwrap()) {
                    backend = self.get_backend();
                }

                read_and_flush_backlog(
                    self,
                    parser.bytes_mut(),
                    backend.as_ref().map(|arc| arc.as_ref()),
                ).await?;
            }
        }
    }

    pub fn get_backend(&self) -> Option<Arc<BackendConn>> {
        self.backend.load()
    }

    pub fn has_backend(&self, backend: &BackendConn) -> bool {
        self.backend.is(backend)
    }

    pub fn set_backend(&self, backend: Option<Arc<BackendConn>>) {
        self.backend.store(backend);
    }

    pub async fn client_connected(&mut self, _: &mut client_connected::Event, params: &ServerParams) -> Result<&'static PostgresCluster> {
        if let Some(encoding) = params.get("client_encoding") {
            if encoding.to_ascii_uppercase() != "UTF8" {
                error!(encoding, "client_encoding must be set to UTF8");
            }
        }
        Ok(PostgresCluster::singleton())
    }

    pub async fn client_message(&mut self, _: &mut client_message::Event, msg: Message) -> Result<()> {
        unimplemented!();
    }
}

impl server::Connection for ClientConn {
    fn new(stream: TcpStream) -> Self {
        ClientConn {
            transport: Transport::new(stream),
            id: Default::default(),
            last_active: Default::default(),
            has_send_backlog: Default::default(),
            state: Default::default(),
            backend: Default::default(),
            pool: Default::default(),
            send_backlog: Mutex::new(VecDeque::new()),
            salt: Worker::get().rand32()
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
        self.state.transition(ClientState::Closed);
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
}

impl Debug for ClientConn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "pg::ClientConn{{id: {}, state: {:?}}}",
             self.id.load(Relaxed),
             self.state))
    }
}

/// client_connected is called when a new client session is being established.
///     client: &mut ClientConn : the event source handling the client connection
///     params: &mut FnvHashMap : key-value pairs passed by the connected client in the startup message (including database and user)
/// Returns the database cluster where the BackendConn will later be established (usually pool.get_cluster()).
/// ClientConn::client_connected is called by default and sends the authentication challenge in response.
/// If it returns an error, the associated session is terminated.
define_event!(client_connected, (client: &'a mut ClientConn, params: &'a ServerParams) -> Result<&'static PostgresCluster>);

/// client_message is called when a Postgres protocol.Message is received in a client session.
///     client: &mut ClientConn : the event source handling the client connection
///     msg: protocol.Message is the received protocol.Message
/// You can replace msg by creating and passing a new Message object to ev.next(...)
/// It's also possible to replace a single Message with many by calling ev.next() for each.
/// Or conversely replace many messages with fewer by buffering the Message and not immediately calling next.
/// ClientConn::client_message is called by default and does further processing on the Message,
/// including potentially calling the higher-level client_query. Symmetric with backend_message.
/// If it returns an error, the associated session is terminated.
define_event!(client_message, (client: &'a mut ClientConn, msg: Message) -> Result<()>);