use std::sync::atomic::{AtomicBool, AtomicU32, AtomicPtr};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release, AcqRel};
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};
use std::cell::Cell;
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
use crate::riverdb::pg::protocol::{Message, MessageParser};
use crate::riverdb::pg::{ClientConnState, BackendConn, Connection};
use crate::riverdb::server::Transport;
use crate::riverdb::server;
use crate::riverdb::pg::pool::PostgresCluster;
use crate::riverdb::pg::connection::{read_and_flush_backlog, Backlog};
use crate::riverdb::pg::backend_state::BackendState;
use crate::riverdb::pg::client_state::ClientState;


pub struct ClientConn {
    /// client_stream is a possibly uninitialized Transport, may check if client_id != 0 first
    transport: Transport,
    /// id is set once and then read-only. Starts as 0.
    id: AtomicU32,
    /// last-active is a course-grained monotonic clock that is advanced when data is received from the client
    last_active: AtomicU32,
    has_send_backlog: AtomicBool,
    state: ClientConnState,
    backend: AtomicPtr<BackendConn>,
    send_backlog: Backlog,
}

impl ClientConn {
    #[instrument]
    pub async fn run(&self) -> Result<()> {
        // XXX: This code is very similar to BackendConn::read_loop.
        // If you change this, you probably need to change that too.

        let mut parser = MessageParser::new();
        let mut backend: Option<&BackendConn> = None;
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
                read_and_flush_backlog(
                    self,
                    parser.bytes_mut(),
                    self.backend(),
                ).await?;
            }
        }
    }

    pub fn set_backend(&self, backend: *mut BackendConn) -> *mut BackendConn {
        let prev = self.backend.swap(backend, AcqRel);
        // If there was a previous ClientConn set, and we're set to the backend, clear that relation as well
        if !prev.is_null() {
            let prev_backend = unsafe { &*prev };
            if let Some(maybe_me) = prev_backend.client() {
                if maybe_me as * const _ == self as * const _ {
                    prev_backend.set_client(std::ptr::null_mut());
                }
            }
        }
        prev
    }

    pub async fn client_connected(&mut self, _: &mut client_connected::Event, params: &mut FnvHashMap<String, String>) -> Result<&'static PostgresCluster> {
        unimplemented!();
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
            send_backlog: Mutex::new(VecDeque::new())
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

    fn backend(&self) -> Option<&BackendConn> {
        let p = self.backend.load(Acquire);
        if !p.is_null() {
            let backend = unsafe { &*p };
            if !backend.is_closed() {
                return Some(backend);
            }
            self.backend.store(std::ptr::null_mut(), Relaxed);
        }
        None
    }

    fn client(&self) -> Option<&Self> {
        Some(self)
    }

    fn is_closed(&self) -> bool {
        if let ClientState::Closed = self.state.get() {
            true
        } else {
            false
        }
    }
}

impl Drop for ClientConn {
    fn drop(&mut self) {
        if let Some(backend) = self.backend() {
            backend.set_client(std::ptr::null_mut());
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
define_event!(client_connected, (client: &'a mut ClientConn, params: &'a mut FnvHashMap<String, String>) -> Result<&'static PostgresCluster>);

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