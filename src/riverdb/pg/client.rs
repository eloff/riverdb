use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::atomic::Ordering::Relaxed;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use fnv::FnvHashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::net::TcpStream;
use tracing::{debug, error, info, instrument};
use rustls::{ClientConnection};

use crate::define_event;
use crate::riverdb::{Error, Result, common};
use crate::riverdb::worker::{Worker};
use crate::riverdb::pg::protocol::{Message, MessageParser};
use crate::riverdb::pg::{Session, SessionSide, ClientConnState};
use crate::riverdb::pool::PostgresCluster;
use crate::riverdb::server::{Transport};

pub struct ClientConn {
    pub session: Arc<Session>, // shared session data
    state: ClientConnState,
}

impl ClientConn {
    pub fn new(stream: TcpStream, conn_id: u32, session: Option<Arc<Session>>) -> Self {
        let transport = Transport::new(stream);
        ClientConn {
            session: session
                .clone()
                .unwrap_or_else(|| Session::new_with_client(transport, conn_id)),
            state: ClientConnState::StateInitial,
        }
    }

    #[instrument]
    pub async fn run(&mut self) -> Result<()> {
        info!(?self, "new client session");

        //let _cluster = plugins::run_client_connect_plugins(self).await?;

        self.read_loop().await
    }

    async fn read_loop(&mut self) -> Result<()> {
        // XXX: This code is very similar to BackendConn::read_loop.
        // If you change this, you probably need to change that too.

        let mut parser = MessageParser::new();
        loop {
            // Check first if we have another message in the parser
            if let Some(result) = parser.next() {
                let msg = result?;
                let tag = msg.tag();
                debug!(%tag, "received message from client");
                if !self.state.msg_is_allowed(tag) {
                    return Err(Error::new(format!("unexpected message {} for state {}", tag, self.state)));
                }
            } else {
                self.session.client_read_and_send_backlog(
                    parser.bytes_mut(),
                ).await?;
                continue;
            }

            // TODO call OnClientMessage
        }
    }

    async fn write_loop() -> Result<()> {
        Ok(())
    }

    pub async fn client_connected(&mut self, _: &mut client_connected::Event, params: &mut FnvHashMap<String, String>) -> Result<&'static PostgresCluster> {
        unimplemented!();
    }

    pub async fn client_message(&mut self, _: &mut client_message::Event, msg: Message) -> Result<()> {
        unimplemented!();
    }
}

impl Debug for ClientConn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "pg::Session{{id: {}, state: {}}}",
             self.session.client_id.load(Relaxed),
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
///     backend: Option<&???> is the associated Backend connection, if any, otherwise nil
///     msg: protocol.Message is the received protocol.Message
/// You can replace msg by creating and passing a new Message object to ev.next(...)
/// It's also possible to replace a single Message with many by calling ev.next() for each.
/// Or conversely replace many messages with fewer by buffering the Message and not immediately calling next.
/// ClientConn::client_message is called by default and does further processing on the Message,
/// including potentially calling the higher-level client_query. Symmetric with backend_message.
/// If it returns an error, the associated session is terminated.
define_event!(client_message, (client: &'a mut ClientConn, msg: Message) -> Result<()>);