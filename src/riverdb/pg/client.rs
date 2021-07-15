use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::atomic::Ordering::Relaxed;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::net::TcpStream;
use tracing::{debug, error, info, instrument};
use rustls::{ClientConnection};

use crate::define_plugin;
use crate::riverdb::{Error, Result, common};
use crate::riverdb::worker::{Worker};
use crate::riverdb::pg::protocol::MessageParser;
use crate::riverdb::pg::{Session, SessionSide, ClientConnState};
use crate::riverdb::pool::PostgresCluster;
use crate::riverdb::server::{Transport};

pub struct ClientConn {
    pub session: Arc<Session>, // shared session data
    state: ClientConnState,
}

define_plugin!(client_connected, (client: &'a mut ClientConn) -> Result<&'static PostgresCluster>);

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

    pub async fn client_connected(&mut self, _: &mut client_connected::Event) -> Result<&'static PostgresCluster> {
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