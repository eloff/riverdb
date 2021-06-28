use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::atomic::Ordering::Relaxed;
use std::fmt::{Debug, Formatter};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, instrument};
use rustls::{ClientConnection};

use crate::riverdb::common::{Error, Result};
use crate::riverdb::worker::{get_worker, Worker};
use crate::riverdb::pg::protocol::MessageParser;
use crate::riverdb::pg::plugins;
use crate::riverdb::pool::PostgresCluster;
use crate::riverdb::coarse_monotonic_now;
use crate::riverdb::pg::PostgresClientConnectionState;
use crate::riverdb::server::Transport;

pub struct PostgresSession {
    stream: Transport<ClientConnection>,
    // span is used for logging to identify a trail of messages associated with this session
    id: u32,
    // last-active is a course-grained monotonic clock that is advanced when data is received from the client
    last_active: AtomicU32,
    state: PostgresClientConnectionState,
    read_only: bool,
    closed: AtomicBool,
}

impl PostgresSession {
    pub fn new(stream: TcpStream, id: u32) -> PostgresSession {
        PostgresSession {
            stream: Transport::new(stream, false),
            id,
            last_active: AtomicU32::default(),
            state: PostgresClientConnectionState::ClientConnectionStateInitial,
            read_only: false,
            closed: AtomicBool::default()
        }
    }

    #[instrument]
    pub async fn run(&mut self) -> Result<()> {
        info!(?self, "new session");
        let worker = get_worker();

        // There are up to four async operations that could potentially be happening concurrently
        //
        //  - read from client
        //  - read from backend (if backend is not None)
        //  - write to client (if outbox is not empty)
        //  - write to backend (if backend is not None, and its outbox is not empty)
        //
        // We can make this happen concurrently by calling each read_loop in a tokio task
        //
        // The writing can be tried optimistically. If inside of a read_loop we need to write,
        // write as much as possible until it would block, and if there is still more to write,
        // then spawn a tokio task to invoke the write_loop to drain the buffer. Any other writes
        // that would happen concurrently (e.g. in the next invocation of read_loop) would be
        // immediately queued to the write buffer and the write loop task would take care of them.
        // When the write buffer is completely flushed, the write loop task can exit.
        // The write buffer can be a simple Deque<Bytes>, as there is no multi-threading involved.

        let _cluster = plugins::run_client_connect_plugins(self).await?;

        self.read_loop(worker).await
    }

    async fn read_loop(&mut self, worker: &mut Worker) -> Result<()> {
        // This code is very similar to PostgresBackend::read_loop.
        // If you change this, check if you need to change that too.

        let mut parser = MessageParser::new(worker.get_recv_buffer());
        while !self.closed.load(Relaxed) {
            if let Some(msg) = parser.next(&mut self.stream).await? {
                let tag = msg.tag();
                debug!(%tag, "recevied message from client");
                if !self.state.msg_is_allowed(tag) {
                    return Err(Error::new(format!("unexpected message {} for state {}", tag, self.state)));
                }

                // TODO call OnClientMessage or OnBackendMessage
            }

            if parser.last_bytes_read != 0 {
                // TODO this particular shuffle would probably be useful elsewhere, factor it out into a utility function
                let current = self.last_active.load(Relaxed);
                if current != 0 {
                    let now = coarse_monotonic_now();
                    if current != now {
                        self.last_active.store(now, Relaxed);
                    }
                }
            }
        }
        Ok(())
    }

    async fn write_loop() -> Result<()> {
        Ok(())
    }

    pub async fn client_connected(&mut self, _: &mut plugins::ClientConnectContext) -> Result<&'static PostgresCluster> {
        unimplemented!();
    }
}

impl Debug for PostgresSession {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("pg::Session{{id: {}, state: {}}}", self.id, self.state))
    }
}