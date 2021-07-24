use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, AtomicBool, AtomicPtr};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release, AcqRel};
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};

use tokio::net::TcpStream;
use tracing::{debug, error, info, warn, instrument};
use bytes::Bytes;

use crate::riverdb::Result;
use crate::riverdb::pg::{BackendConnState, ClientConn, Connection};
use crate::riverdb::server::{Transport};
use crate::riverdb::server;
use crate::riverdb::pg::connection::Backlog;
use crate::riverdb::pg::backend_state::BackendState;
use crate::riverdb::common::{AtomicCell, AtomicArc, coarse_monotonic_now};


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
}

impl BackendConn {
    #[instrument]
    pub async fn run(&self) -> Result<()> {
        // XXX: This code is very similar to ClientConn::run.
        // If you change this, you probably need to change that too.

        todo!();
    }

    pub fn get_client(&self) -> Option<Arc<ClientConn>> {
        self.client.load()
    }

    pub fn has_client(&self, client: &ClientConn) -> bool {
        self.client.is(client)
    }

    pub fn set_client(&self, client: Option<Arc<ClientConn>>) {
        self.client.store(client);
    }

    pub async fn check_health_and_set_role(&self, role: &str) -> Result<()> {
        Ok(())
    }

    pub fn created_for_transaction(&self) -> bool {
        self.for_transaction.load(Relaxed)
    }

    pub fn in_pool(&self) -> bool {
        if let BackendState::InPool = self.state.get() {
            debug_assert_ne!(self.added_to_pool.load(Relaxed), 0);
            true
        } else {
            false
        }
    }

    pub fn set_in_pool(&self) -> bool {
        if let Err(e) = self.state.transition(BackendState::InPool) {
            warn!(?e, "cannot transition to InPool state");
            false
        } else {
            self.added_to_pool.store(coarse_monotonic_now(), Relaxed);
            self.for_transaction.store(false, Relaxed);
            true
        }
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
            client: Default::default(),
            send_backlog: Mutex::new(Default::default())
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
        if let BackendState::Closed = self.state.get() {
            true
        } else {
            false
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