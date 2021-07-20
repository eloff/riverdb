use std::sync::{Arc, Mutex};
use std::cell::Cell;
use std::sync::atomic::{AtomicU32, AtomicBool, AtomicPtr};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release, AcqRel};
use std::collections::VecDeque;

use tokio::net::TcpStream;
use bytes::Bytes;

use crate::riverdb::pg::{BackendConnState, ClientConn, Connection};
use crate::riverdb::server::{Transport};
use crate::riverdb::server;
use std::fmt::{Debug, Formatter};
use crate::riverdb::pg::connection::Backlog;
use crate::riverdb::pg::backend_state::BackendState;


pub struct BackendConn {
    transport: Transport,
    /// id is set once and then read-only. Starts as 0.
    id: AtomicU32,
    /// added_to_pool is a course-grained monotonic clock that is 0, or records when this was returned to the pool
    added_to_pool: AtomicU32,
    has_send_backlog: AtomicBool,
    state: BackendConnState,
    client: AtomicPtr<ClientConn>,
    send_backlog: Backlog,
}

impl BackendConn {
    pub fn set_client(&self, client: *mut ClientConn) -> *mut ClientConn {
        let prev = self.client.swap(client, AcqRel);
        // If there was a previous ClientConn set, and we're set to the backend, clear that relation as well
        if !prev.is_null() {
            let prev_client = unsafe { &*prev };
            if let Some(maybe_me) = prev_client.backend() {
                if maybe_me as * const _ == self as * const _ {
                    prev_client.set_backend(std::ptr::null_mut());
                }
            }
        }
        prev
    }
}

impl server::Connection for BackendConn {
    fn new(stream: TcpStream) -> Self {
        BackendConn {
            transport: Transport::new(stream),
            id: Default::default(),
            added_to_pool: Default::default(),
            has_send_backlog: Default::default(),
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

    fn backend(&self) -> Option<&Self> {
        Some(self)
    }

    fn client(&self) -> Option<&ClientConn> {
        let p = self.client.load(Acquire);
        if !p.is_null() {
            let client = unsafe { &*p };
            if !client.is_closed() {
                return Some(client);
            }
            self.client.store(std::ptr::null_mut(), Relaxed);
        }
        None
    }

    fn is_closed(&self) -> bool {
        if let BackendState::Closed = self.state.get() {
            true
        } else {
            false
        }
    }
}

impl Drop for BackendConn {
    fn drop(&mut self) {
        if let Some(client) = self.client() {
            client.set_backend(std::ptr::null_mut());
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