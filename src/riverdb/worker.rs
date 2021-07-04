#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::io;
use std::cell::{RefCell, RefMut};
use std::net::{SocketAddr, IpAddr};

use tokio::runtime::{Runtime, Builder, EnterGuard};
use tokio::net::{TcpListener, TcpSocket};
use tracing::{debug, error, info_span};
use bytes::BytesMut;

use crate::riverdb::{Error, Result};
use crate::riverdb::config::{conf, LISTEN_BACKLOG};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;

thread_local! {
    static CURRENT_WORKER: RefCell<*mut Worker> = RefCell::new(std::ptr::null_mut());
}

static mut ALL_WORKERS: &[Worker] = &[];

/// Worker represents a Worker thread and serves as a thread-local storage
/// for all the resources the worker thread accesses. This includes
/// the glommio runtime, random number generators, and
/// sharded data structures. It corresponds 1-to-1 with tokio worker threads.
///
/// All Worker methods take &mut self, because there should never be more than one reference to Worker.
/// That's mostly true if you don't hold references to a Worker across await points. Otherwise
/// another task on the same tokio runtime can run and get a Worker references while the first is
/// suspended in await. The one place we do break this rule, and is undefined behavior, is to
/// iterate over all workers with a shared reference. We only use sync::atomics in that case,
/// so it's very unlikely LLVM can generate invalid code for that. This is used when collecting
/// statistics, for one.
pub struct Worker {
    pub id: u32,
    locked: AtomicBool, // locked to a thread
}

pub unsafe fn init_workers(num_workers: u32) {
    let mut workers = Vec::with_capacity(num_workers as usize);
    for id in 1..num_workers+1 {
        workers.push(Worker::new(id));
    }
    ALL_WORKERS = &*workers.leak();
}

impl Worker {
    pub fn new(id: u32) -> Self {
        Worker {
            id,
            locked: AtomicBool::default(),
        }
    }

    /// get returns a mutable Worker reference to the thread-local Worker.
    /// panics if not called on one of the original tokio worker threads.
    /// If there's already a reference (e.g. you're holding a reference across an await point)
    /// then this will panic. Keep in mind also that some destructors may call get_worker
    /// to free memory or a resource, so holding this across nested scopes may panic.
    /// That can be resolved by dropping this first.
    pub fn get() -> RefMut<'static, &'static mut Worker> {
        Self::try_get().expect("not a worker thread")
    }

    pub fn try_get() -> Option<RefMut<'static, &'static mut Worker>> {
        CURRENT_WORKER.with(|ctx| {
            let mut p = ctx.borrow_mut();
            if p.is_null() {
                // Grab an unallocated worker from ALL_WORKERS
                if let Some(worker) = unsafe { ALL_WORKERS }
                    .iter()
                    .filter_map(|w| {
                        if w.try_lock() {
                            Some(w)
                        } else {
                            None
                        }
                    })
                    .next() {
                    *p = worker as *const Worker as *mut Worker;
                } else {
                    return None;
                }
            }
            // Transmute a *mut to &mut (safe, because it's not null)
            unsafe { std::mem::transmute(p) }
        })
    }

    pub fn try_lock(&self) -> bool {
        self.locked.compare_exchange(false, true, Relaxed, Relaxed).is_ok()
    }
}