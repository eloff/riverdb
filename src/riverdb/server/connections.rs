use std::pin::Pin;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::{Relaxed, AcqRel, Acquire, Release};
use std::sync::atomic::{AtomicPtr, AtomicI64, AtomicI32, AtomicUsize};
use std::sync::{Mutex, Weak, Arc};

use tokio::net::TcpStream;
use tokio::time::{interval, Duration};
use tracing::{warn, info, info_span};

use crate::riverdb::worker::Worker;
use crate::riverdb::common::coarse_monotonic_now;
use crate::riverdb::config::CHECK_TIMEOUTS_INTERVAL;

pub trait Connection: std::fmt::Debug {
    fn new(s: TcpStream) -> Self;
    fn id(&self) -> u32;
    fn set_id(&self, id: u32);
    fn last_active(&self) -> u32;
    /// close closes the underlying socket, unblocking any suspended async tasks awaiting socket readiness
    /// do not call this in ConnectionRef::drop, there's nothing blocked in that case, and dropping the socket closes it.
    fn close(&self);
}

pub struct Connections<C: 'static + Connection> {
    items: &'static [AtomicPtr<C>],
    timeout_seconds: u32,
    max_connections: u32,
    added: AtomicI64,
    removed: AtomicI64,
    errors: AtomicI64,
    remove_lock: Mutex<()>,
}

impl<C: 'static + Connection> Connections<C> {
    pub fn new(max_connections: u32, timeout_seconds: u32) -> &'static Self {
        let connections = &*Box::leak(Box::new(Self{
            items: Vec::with_capacity((max_connections as f64 * 1.1) as usize).leak(),
            timeout_seconds,
            max_connections,
            added: Default::default(),
            removed: Default::default(),
            errors: Default::default(),
            remove_lock: Mutex::new(())
        }));

        if timeout_seconds > 0 {
            tokio::spawn(connections.timeouts_task());
        }

        connections
    }

    /// len returns the number of active connections at the current moment.
    /// Unlike the count we do in add() that may understate the actual, this may slightly overstate it.
    /// That's because this is used to skip iteration if len() == 0, and we don't want to do that if there's
    /// a chance it's not empty.
    pub fn len(&self) -> usize {
        let removed = self.removed.load(Acquire);
        let count = self.added.load(Acquire) - removed;
        // This can't be negative, because we load removed first.
        // Added will always be >= removed at the same or later point in time.
        debug_assert!(count >= 0);
        count as usize
    }

    pub fn is_full(&self) -> bool {
        self.len() >= self.max_connections as usize
    }

    pub fn add(&'static self, stream: TcpStream) -> Option<ConnectionRef<C>> {
        // Because remove is loaded second, this might impose a very slightly lower limit (but never higher)
        let added = self.added.fetch_add(1, AcqRel) + 1;
        if added - self.removed.load(Acquire) > self.max_connections as i64 {
            self.added.fetch_add(-1, Relaxed);
            warn!(limit=self.max_connections, "reached connection limit");
            return None;
        }

        let mut conn = Arc::new(C::new(stream));
        // Storing a raw pointer is fine, the object is removed from this collection before the Arc is dropped
        // See ConnectionRef::drop for where we do that.
        let conn_ptr = conn.as_ref() as *const C as *mut C;

        // Pick a random place in the array and search from there for a free connection slot.
        // This shouldn't take long because we allocated items to be at least 10% larger than maxConcurrent.
        let end = self.items.len();
        let mid = Worker::get().uniform_rand32(end as u32) as usize;
        let mut i = mid + 1;

        // Scan from (mid, end), and then [start, mid]
        while i != mid {
            if i == end {
                i = 0;
            }
            // Safety: get_unchecked is safe because we iterate between [0, items.len())
            let slot = unsafe { self.items.get_unchecked(i) };
            if slot.load(Relaxed).is_null() {
                if slot.compare_exchange(std::ptr::null_mut(), conn_ptr, Release, Relaxed).is_ok() {
                    conn.set_id((i + 1) as u32);
                    break;
                }
            }
            i += 1;
        }

        Some(ConnectionRef{
            connections: self,
            conn,
        })
    }

    fn remove(&self, conn: &C, id: u32) {
        let slot = self.items.get((id - 1) as usize).expect("invalid id");
        let current = slot.load(Acquire);

        assert!(!current.is_null());
        assert_eq!(current, conn as *const C as *mut C);

        let _guard = self.remove_lock.lock().unwrap();
        // These can all be relaxed loads/stores since the mutex acquire/release will ensure they have total order
        slot.store(std::ptr::null_mut(), Relaxed);
        self.removed.store(self.removed.load(Relaxed) + 1, Relaxed);
    }

    /// for_each iterates over all active connections and calls f(&connection) for each.
    /// This should only ever be used for read-only access and only to atomic fields.
    /// We use this for collecting statistics and timing out inactive connections.
    ///
    /// If f returns true, iteration stops and true is returned. Else iteration continues
    /// until exhausted, and false is returned.
    pub fn for_each<F: FnMut(&C) -> bool>(&self, mut f: F) -> bool {
        if self.len() == 0 {
            return false
        }

        // This must be exclusive with remove to ensure we don't see freed memory
        // A concurrent remove can free the connection memory, after we've seen a pointer to it.
        let _guard = self.remove_lock.lock().unwrap();
        for slot in self.items.iter() {
            let p = slot.load(Acquire);
            if !p.is_null() {
                // Safety: Because of the remove_lock that we're holding we know this points inside a valid Arc<C>
                if f(unsafe { &*p }) {
                    return true
                }
            }
        }
        return false
    }

    fn do_timeouts(&self) {
        let _span = info_span!("scanning for inactive, timed-out connections", "estimated {} total connections", self.len()).entered();

        let now = coarse_monotonic_now();
        self.for_each(|conn| {
            let last_active = conn.last_active();
            if last_active != 0 && last_active + self.timeout_seconds < now {
                warn!(timeout=self.timeout_seconds, "closing connection {:?} because it timed out", conn);
                // This will trigger the task that called conn.run() to exit,
                // and the connection to be dropped (including calling self.remove for it.)
                conn.close();
            }
            false
        });
    }

    async fn timeouts_task(&self) {
        let mut interval = interval(Duration::from_secs(CHECK_TIMEOUTS_INTERVAL));
        loop {
            interval.tick().await;
            self.do_timeouts();
        }
    }
}

pub struct ConnectionRef<C: 'static + Connection> {
    connections: &'static Connections<C>,
    conn: Arc<C>,
}

impl<C: 'static + Connection> ConnectionRef<C> {
    pub fn arc_ref(this: &Self) -> &Arc<C> {
        &this.conn
    }

    pub fn clone_arc(this: &Self) -> Arc<C> {
        this.conn.clone()
    }
}

impl<C: 'static + Connection> Deref for ConnectionRef<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl<C: 'static + Connection> Drop for ConnectionRef<C> {
    fn drop(&mut self) {
        self.connections.remove(&self.conn, self.conn.id())
    }
}

// Safety: although these contain a reference, it's a shared thread-safe 'static reference.
// It is safe to send a ConnectionRef between threads.
unsafe impl<C: 'static + Connection> Sync for Connections<C> {}
unsafe impl<C: 'static + Connection> Send for ConnectionRef<C> {}