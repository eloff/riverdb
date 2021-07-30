use std::sync::atomic::{AtomicI32, AtomicPtr};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release, AcqRel};
use std::ops::Deref;
use std::sync::{Mutex, Arc};
use std::fmt::{Debug, Formatter};

use tokio::net::TcpStream;
use tracing::{warn};

use crate::riverdb::{Result};
use crate::riverdb::pg::isolation::IsolationLevel;
use crate::riverdb::server::{Connections, ConnectionRef, Connection};
use crate::riverdb::pg::{BackendConn};
use crate::riverdb::config;
use crate::riverdb::config::{Postgres, conf};
use crate::riverdb::common::{coarse_monotonic_now, Version, AtomicCell, change_lifetime};
use crate::riverdb::pg::protocol::ServerParams;



// We just use a Mutex and Vec here to implement the pool.
// if contention is light, this is optimal. We hold the lock for very short
// periods, so it may well be the way to go.
//
// If that proves to be a bottleneck we can scale it with the same
// work-stealing algorithm/containers that tokio uses:
// https://tokio.rs/blog/2019-10-scheduler#a-better-run-queue
// But that's too involved for the MVP.
//
// We really just have to slap the thread-local work-stealing queues
// on top in the Worker struct and then this Mutex<Vec> becomes the
// shared global queue as-in the tokio algorithm.

pub struct ConnectionPool {
    pub config: &'static Postgres,
    connections: &'static Connections<BackendConn>,
    active_transactions: AtomicI32,
    max_transactions: i32,
    default_isolation_level: AtomicCell<IsolationLevel>,
    server_version: AtomicCell<Version>,
    pooled_connections: Mutex<Vec<Arc<BackendConn>>>,
}

impl ConnectionPool {
    pub fn new(config: &'static Postgres) -> Self {
        Self{
            config,
            connections: Connections::new(config.max_connections, 0), // we don't use the Connections level timeout
            active_transactions: Default::default(),
            max_transactions: config.max_concurrent_transactions as i32,
            default_isolation_level: AtomicCell::<IsolationLevel>::default(),
            server_version: Default::default(),
            pooled_connections: Mutex::new(Vec::new()),
        }
    }
    
    pub async fn get(&self, application_name: &str, role: &str, for_transaction: bool) -> Result<Option<Arc<BackendConn>>> {
        if for_transaction && self.active_transactions.fetch_add(1, Relaxed) > self.max_transactions {
            let prev = self.active_transactions.fetch_add(-1, Relaxed);
            debug_assert!(prev > 0);
            return Ok(None);
        }

        loop {
            let mut created = false;
            let conn = if let Some(conn) = self.pooled_connections.lock().unwrap().pop() {
                conn
            } else {
                if let Some(conn_ref) = self.connect().await? {
                    // Clone the Arc<BackendConn> so we can return that.
                    let conn = ConnectionRef::clone_arc(&conn_ref);
                    // Spawn off conn_ref.run() to handle incoming messages from the database server
                    // Which can happen asynchronously, and need to be handled (if only by dropping them)
                    // even if the connection is idle in the pool.

                    // Safety: self is 'static, but if we mark it as such the compiler throws a fit?!?
                    let static_self: &'static Self = unsafe { change_lifetime(self) };
                    tokio::spawn(async move {
                        conn_ref.run(static_self).await;
                        static_self.remove(ConnectionRef::arc_ref(&conn_ref));
                    });

                    if let IsolationLevel::None = self.default_isolation_level.load() {
                        // TODO Check the isolation level and record it
                    }

                    created = true;
                    conn
                } else {
                    return Ok(None);
                }
            };

            // Set the role for the connection, which also checks that it's healthy.
            // If this fails, and the connection came from the pool, we try with another connection.
            if let Err(e) = conn.check_health_and_set_role(application_name, role).await {
                // If this connection came from the pool, and failed the health check
                // Record how long it was idle in the pool.
                let mut idle_seconds = 0;
                let now = coarse_monotonic_now();
                let added_to_pool = conn.last_active();
                if added_to_pool != 0 {
                    idle_seconds = now - added_to_pool;
                }
                warn!(?e, idle_seconds, role, "connection failed health check / set role");

                if !created {
                    continue;
                }
                // If even a new connection isn't healthy or can't set the role
                // then trying again with another new connection is unlikely to work.
                // Just return the error.
                return Err(e);
            } else {
                return Ok(Some(conn));
            }
        }
    }

    async fn connect(&self) -> Result<Option<ConnectionRef<BackendConn>>> {
        if self.connections.is_full() {
            return Ok(None);
        }

        let stream = TcpStream::connect(self.config.address.unwrap()).await?;

        Ok(self.connections.add(stream))
    }

    pub fn put(&self, conn: Arc<BackendConn>) {
        if conn.created_for_transaction() {
            let prev = self.active_transactions.fetch_add(-1, Relaxed);
            debug_assert!(prev > 0);
        }

        if !conn.set_in_pool() {
            conn.close();
            return // free connection
        }

        self.pooled_connections.lock().unwrap().push(conn);
    }

    fn remove(&self, conn: &Arc<BackendConn>) {
        if !conn.in_pool() {
            return
        }

        let mut pool = self.pooled_connections.lock().unwrap();
        // rposition should be slightly better than position here, as we remove needs to slide the
        // tail elements down, which will now be in cache after the search with rposition.
        if let Some(i) = pool.iter().rposition(|a| Arc::ptr_eq(a,conn)) {
            pool.remove(i);
        }
    }
}

// Safety: although ConnectionPool contains a reference, it's a shared thread-safe 'static reference.
// It is safe to send and share a ConnectionPool between threads.
unsafe impl Send for ConnectionPool {}
unsafe impl Sync for ConnectionPool {}

impl Debug for ConnectionPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("pg::ConnectionPool({})", self.config.address.as_ref().unwrap()))
    }
}