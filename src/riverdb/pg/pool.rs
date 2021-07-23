use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering::Relaxed;
use std::ops::Deref;
use std::sync::{Mutex, Arc};

use tokio::net::TcpStream;

use crate::riverdb::{Result};
use crate::riverdb::pg::isolation::IsolationLevel;
use crate::riverdb::server::{Connections, ConnectionRef};
use crate::riverdb::pg::BackendConn;
use crate::riverdb::pool::{Cluster, ReplicationGroup};
use crate::riverdb::config::Postgres;


// We just use a Mutex and Vec here to implement the pool.
// If that proves to be a bottleneck we can scale it with the same
// work-stealing algorithm/containers that tokio uses:
// https://tokio.rs/blog/2019-10-scheduler#a-better-run-queue
// But that's too involved for the MVP.
//
// We really just have to slap the thread-local work-stealing queues
// on top in the Worker struct and then this Mutex<Vec> becomes the
// shared global queue in the tokio algorithm.

pub struct ConnectionPool {
    config: &'static Postgres,
    connections: &'static Connections<BackendConn>,
    active_transactions: AtomicI32,
    max_transactions: i32,
    default_isolation_level: IsolationLevel,
    pooled_connections: Mutex<Vec<Arc<BackendConn>>>,
}

impl ConnectionPool {
    pub fn new(config: &'static Postgres) -> Self {
        Self{
            config,
            connections: Connections::new(config.max_connections, 0), // we don't use the Connections level timeout
            active_transactions: Default::default(),
            max_transactions: config.max_concurrent_transactions as i32,
            default_isolation_level: IsolationLevel::None,
            pooled_connections: Mutex::new(Vec::new()),
        }
    }
    
    pub async fn get(&'static self, role: &str, for_transaction: bool) -> Result<Option<Arc<BackendConn>>> {
        if for_transaction && self.active_transactions.fetch_add(1, Relaxed) > self.max_transactions {
            self.active_transactions.fetch_add(-1, Relaxed);
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
                    tokio::spawn(async move {
                        conn_ref.run().await;
                        self.remove(ConnectionRef::arc_ref(&conn_ref));
                    });

                    created = true;
                    conn
                } else {
                    return Ok(None);
                }
            };

            if let Err(e) = conn.check_health_and_set_role(role).await {
                // TODO log error
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

    pub fn put(&self, conn: *const BackendConn) {


        // TODO add to pool, set added time (if timeout != 0), decrement active_transactions if created_for_transaction
    }

    fn remove(&self, conn: &Arc<BackendConn>) {
        if !conn.in_pool() {
            return
        }

        let mut pool = self.pooled_connections.lock().unwrap();
        if let Some(i) = pool.iter().rposition(|a| Arc::ptr_eq(a,conn)) {
            pool.remove(i);
        }
    }
}

pub type PostgresCluster = Cluster<ConnectionPool>;
pub type PostgresReplicationGroup = ReplicationGroup<ConnectionPool>;

// Safety: although ConnectionPool contains a reference, it's a shared thread-safe 'static reference.
// It is safe to send a ConnectionPool between threads.
unsafe impl Send for ConnectionPool {}