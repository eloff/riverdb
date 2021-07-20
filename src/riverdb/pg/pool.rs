use std::sync::atomic::AtomicI32;

use crate::riverdb::{Result};
use crate::riverdb::pg::isolation::IsolationLevel;
use crate::riverdb::server::{Connections, ConnectionRef};
use crate::riverdb::pg::BackendConn;
use crate::riverdb::pool::{Cluster, ReplicationGroup};
use std::sync::atomic::Ordering::Relaxed;

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
    connections: &'static Connections<BackendConn>,
    active_transactions: AtomicI32,
    max_transactions: i32,
    default_isolation_level: IsolationLevel,
}

impl ConnectionPool {
    pub fn get(&self, for_transaction: bool) -> Result<Option<PoolConnection>> {
        if for_transaction && self.active_transactions.fetch_add(1, Relaxed) > self.max_transactions {
            self.active_transactions.fetch_add(-1, Relaxed);
            return Ok(None);
        }


        Ok(Some(
            todo!()
        ))
    }

    fn put(&self, conn: BackendConn) {

    }
}

pub struct PoolConnection {
    pool: &'static ConnectionPool,
    conn: Box<BackendConn>,
}

pub type PostgresCluster = Cluster<ConnectionPool>;
pub type PostgresReplicationGroup = ReplicationGroup<ConnectionPool>;

// Safety: although ConnectionPool contains a reference, it's a shared thread-safe 'static reference.
// It is safe to send a ConnectionPool between threads.
unsafe impl Send for ConnectionPool {}