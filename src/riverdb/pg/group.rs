use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;

use crate::riverdb::config;
use crate::riverdb::{Result};
use crate::riverdb::pg::ConnectionPool;
use crate::riverdb::common::AtomicRef;

pub struct PostgresReplicationGroup {
    pub master: AtomicRef<'static, ConnectionPool>,
    pub replicas: Vec<&'static ConnectionPool>,
    next_replica: AtomicU32,
}

impl PostgresReplicationGroup {
    pub fn new(config: &'static config::Postgres) -> Self {
        let replicas = config.replicas.iter().map(|c| &*Box::leak(Box::new(ConnectionPool::new(c)))).collect();
        Self{
            master: AtomicRef::new(Some(Box::leak(Box::new(ConnectionPool::new(config))))),
            replicas,
            next_replica: AtomicU32::new(0),
        }
    }

    pub fn has_replica(&self) -> bool {
        !self.replicas.is_empty()
    }

    pub fn round_robin(&self, allow_replica: bool) -> &'static ConnectionPool {
        if !allow_replica || !self.has_replica() {
            return self.master.load().unwrap();
        }

        // This can produce the same replica occasionally under load, that's fine.
        let cur = self.next_replica.load(Relaxed);
        let mut next = cur + 1;
        if next == self.replicas.len() as u32 {
            next = 0;
        }
        self.next_replica.store(next, Relaxed);
        self.replicas.get(cur as usize).unwrap()
    }

    pub async fn test_connection(&self) -> Result<Vec<(String, String)>> {
        let conn = self.master.load().unwrap().get("", false).await?;

        todo!();
        //conn.params
    }
}