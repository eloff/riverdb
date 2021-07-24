use crate::riverdb::config;
use crate::riverdb::pg::ConnectionPool;
use crate::riverdb::common::AtomicRef;

pub struct PostgresReplicationGroup {
    pub master: AtomicRef<'static, ConnectionPool>,
    pub replicas: Vec<&'static ConnectionPool>,
}

impl PostgresReplicationGroup {
    pub fn new(config: &'static config::Postgres) -> Self {
        let replicas = config.replicas.iter().map(|c| &*Box::leak(Box::new(ConnectionPool::new(c)))).collect();
        Self{
            master: AtomicRef::new(Some(Box::leak(Box::new(ConnectionPool::new(config))))),
            replicas,
        }
    }
}