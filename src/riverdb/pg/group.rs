use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;

use tracing::{warn};

use crate::riverdb::config;
use crate::riverdb::{Result, Error};
use crate::riverdb::pg::ConnectionPool;
use crate::riverdb::common::{AtomicRef, Version};
use crate::riverdb::pg::protocol::ServerParams;
use std::str::FromStr;

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

    pub async fn test_connection(&self) -> Result<ServerParams> {
        let master = self.master.load().unwrap();
        let conn = master.get("", false).await?
            .ok_or_else(|| Error::new(format!("could not connect {:?}", master)))?;
        let mut master_params = conn.params().clone();

        for replica in &self.replicas {
            let conn = replica.get("", false).await?
                .ok_or_else(|| Error::new(format!("could not connect {:?}", replica)))?;
            let replica_params = conn.params();
            merge_server_params(&mut master_params, &*replica_params);
        }
        Ok(master_params)
    }
}

pub(crate) fn merge_server_params(master: &mut ServerParams, server: &ServerParams) {
    for (key, val) in server.iter() {
        if let Some(master_val) = master.get(key) {
            if key == "server_version" {
                // Compare versions and keep the lower one
                if let Ok(master_version) = Version::from_str(master_val) {
                    if let Ok(server_version) = Version::from_str(val) {
                        if server_version < master_version {
                            warn!("server has lower version {} to master {}, using the lower version", val, master_val);
                            master.set(key, val);
                        }
                    }
                }
            } else if master_val != val {
                warn!("server value for server param {} of {} differs from the master value of {}. Clients will see the master's params, this may cause broken or unexpected behavior.", key, val, master_val);
            }
        } else {
            warn!("server has server param {}={}, but master has no value for that parameter. Clients will see the master's params.", key, val);
        }
    }
}