use std::fmt::{Debug, Formatter};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;
use std::str::FromStr;

use tracing::{warn};

use crate::riverdb::config;
use crate::riverdb::{Result, Error};
use crate::riverdb::pg::{ConnectionPool, TransactionType};
use crate::riverdb::common::{AtomicRef, Version};
use crate::riverdb::pg::protocol::ServerParams;


/// Represents a Postgres master (writable) database plus optional replicas.
pub struct PostgresReplicationGroup {
    /// The configuration for this replication group.
    pub config: &'static config::Postgres,
    master: AtomicRef<'static, ConnectionPool>,
    replicas: Vec<&'static ConnectionPool>,
    next_replica: AtomicU32,
}

impl PostgresReplicationGroup {
    /// Create a new replication group with the given configuration.
    pub fn new(config: &'static config::Postgres) -> Self {
        let replicas = config.replicas.iter().map(|c| &*Box::leak(Box::new(ConnectionPool::new(c)))).collect();
        Self{
            config,
            master: AtomicRef::new(Some(Box::leak(Box::new(ConnectionPool::new(config))))),
            replicas,
            next_replica: AtomicU32::new(0),
        }
    }

    /// Return a reference to the master of the group (can be None if master failed).
    pub fn master(&self) -> Option<&'static ConnectionPool> {
        self.master.load()
    }

    /// Returns true if there is a replica that we can query (see config.can_query).
    pub fn has_query_replica(&self) -> bool {
        self.replicas.iter().cloned().find(|db| db.config.can_query).is_some()
    }

    /// Return the ConnectionPool for the next one of the replicas (if any) or the master.
    pub fn round_robin(&self, allow_replica: bool) -> &'static ConnectionPool {
        if !allow_replica || !self.has_query_replica() {
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

    /// Test connecting to the master and each replica. Returns the ServerParams from the master
    /// merged with the parameters from the replicas. See merge_server_params for details.
    pub async fn test_connection(&self) -> Result<ServerParams> {
        let master = self.master.load().unwrap();
        let conn = master.get("riverdb","", TransactionType::None).await?;
        if conn.is_none() {
            return Err(Error::new(format!("could not connect {:?}", master)));
        }
        let mut master_params = conn.params().clone();

        for replica in &self.replicas {
            let conn = replica.get("riverdb", "", TransactionType::None).await?;
            if conn.is_none() {
                return Err(Error::new(format!("could not connect {:?}", replica)));
            }
            let replica_params = conn.params();
            merge_server_params(&mut master_params, &*replica_params);
        }
        Ok(master_params)
    }
}

impl Debug for PostgresReplicationGroup {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("pg::PostgresReplicationGroup(db={})", self.config.database))
    }
}

/// Merge the second ServerParams into the first.
/// server_version will be the minimum server_version seen.
/// Otherwise if both have the same paramter, the first value (master) will be kept.
/// Logs warnings if parameters or versions differ.
pub(crate) fn merge_server_params(master: &mut ServerParams, server: &ServerParams) {
    for (key, val) in server.iter() {
        if let Some(master_val) = master.get(key) {
            if key == "server_version" {
                // Compare versions and keep the lower one
                if let Ok(master_version) = Version::from_str(master_val) {
                    if let Ok(server_version) = Version::from_str(val) {
                        if server_version < master_version {
                            warn!("server has lower version {} to master {}, using the lower version", val, master_val);
                            master.set(key.to_string(), val.to_string());
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