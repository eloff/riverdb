use std::fmt::{Debug, Formatter};
use std::cell::UnsafeCell;
use std::sync::RwLock;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{AcqRel, Acquire};


use fnv::FnvHashSet;
use crypto::sha2::Sha256;
use crypto::digest::Digest;

use crate::riverdb::{Result};
use crate::riverdb::config;
use crate::riverdb::pg::{PostgresReplicationGroup, ConnectionPool, BackendConn};
use crate::riverdb::pg::group::merge_server_params;
use crate::riverdb::pg::protocol::ServerParams;


/// A Cluster represents a collection of nodes which store all database partitions.
/// Each node itself may be a replication group with a single master and multiple read-only replicas.
/// By default there is only one global singleton Cluster. If you need multiple
/// clusters, you can run multiple riverdb processes. It's also possible to
/// have multiple Clusters managed in a single process by using custom plugins.
pub struct PostgresCluster {
    /// The configuration for this cluster of replication groups.
    pub config: &'static config::PostgresCluster,
    /// The nodes of the cluster (each node is a replication group which may consist of multiple servers.)
    pub nodes: Vec<PostgresReplicationGroup>,
    startup_params: UnsafeCell<ServerParams>,
    auth_cache: RwLock<FnvHashSet<[u8; 32]>>, // keyed by sha256(user+database+password)
}

impl PostgresCluster {
    /// Create a new PostgresCluster from the passed configuration.
    pub fn new(config: &'static config::PostgresCluster) -> Self {
        let nodes = config.servers.iter().map(PostgresReplicationGroup::new).collect();
        Self{
            config,
            nodes,
            startup_params: UnsafeCell::new(ServerParams::default()),
            auth_cache: RwLock::new(FnvHashSet::default()),
        }
    }

    /// Return the global PostgresCluster instance. It's possible to have multiple PostgresCluster
    /// in a single server process, but that must be managed through plugins. The typical
    /// configuration is to have only a single logical cluster. Each node of the cluster
    /// represents a Postgres master plus optional replicas.
    pub fn singleton() -> &'static Self {
        static SINGLETON_CLUSTER: AtomicPtr<PostgresCluster> = AtomicPtr::new(std::ptr::null_mut());
        unsafe {
            let mut p = SINGLETON_CLUSTER.load(Acquire);
            if p.is_null() {
                let mut cluster = Box::new(PostgresCluster::new(&config::conf().postgres));
                p = cluster.as_mut() as _;
                match SINGLETON_CLUSTER.compare_exchange(std::ptr::null_mut(), p, AcqRel, Acquire) {
                    Ok(_) => {
                        Box::leak(cluster);
                    },
                    Err(current) => {
                        p = current;
                    },
                }
            }
            &*p
        }
    }

    /// Returns a reference to the PostgresReplicationGroup of the first partition with a matching database
    pub fn get_by_database(&'static self, database: &str) -> Option<&'static PostgresReplicationGroup> {
        for node in self.nodes.iter() {
            if node.config.database == database {
                return Some(node);
            }
        }
        None
    }

    /// Test a connection to each node in the cluster.
    pub async fn test_connection(&self) -> Result<()> {
        let mut params = futures::future::try_join_all(
            self.nodes.iter()
                .map(|n| n.test_connection())).await?;

        params.reverse();
        if let Some(master_params) = params.pop() {
            let master = params.iter().fold(master_params, |m, o| {
                let mut m = m;
                merge_server_params(&mut m, o);
                m
            });
            unsafe {
                *self.startup_params.get() = master;
            }
        }
        Ok(())
    }

    /// Get the common/shared ServerParams for the cluster.
    pub fn get_startup_params(&self) -> &ServerParams {
        // Safety: this is not called until after it's initialized (prior to starting the server)
        unsafe { &*self.startup_params.get() }
    }

    /// Authenticate with the given credentials against pool for this cluster and cache the result.
    /// Returns if the authentication was successful (or if cached, returns the cache result.)
    pub async fn authenticate<'a, 'b: 'a, 'c: 'a>(&'a self, user: &'b str, password: &'c str, pool: &'static ConnectionPool) -> Result<bool> {
        let key = hash_sha256(user, password, &pool.config.database);
        if !self.auth_cache.read().unwrap().contains(&key[..]) {
            let backend = BackendConn::connect(pool.config.address.as_ref().unwrap(), pool.connections).await?;
            backend.test_auth(user, password, pool).await?;
            self.auth_cache.write().unwrap().insert(key);
        }
        Ok(true)
    }
}

/// hashes a (user, database, password) tuple with sha256
fn hash_sha256(user: &str, password: &str, database: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.input_str(user);
    hasher.input_str(database);
    hasher.input_str(password);
    let mut result = [0; 32];
    hasher.result(&mut result);
    result
}

impl Debug for PostgresCluster {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("PostgresCluster(num_partitions={})", self.config.servers.len()))
    }
}

// Safety: UnsafeCell<ServerParams> is not Sync, but we use it safely
// by setting it before accessing it from other threads.
unsafe impl Sync for PostgresCluster {}

