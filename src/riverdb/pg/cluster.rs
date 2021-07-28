use std::cell::UnsafeCell;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{AcqRel, Acquire};

use crate::riverdb::{Result};
use crate::riverdb::config;
use crate::riverdb::pg::PostgresReplicationGroup;
use crate::riverdb::pg::group::merge_server_params;
use crate::riverdb::pg::protocol::ServerParams;
use crate::riverdb::common::AtomicRef;

/// A Cluster represents a collection of nodes which store all database partitions.
/// Each node itself may be a replication group with a single master and multiple read-only replicas.
/// By default there is only one global singleton Cluster. If you need multiple
/// clusters, you can run multiple riverdb processes. It's also possible to
/// have multiple Clusters managed in a single process by using custom plugins.
pub struct PostgresCluster {
    pub config: &'static config::PostgresCluster,
    pub nodes: Vec<PostgresReplicationGroup>,
    startup_params: UnsafeCell<ServerParams>,
}

impl PostgresCluster {
    pub fn new(config: &'static config::PostgresCluster) -> Self {
        let nodes = config.servers.iter().map(PostgresReplicationGroup::new).collect();
        Self{
            config,
            nodes,
            startup_params: UnsafeCell::new(ServerParams::default()),
        }
    }

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

    pub fn get_startup_params(&self) -> &ServerParams {
        // Safety: this is not called until after it's initialized (prior to starting the server)
        unsafe { &*self.startup_params.get() }
    }
}

// Safety: UnsafeCell<ServerParams> is not Sync, but we use it safely
// by setting it before accessing it from other threads.
unsafe impl Sync for PostgresCluster {}