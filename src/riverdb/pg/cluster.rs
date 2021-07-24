use std::cell::UnsafeCell;

use crate::riverdb::config;
use crate::riverdb::pg::PostgresReplicationGroup;
use crate::riverdb::pg::protocol::StartupParams;
use crate::riverdb::common::AtomicRef;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{AcqRel, Acquire};

/// A Cluster represents a collection of nodes which store all database partitions.
/// Each node itself may be a replication group with a single master and multiple read-only replicas.
/// By default there is only one global singleton Cluster. If you need multiple
/// clusters, you can run multiple riverdb processes. It's also possible to
/// have multiple Clusters managed in a single process by using custom plugins.
pub struct PostgresCluster {
    pub config: &'static config::PostgresCluster,
    pub nodes: Vec<PostgresReplicationGroup>,
    startup_params: UnsafeCell<StartupParams>,
}

impl PostgresCluster {
    pub fn new(config: &'static config::PostgresCluster) -> Self {
        let nodes = config.servers.iter().map(PostgresReplicationGroup::new).collect();
        Self{
            config,
            nodes,
            startup_params: UnsafeCell::new(StartupParams::default()),
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

    pub fn get_startup_params(&self) -> &StartupParams {
        // Safety: this is not called until after it's initialized (prior to starting the server)
        unsafe { &*self.startup_params.get() }
    }
}