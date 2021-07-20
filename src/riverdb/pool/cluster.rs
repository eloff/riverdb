use crate::riverdb::pool::ReplicationGroup;

/// A Cluster represents a collection of nodes which store all database partitions.
/// Each node itself may be a replication group with a single master and multiple read-only replicas.
/// By default there is only one global singleton Cluster. If you need multiple
/// clusters, you can run multiple riverdb processes. It's also possible to
/// have multiple Clusters managed in a single process by using custom plugins.
pub struct Cluster<P> {
    pub nodes: Vec<ReplicationGroup<P>>,
}