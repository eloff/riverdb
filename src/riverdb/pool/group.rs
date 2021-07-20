
pub struct ReplicationGroup<P> {
    pub master: P,
    pub replicas: Vec<P>,
}