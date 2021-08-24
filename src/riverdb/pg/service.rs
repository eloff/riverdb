#[cfg(unix)]



use tracing::{info, warn};


use crate::riverdb::worker::Worker;
use crate::riverdb::server::{Connections, Listener};
use crate::riverdb::pg::ClientConn;

pub struct PostgresService {
    listener: Listener,
    connections: &'static Connections<ClientConn>
}

impl PostgresService {
    pub fn new(address: String, max_connections: u32, timeout_seconds: u32, reuseport: bool) -> Self{
        Self{
            listener: Listener::new(address, reuseport).expect("could not create listener"),
            connections: Connections::new(max_connections, timeout_seconds),
        }
    }

    pub async fn run(&self) {
        info!(adress = %self.listener.address.as_str(), "starting PostgresService on worker thread {}", Worker::get().id);
        // Use an explicit handle here rather than looking it up in thread local storage each time
        let tokio = tokio::runtime::Handle::current();
        while let Some(sock) = self.listener.accept().await {
            if let Some(conn) = self.connections.add(sock) {
                tokio.spawn(async move {
                    if let Err(e) = conn.run().await {
                        warn!(%e, "error in Postgres ClientConn");
                    }
                });
            }
        }
    }
}