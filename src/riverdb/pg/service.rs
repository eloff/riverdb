#[cfg(unix)]
use std::os::unix::io::AsRawFd;

use tokio::net::TcpStream;
use tracing::{info, warn};

use crate::riverdb::Result;
use crate::riverdb::worker::Worker;
use crate::riverdb::server::Listener;

pub struct PostgresService {
    listener: Listener
}

impl PostgresService {
    pub fn new(address: String, reuseport: bool) -> Self{
        Self{
            listener: Listener::new(address, reuseport).expect("could not create listener"),
        }
    }

    pub async fn run(&self) {
        info!(adress = %self.listener.address.as_str(), "starting PostgresService on worker thread {}", Worker::get().id);
        // Use an explicit handle here rather than looking it up in thread local storage each time
        let tokio = tokio::runtime::Handle::current();
        while let Some(sock) = self.listener.accept().await {
            tokio::spawn(async move {
                if let Err(e) = postgres_service(sock).await {
                    warn!(%e, "error in postgres_service");
                }
            });
        }
    }
}

pub async fn postgres_service(sock: TcpStream) -> Result<()> {
    todo!()
}