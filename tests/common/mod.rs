use tokio::net::{TcpListener, TcpSocket};
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering::Relaxed;
use std::net::{SocketAddrV4, Ipv4Addr, SocketAddr, IpAddr};
use tokio::process::{Command, Child};

const TEST_DATABASE: &str = "riverdb_test";
const TEST_USER: &str = TEST_DATABASE;
const TEST_PASSWORD: &str = "1234"; // the kind of thing an idiot might put on their luggage

pub static LISTEN_PORT: AtomicU16 = AtomicU16::new(10101);

pub fn listener() -> TcpListener {
    let mut port: u16 = 0;
    for i in 0..10 {
        port = LISTEN_PORT.fetch_add(1, Relaxed);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let sock = TcpSocket::new_v4().unwrap();
        if let Ok(_) = sock.bind(addr) {
            return sock.listen(32).expect("couldn't listen on socket");
        }
    }
    panic!("couldn't find an available listen port between {}-{}", port-10, port);
}

pub fn psql(connection_str: &str) -> Child {
    Command::new("psql")
        .arg(format!("user={} dbname={} {}", TEST_USER, TEST_DATABASE, connection_str))
        .env("PGPASSWORD", TEST_PASSWORD)
        .spawn()
        .expect("couldn't run psql")
}