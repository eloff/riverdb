use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering::{Relaxed, Release};
use std::net::{SocketAddrV4, Ipv4Addr, SocketAddr, IpAddr};
use std::process::{Command, Child, Stdio};

use tokio::net::{TcpListener, TcpSocket};

use crate::event_listener;
use crate::riverdb::config;
use crate::riverdb::pg::PostgresCluster;


pub const TEST_DATABASE: &str = "riverdb_test";
pub const TEST_USER: &str = TEST_DATABASE;
pub const TEST_USER_RO: &str = "riverdb_test_ro";
pub const TEST_PASSWORD: &str = "1234"; // the kind of thing an idiot might put on their luggage
pub const TEST_PASSWORD_RO: &str = "openseasame";
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

pub fn cluster() -> &'static PostgresCluster {
    let conf = Box::leak(Box::new(config::PostgresCluster{
        servers: vec![
            config::Postgres{
                database: TEST_DATABASE.to_string(),
                host: "127.0.0.1".to_string(),
                user: TEST_USER.to_string(),
                password: TEST_PASSWORD.to_string(),
                tls_host: "".to_string(),
                port: 5432,
                is_master: true,
                can_query: true,
                max_concurrent_transactions: 10,
                max_connections: 10,
                idle_timeout_seconds: 0,
                replicas: vec![],
                address: None,
                cluster: None
            }
        ],
        default: Default::default(),
        port: 5433,
        pinned_sessions: false,
        defer_begin: false,
        max_connections: 10,
        idle_timeout_seconds: 0,
        client_tls: Default::default(),
        backend_tls: Default::default(),
        tls_client_certificate: "".to_string(),
        tls_client_key: "".to_string(),
        tls_root_certificate: "".to_string(),
        tls_server_certificate: "".to_string(),
        tls_server_key: "".to_string(),
        tls_config: None,
        backend_tls_config: None
    }));
    conf.load().expect("invalid config");
    Box::leak(Box::new(PostgresCluster::new(&*conf)))
}

pub fn psql(connection_str: &str, mut password: &str) -> Child {
    let s = if connection_str.contains("user") {
        connection_str.to_string()
    } else {
        format!("user={} dbname={} {}", TEST_USER, TEST_DATABASE, connection_str)
    };
    if password.is_empty() {
        password = TEST_PASSWORD;
    }

    Command::new("psql")
        .stdin(Stdio::piped())
        .arg(s)
        .env("PGPASSWORD", password)
        .spawn()
        .expect("couldn't run psql")
}

#[macro_export]
macro_rules! register_scoped {
    ($plugin:expr, $plugin_ty:ident : $plugin_module:ident<$l:lifetime>($($arg:ident: $arg_ty:ty),*) -> $result:ty) => {
        crate::event_listener!($plugin, $plugin_ty:$plugin_module<$l>($($arg: $arg_ty),*) -> $result);

        unsafe {
            $plugin_module::configure();
        }

        struct PluginUninstall {}

        impl Drop for PluginUninstall {
           fn drop(&mut self) {
                unsafe { $plugin_module::clear() }
           }
        }

        let _cleanup = PluginUninstall{};
    };
}