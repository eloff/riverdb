use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::net::{SocketAddr, IpAddr};
use std::str::FromStr;
use std::collections::hash_map::Entry;

use serde::{Deserialize};
use serde_yaml::Value;
use fnv::FnvHashMap;
use tracing::{info_span, info, debug};

use crate::riverdb::config::postgres::PostgresCluster;
use crate::riverdb::{Error, Result};


// Things that are not configurable, but might be one day
pub const SMALL_BUFFER_SIZE: u32 = 1024;
pub const CONNECT_TIMEOUT_SECONDS: u32 = 30;
pub const CHECK_TIMEOUTS_INTERVAL: u32 = 5 * 60;
pub const LISTEN_BACKLOG: u32 = 1024;

pub type ConfigMap = FnvHashMap<String, Value>;

#[derive(Deserialize, Default)]
pub struct Settings {
    /// config_path is the path of the loaded config file
    pub config_path: PathBuf,
    /// app_name is used as the application name to identify connected sessions to the Postgres databases if not provided by the client
    #[serde(default = "default_app_name")]
    pub app_name: String,
    /// host to listen on, defaults to 0.0.0.0
    #[serde(default = "default_host")]
    pub host: String,
    /// https_port is the port to listen on for HTTPS and WebSocket connections: default 443
    #[serde(default = "default_https_port")]
    pub https_port: u16,
    /// disable_keepalives disables use of TCP Keep Alives with long-running client-facing connections to detect and close broken connections. Default false.
    /// If you disable this, use client_idle_timeout_seconds to avoid exhausting server connections when clients disconnect without closing the connection.
    #[serde(default)]
    pub disable_keepalives: bool,
    /// reuseport is unix only, if true we create a listening socket per worker thread with SO_REUSEPORT options.
    /// this reduces lock contention in the kernel when calling accept. Default true.
    #[serde(default = "default_reuseport")]
    pub reuseport: bool,
    /// num_workers is the number of worker threads. Default is the number of hardware threads (hyperthreads) for the host.
    #[serde(default = "default_num_workers")]
    pub num_workers: u32,
    /// recv_buffer_size is the default size for (user-space) buffers used to read from TCP sockets
    #[serde(default = "default_recv_buffer_size")]
    pub recv_buffer_size: u32,
    /// max_http_connections to allow before rejecting new connections. Important to introduce back-pressure. Default 100,000.
    #[serde(default = "default_max_http_connections")]
    pub max_http_connections: u32,
    /// web_socket_idle_timeout_seconds closes connections that have been idle longer than this. Defaults to 20 minutes. 0 is disabled.
    #[serde(default = "default_web_socket_idle_timeout_seconds")]
    pub web_socket_idle_timeout_seconds: u32,
    /// postgres specific settings
    pub postgres: PostgresCluster,
    /// plugin settings
    pub plugins: Vec<ConfigMap>,
    #[serde(skip)]
    plugins_by_name: FnvHashMap<String, i32>,
}

fn default_num_workers() -> u32 { num_cpus::get() as u32 }
fn default_reuseport() -> bool { cfg!(unix) }
fn default_app_name() -> String { "riverdb".to_string() }
fn default_host() -> String { "0.0.0.0".to_string() }
const fn default_https_port() -> u16 { 443 }
const fn default_recv_buffer_size() -> u32 { 32 * 1024 }
const fn default_max_http_connections() -> u32 { 100000 }
const fn default_web_socket_idle_timeout_seconds() -> u32 { 20 * 60 }

pub(crate) static mut SETTINGS: MaybeUninit<Settings> = MaybeUninit::uninit();

#[cfg(test)]
thread_local! {
    static TEST_SETTINGS: std::cell::UnsafeCell<Settings> = std::cell::UnsafeCell::new(Settings::default());
}

pub fn conf() -> &'static Settings {
    unsafe {
        #[cfg(test)]
        {
            return &*test_config_mut();
        }
        &*SETTINGS.as_ptr()
    }
}

#[cfg(test)]
pub fn test_config_mut() -> &'static mut Settings {
    TEST_SETTINGS.with(|settings| {
        unsafe {
            &mut *settings.get()
        }
    })
}

impl Settings {
    pub(crate) fn load(&mut self, path: PathBuf) -> Result<()> {
        self.config_path = path;
        if self.recv_buffer_size < 4096 {
            self.recv_buffer_size = default_recv_buffer_size();
        }
        if self.recv_buffer_size > 1024*1024 {
            return Err(Error::new("recv_buffer_size cannot be > 1MB"));
        }
        self.recv_buffer_size = self.recv_buffer_size.next_power_of_two();

        let mut i = 0;
        for plugin in &mut self.plugins {
            if let Some(name) = plugin.get("name") {
                if let Value::String(name_str) = name {
                    self.plugins_by_name.insert(name_str.to_lowercase(), i);
                } else {
                    return Err(Error::new(format!("plugins name must be a string at index {}", i)));
                }
            } else {
                return Err(Error::new(format!("plugins entry missing name at index {}", i)));
            }

            i += 1;

            match plugin.entry("order".to_string()) {
                Entry::Occupied(_) => (),
                Entry::Vacant(entry) => {
                    // Set order to the 1-based index by default
                    entry.insert(Value::from(i));
                }
            }
        }

        self.postgres.load()
    }

    pub fn get_plugin_config(&'static self, name: &str) -> Option<&'static ConfigMap> {
        if let Some(i) = self.plugins_by_name.get(&name.to_lowercase()) {
            self.plugins.get(*i as usize)
        } else {
            None
        }
    }

    pub fn listen_address(&self) -> String {
        format!("{}:{}", self.host, self.https_port)
    }

    pub fn postgres_listen_address(&self) -> String {
        format!("{}:{}", self.host, self.postgres.port)
    }
}
