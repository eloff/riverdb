use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::fs::File;
use std::env;

use serde::{Deserialize};
use tracing::{info_span, info, debug};

use crate::riverdb::config::postgres::PostgresCluster;
use crate::riverdb::common::{Error, Result};


#[derive(Deserialize)]
pub struct Settings {
    /// config_path is the path of the loaded config file
    config_path: PathBuf,
    /// app_name is used as the application name to identify connected sessions to the Postgres databases if not provided by the client
    #[serde(default = "default_app_name")]
    app_name: String,
    /// host to listen on, defaults to 0.0.0.0
    #[serde(default = "default_host")]
    host: String,
    /// https_port is the port to listen on for HTTPS and WebSocket connections: default 443
    #[serde(default = "default_https_port")]
    https_port: u16,
    /// disable_keepalives disables use of TCP Keep Alives with long-running client-facing connections to detect and close broken connections. Default false.
    /// If you disable this, use client_idle_timeout_seconds to avoid exhausting server connections when clients disconnect without closing the connection.
    #[serde(default)]
    disable_keepalives: bool,
    /// recv_buffer_size is the default size for (user-space) buffers used to read from TCP sockets
    #[serde(default = "default_recv_buffer_size")]
    recv_buffer_size: u32,
    /// max_http_connections to allow before rejecting new connections. Important to introduce back-pressure. Default 100,000.
    #[serde(default = "default_max_http_connections")]
    max_http_connections: u32,
    /// web_socket_idle_timeout_seconds closes connections that have been idle longer than this. Defaults to 20 minutes. 0 is disabled.
    #[serde(default = "default_web_socket_idle_timeout_seconds")]
    web_socket_idle_timeout_seconds: u32,
    /// postgres specific SETTINGS
    postgres: PostgresCluster,
}

fn default_app_name() -> String { "riverdb".to_string() }
fn default_host() -> String { "0.0.0.0".to_string() }
const fn default_https_port() -> u16 { 443 }
const fn default_recv_buffer_size() -> u32 { 32 * 1024 }
const fn default_max_http_connections() -> u32 { 100000 }
const fn default_web_socket_idle_timeout_seconds() -> u32 { 20 * 60 }

static mut SETTINGS: MaybeUninit<Settings> = MaybeUninit::uninit();

pub fn config() -> &'static Settings {
    // TODO in tests return a thread-local Settings
    unsafe {
        &*SETTINGS.as_ptr()
    }
}

// #[cfg(test)]
// pub fn test_config_mut() -> &'static mut Settings {
//     // TODO in tests return a thread-local Settings
// }

pub fn load_config() -> Result<()> {
    let _span = info_span!("loading config file");
    let config_path = find_config_file("riverdb.yaml")?;
    info!(config_path = %config_path.to_string_lossy().into_owned(), "found config file");
    let file = File::open(&config_path)?;

    let config = unsafe { &mut *SETTINGS.as_mut_ptr() };
    *config = serde_yaml::from_reader(file)?;
    config.load(config_path)
}

impl Settings {
    fn load(&mut self, path: PathBuf) -> Result<()> {
        self.config_path = path;
        if self.recv_buffer_size < 4096 {
            self.recv_buffer_size = default_recv_buffer_size();
        }
        if self.recv_buffer_size > 1024*1024 {
            return Err(Error::new("recv_buffer_size cannot be > 1MB"));
        }
        self.recv_buffer_size = self.recv_buffer_size.next_power_of_two();
        self.postgres.load()
    }
}

fn find_config_file(config_name: &str) -> Result<PathBuf> {
    // Use the full path given as the first command line argument
    if let Some(path) = env::args().skip(1).next() {
        debug!("using config_path passed on command line");
        return Ok(PathBuf::from(path));
    }

    // Check the current directory or any of its parents for config_name
    if let Ok(start) = env::current_dir() {
        let mut dir = start.as_path();
        while !dir.as_os_str().is_empty() {
            debug!("checking for config file in {}", dir.to_string_lossy());
            let fp = Path::join(dir, config_name);
            if fp.exists() {
                return Ok(fp);
            }
            if let Some(parent) = dir.parent() {
                dir = parent;
            } else {
                break;
            }
        }
    }

    // Check  ~/.config/riverdb/{config_name}
    let mut conf_path = Path::join(Path::new(".config/riverdb"), config_name);
    // HOME is required to be set on POSIX systems, but if it's not set we'll try ~/
    let home = env::var("HOME").unwrap_or_else(|_| "~/".to_string());
    conf_path = Path::join(Path::new(&home), conf_path);
    debug!("checking for config file in {}", conf_path.to_string_lossy());
    if conf_path.exists() {
        return Ok(conf_path);
    }

    // Check ~/.{config_name}
    conf_path = Path::join(Path::new(&home), ".".to_string() + config_name);
    debug!("checking for config file in {}", conf_path.to_string_lossy());
    if conf_path.exists() {
        return Ok(conf_path);
    }

    // Check /etc/riverdb/{config_name}
    conf_path = Path::join(Path::new("/etc/riverdb"), config_name);
    debug!("checking for config file in {}", conf_path.to_string_lossy());
    if conf_path.exists() {
        return Ok(conf_path);
    }

    Err(Error::new(format!("config file {} not found", config_name)))
}






