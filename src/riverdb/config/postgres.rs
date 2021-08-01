use serde::{Deserialize};

use crate::riverdb::config::enums::TlsMode;
use crate::riverdb::{Error, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use crate::riverdb::server::DangerousCertificateNonverifier;
use std::path::Path;
use rustls::{Certificate, PrivateKey};
use std::io::BufReader;
use std::fs::File;

#[derive(Deserialize, Default)]
pub struct PostgresCluster {
    pub servers: Vec<Postgres>,
    /// default values used to replace any empty/omitted value for each Postgres config struct
    #[serde(default)]
    pub default: Postgres,
    /// port to listen on for PostgreSQL connections: default 5432
    #[serde(default = "default_port")]
    pub port: u16,
    /// pinned_sessions prevents release of the backend db connection until the session ends. Default false.
    /// Enabling this means that every connection to riverdb that's issued a query is backed 1-to-1 by a
    /// connection to the database, which hurts performance. It's not recommended to change this setting.
    /// This will also prevent client_partition from being called after the first query in a session.
    #[serde(default)]
    pub pinned_sessions: bool,
    /// NOT IMPLEMENTED defer_begin = false requires that transactions are backed 1-to-1 with a backend db transaction.
    /// Default false. If this is true, a BEGIN transaction may be deferred in READ COMMITTED or
    /// lower isolation levels until the first query that would modify the database or take locks.
    /// This means shorter duration transactions and allows SELECTs (but not SELECT FOR UPDATE) at
    /// the start of the transaction to be executed on replicas or served from cache.
    /// There are some small differences in behavior, for example because datetime functions return
    /// the time as of the start of the transaction. Also SELECT queries that invoke impure functions
    /// that modify the database need to be manually tagged as being a write operation.
    #[serde(default)]
    pub defer_begin: bool,
    #[serde(default)]
    /// Issue begin/set/set local queries immediately, do not buffer them until another command/query
    /// is received. A lot of frameworks will start a transaction at the beginning of a request,
    /// and then burn time parsing/validating input before attempting to run a query.
    /// So we reduce the time a transaction is open for (and a backend connection is unavailable.)
    /// Defaults to false (buffering is enabled.)
    pub unbuffered_begin: bool,
    /// max_connections to allow before rejecting new connections. Important to introduce back-pressure. Default 10,000.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// idle_timeout_seconds is the number of seconds a client connection can be idle before it is closed. Default 0 (no timeout).
    #[serde(default)]
    pub idle_timeout_seconds: u32,
    /// client_tls TLS preference between clients and River DB, defaults to disabled
    #[serde(default)]
    pub client_tls: TlsMode,
    /// backend_tls TLS preference between River DB and PostgreSQL, defaults to disabled
    #[serde(default)]
    pub backend_tls: TlsMode,
    /// tls_client_certificate is the client authentication certificate sent from River DB to Postgres
    /// The value can be the inlined certificate, or a file path from which to load it.
    #[serde(default)]
    pub tls_client_certificate: String,
    /// tls_client_key is the client private key used with a TLS connection from River DB to Postgres
    /// The value can be the inlined certificate, or a file path from which to load it.
    #[serde(default)]
    pub tls_client_key: String,
    /// tls_root_certificate are additional certificates to add to the trusted certificate roots for validating the Postgres server certificate
    /// The value can be the inlined key, or a file path from which to load it.
    #[serde(default)]
    pub tls_root_certificate: String,
    /// tls_server_certificate is the certificate chain used for tls connections between the clients and River DB
    /// The value can be the inlined certificate, or a file path from which to load it.
    #[serde(default)]
    pub tls_server_certificate: String,
    /// tls_server_key is the server private key used with a TLS connection from the clients to River DB
    /// The value can be the inlined key, or a file path from which to load it.
    #[serde(default)]
    pub tls_server_key: String,
    #[serde(skip)]
    pub tls_config: Option<Arc<rustls::ServerConfig>>,
    #[serde(skip)]
    pub backend_tls_config: Option<Arc<rustls::ClientConfig>>,
}

const fn default_port() -> u16 { 5432 }
const fn default_max_connections() -> u32 { 10000 }

#[derive(Deserialize, Default)]
pub struct Postgres {
    /// database to connect to
    pub database: String,
    /// host to connect to, defaults to localhost
    #[serde(default = "default_host")]
    pub host: String,
    /// user to connect with.
    /// This should usually be a superuser, if the login user is different we'll call SET ROLE to the login user.
    #[serde(default)]
    pub user: String,
    /// password if using password authentication
    #[serde(default)]
    pub password: String,
    /// tls_host is the hostname expected in the server's certificate, if different from host.
    #[serde(default)]
    pub tls_host: String,
    /// Port to connect to, defaults to 5432
    #[serde(default = "default_port")]
    pub port: u16,
    /// is_master is set to true if this isn't inside a replicas vec
    #[serde(skip)]
    pub is_master: bool,
    /// true if queries can be routed to this database. Set to false for failover only databases.
    pub can_query: bool,
    /// max_concurrent_transactions is the maximum number of db connections with open transactions permitted, defaults to 80.
    #[serde(default = "default_max_concurrent_transactions")]
    pub max_concurrent_transactions: u32,
    /// max_connections is the total maximum number of db connections for one-off queries and transactions, defaults to 100.
    #[serde(default = "default_max_db_connections")]
    pub max_connections: u32,
    /// idle_timeout_seconds is the number of seconds a client connection can be idle in the pool before it is closed. Default 30min. 0 is disabled.
    #[serde(default = "default_idle_timeout_seconds")]
    pub idle_timeout_seconds: u32,
    /// replicas are other Postgres servers that host read-only replicas of this database
    pub replicas: Vec<Postgres>,
    #[serde(skip)]
    pub address: Option<SocketAddr>,
    #[serde(skip)]
    pub cluster: Option<&'static PostgresCluster>,
}

fn default_host() -> String { "localhost".to_string() }
const fn default_max_concurrent_transactions() -> u32 { 80 }
const fn default_max_db_connections() -> u32 { 100 }
const fn default_idle_timeout_seconds() -> u32 { 30 * 60 }

impl PostgresCluster {
    pub(crate) fn load(&mut self) -> Result<()> {
        match self.client_tls {
            TlsMode::Invalid => {
                self.client_tls = TlsMode::Disabled;
            },
            TlsMode::Disabled => (),
            _ => {
                let b = rustls::server_config_builder_with_safe_defaults();
                let b = if let TlsMode::DangerouslyUnverifiedCertificates = self.client_tls {
                    b.with_client_cert_verifier(DangerousCertificateNonverifier::new())
                } else {
                    b.with_no_client_auth()
                }; // TODO add client certificate verification

                let server_certs = Path::new(self.tls_server_certificate.as_str());
                let server_key = Path::new(self.tls_server_key.as_str());

                if !server_certs.exists() {
                    return Err(Error::new("tls_server_certificate does not exist"));
                }

                if !server_key.exists() {
                    return Err(Error::new("tls_server_key does not exist"));
                }

                let mut r = BufReader::new(File::open(server_key)?);
                let certs: Vec<Certificate> = rustls_pemfile::certs(&mut r)?
                    .into_iter()
                    .map(|cert| Certificate(cert))
                    .collect();

                if certs.is_empty() {
                    return Err(Error::new("tls_server_certificate file does not contain any certificates"));
                }

                let mut r = BufReader::new(File::open(server_key)?);
                let mut keys = rustls_pemfile::rsa_private_keys(&mut r)?;
                if keys.is_empty() {
                    return Err(Error::new("tls_server_key file does not contain any keys"));
                }
                let key = PrivateKey(keys.pop().unwrap());

                self.tls_config = Some(Arc::new(b.with_single_cert(certs, key)?));
            }
        }

        match self.backend_tls {
            TlsMode::Invalid => {
                self.backend_tls = TlsMode::Disabled;
            },
            TlsMode::Disabled => (),
            _ => {
                let b = rustls::client_config_builder_with_safe_defaults();
                let backend_config = if let TlsMode::DangerouslyUnverifiedCertificates = self.backend_tls {
                    b.with_custom_certificate_verifier(DangerousCertificateNonverifier::new())
                        .with_no_client_auth()
                } else {
                    let mut root_store = rustls::RootCertStore::empty();
                    root_store.add_server_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.0);

                    if !self.tls_root_certificate.is_empty() {
                        let root_cert_path = Path::new(self.tls_root_certificate.as_str());
                        if !root_cert_path.exists() {
                            return Err(Error::new("tls_root_certificate does not exist"));
                        }

                        let mut r = BufReader::new(File::open(root_cert_path)?);
                        let certs = rustls_pemfile::certs(&mut r)?;
                        root_store.add_parsable_certificates(certs.as_slice());
                    }

                    let b = b.with_root_certificates(root_store, &[]);
                    b.with_no_client_auth() // TODO add client certificate if configured
                };

                self.backend_tls_config = Some(Arc::new(backend_config));
            }
        }

        let self_ptr = self as *mut PostgresCluster as *const PostgresCluster;
        for server in &mut self.servers {
            if let Err(e) = server.load(self_ptr, &self.default, true) {
                return Err(e);
            }
        }

        Ok(())
    }
}

impl Postgres {
    pub(crate) fn load(&mut self, cluster: *const PostgresCluster, defaults: &Postgres, is_master: bool) -> Result<()> {
        self.is_master = is_master;
        if self.database.is_empty() {
            self.database = defaults.database.clone();
        }
        if self.host.is_empty() {
            self.host = defaults.host.clone();
        }
        if self.tls_host.is_empty() {
            self.tls_host = self.host.clone();
        }
        if self.user.is_empty() {
            self.user = defaults.user.clone();
        }
        if self.port == 0 {
            self.port = defaults.port;
        }
        if self.max_connections == 0 {
            self.max_connections = defaults.max_connections;
            if self.max_connections == 0 {
                return Err(Error::new("max_connections cannot be 0"));
            }
        }
        if self.max_concurrent_transactions == 0 {
            self.max_concurrent_transactions = defaults.max_concurrent_transactions;
            if self.max_concurrent_transactions == 0 {
                self.max_concurrent_transactions = self.max_connections*4/5;
            }
        }

        self.address = Some(to_address(&self.host, self.port)?);

        // Safety: we're using a raw pointer here to get around a limitation in rusts borrow checker
        // the caller holds a &mut PostgresCluster, so having a &PostgresCluster here doesn't work
        // (even though we don't use it until after the caller returns.)
        self.cluster = Some(unsafe { &*cluster });
        for replica in &mut self.replicas {
            if let Err(e) = replica.load(cluster, defaults, false) {
                return Err(e);
            }
        }
        Ok(())
    }
}

fn to_address(host: &str, port: u16) -> Result<SocketAddr> {
    format!("{}:{}", host, port).parse().map_err(Error::from)
}