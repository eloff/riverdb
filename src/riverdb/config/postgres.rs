use serde::{Deserialize};

use crate::riverdb::config::enums::TlsMode;
use crate::riverdb::common::{Error, Result};

#[derive(Deserialize)]
pub struct PostgresCluster {
    pub servers: Vec<Postgres>,
    /// default values used to replace any empty/omitted value for each Postgres config struct
    pub default: Postgres,
    /// port to listen on for PostgreSQL connections: default 5432
    #[serde(default = "default_port")]
    pub port: u16,
    /// pinned_sessions prevents release of the backend db connection until the session ends. Default false.
    /// Enabling this means that every connection to riverdb that's issued a query is backed 1-to-1 by a
    /// connection to the database, which hurts performance. It's not recommended to change this setting.
    /// This will also prevent on_route_partition from being called after the first query in a session.
    #[serde(default)]
    pub pinned_sessions: bool,
    /// defer_begin = false requires that transactions are backed 1-to-1 with a backend db transaction.
    /// Default false. If this is true, a BEGIN transaction may be deferred in READ COMMITTED or
    /// lower isolation levels until the first query that would modify the database or take locks.
    /// This means shorter duration transactions and allows SELECTs (but not SELECT FOR UPDATE) at
    /// the start of the transaction to be executed on replicas. There are some small differences in
    /// behavior, for example because datetime functions return the time as of the start of the transaction.
    /// Also SELECt queries that invoke impure functions that modify the database need to be manually
    /// tagged as being a write operation.
    #[serde(default)]
    pub defer_begin: bool,
    /// max_connections to allow before rejecting new connections. Important to introduce back-pressure. Default 10,000.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32
}

const fn default_port() -> u16 { 5432 }
const fn default_max_connections() -> u32 { 10000 }

#[derive(Deserialize)]
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
    /// Port to connect to, defaults to 5432
    #[serde(default = "default_port")]
    pub port: u16,
    /// backend_tls TLS preference between River DB and PostgreSQL, defaults to prefer
    #[serde(default)]
    pub backend_tls: TlsMode,
    /// is_master is set to true if this isn't inside a replicas vec
    #[serde(skip_deserializing)]
    pub is_master: bool,
    /// max_concurrent_transactions is the maximum number of db connections with open transactions permitted, defaults to 80.
    #[serde(default = "default_max_concurrent_transactions")]
    pub max_concurrent_transactions: u32,
    /// max_connections is the total maximum number of db connections for one-off queries and transactions, defaults to 100.
    #[serde(default = "default_max_db_connections")]
    pub max_connections: u32,
    /// replicas are other Postgres servers that host read-only replicas of this database
    pub replicas: Vec<Postgres>
}

fn default_host() -> String { "localhost".to_string() }
const fn default_max_concurrent_transactions() -> u32 { 80 }
const fn default_max_db_connections() -> u32 { 100 }

impl PostgresCluster {
    pub(crate) fn load(&mut self) -> Result<()> {
        for server in &mut self.servers {
            if let Err(e) = server.load(&self.default, true) {
                return Err(e);
            }
        }
        Ok(())
    }
}

impl Postgres {
    pub(crate) fn load(&mut self, defaults: &Postgres, is_master: bool) -> Result<()> {
        self.is_master = is_master;
        if self.database == "" {
            self.database = defaults.database.clone();
        }
        if self.host == "" {
            self.host = defaults.host.clone();
        }
        if self.user == "" {
            self.user = defaults.user.clone();
        }
        if self.port == 0 {
            self.port = defaults.port;
        }
        if let TlsMode::Invalid = self.backend_tls {
            self.backend_tls = defaults.backend_tls;
            if let TlsMode::Invalid = self.backend_tls {
                return Err(Error::new("tls mode not set"));
            }
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
        for replica in &mut self.replicas {
            if let Err(e) = replica.load(defaults, false) {
                return Err(e);
            }
        }
        Ok(())
    }
}