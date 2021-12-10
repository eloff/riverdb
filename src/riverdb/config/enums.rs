use serde::{Deserialize};

/// TlsMode is an enum of the supported TLS settings for the PostgreSQL connection.
/// Used for both backend (to db server) and client connections (clients connected to this server.)
#[derive(Deserialize, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum TlsMode {
    /// Invalid, used to indicate value was not explicitly set
    Invalid,
    /// Disabled do not use TLS
    Disabled,
    /// Prefer use TLS when the other side of the connection permits it, and verifies the issuing CA is trusted, and the hostname matches
    Prefer,
    /// Required requires TLS and verifies the issuing CA is trusted, and the hostname matches
    Required,
    /// DangerouslyUnverifiedCertificates requires TLS but does not verify the issuing CA or hostname.
    /// DO NOT USE in production! This only exists for facilitating testing/troubleshooting.
    DangerouslyUnverifiedCertificates,
}

impl Default for TlsMode {
    fn default() -> Self {
        TlsMode::Invalid
    }
}

