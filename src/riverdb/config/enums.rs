use serde::{Deserialize};

#[derive(Deserialize, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum TlsMode {
    Invalid,
    // Disabled do not use TLS
    Disabled,
    // Prefer use TLS when possible, don't verify the issuing CA
    Prefer,
    // Required requires TLS and verifies the issuing CA is trusted
    Required,
    // Strict requires TLS, verifies the issuing CA is trusted, and the hostname matches
    Strict
}

impl Default for TlsMode {
    fn default() -> Self {
        TlsMode::Invalid
    }
}

