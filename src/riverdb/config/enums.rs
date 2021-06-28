use serde::{Deserialize};

#[derive(Deserialize, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum TlsMode {
    // Invalid, used to indicate value was not explicitly set
    Invalid,
    // Disabled do not use TLS
    Disabled,
    // Prefer use TLS when the other side of the connection permits it, and verifies the issuing CA is trusted, and the hostname matches
    Prefer,
    // Required requires TLS and verifies the issuing CA is trusted, and the hostname matches
    Required,
}

impl Default for TlsMode {
    fn default() -> Self {
        TlsMode::Invalid
    }
}

