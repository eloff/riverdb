mod transport;
mod transport_stream;
mod certificate_verifier;

/// ClientTransport is used to establish an optionally TLS encrypted TCP session to a remote server
pub type ClientTransport = transport::Transport<rustls::ClientConnection>;
/// ServerTransport is used for an optionally TLS encrypted TCP session from our server to a remote client
pub type ServerTransport = transport::Transport<rustls::ServerConnection>;

pub use certificate_verifier::DangerousCertificateNonverifier;