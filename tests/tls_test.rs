mod common;

use tokio;
use tokio::net::TcpStream;

use std::error::Error;
use std::sync::Arc;

use riverdb::config::TlsMode;
use riverdb::server::{ClientTransport, DangerousCertificateNonverifier, ServerTransport};
use rustls::{PrivateKey, Certificate};

const SSL_REQUEST: &[u8] = &[0, 0, 0, 8, 4, 210, 22, 47];

#[tokio::test]
async fn test_tls_client_handshake() -> Result<(), Box<dyn Error>> {
    let s = TcpStream::connect("127.0.0.1:5432").await?;
    let t = ClientTransport::new(s, false);
    let n = t.try_write(SSL_REQUEST)?;
    assert_eq!(n, 8);
    t.readable().await?;
    let mut buf = [0u8; 1];
    let n = t.try_read(&mut buf[..])?;
    assert_eq!(n, 1);
    assert_eq!(buf[0], 'S' as u8);

    let conf = rustls::client_config_builder_with_safe_defaults()
        .with_custom_certificate_verifier(DangerousCertificateNonverifier::new())
        .with_no_client_auth();

    t.upgrade(Arc::new(conf), TlsMode::DangerouslyUnverifiedCertificates, "localhost").await?;
    assert!(!t.is_handshaking());
    Ok(())
}

#[tokio::test]
async fn test_tls_server_handshake() -> Result<(), Box<dyn Error>> {
    let listener = common::listener();
    let mut psql = common::psql(format!("host=localhost port={}", listener.local_addr().unwrap().port()).as_str());

    let (s, _) = listener.accept().await?;
    let t = ServerTransport::new(s, false);

    t.readable().await?;
    let mut buf = [0u8; 8];
    let n = t.try_read(&mut buf[..])?;
    assert_eq!(n, 8);
    assert_eq!(&buf[..], SSL_REQUEST);

    buf[0] = 'S' as u8;
    let n = t.try_write(&buf[..1])?;
    assert_eq!(n, 1);

    let mut certs: &[u8] = include_bytes!("testdata/test-ca/rsa/end.fullchain");
    let mut private_key: &[u8] = include_bytes!("testdata/test-ca/rsa/end.rsa");

    let certs = rustls_pemfile::certs(&mut certs)?
        .into_iter()
        .map(|cert| Certificate(cert))
        .collect();

    let mut keys = rustls_pemfile::rsa_private_keys(&mut private_key)?;
    assert!(!keys.is_empty());
    let key = PrivateKey(keys.pop().unwrap());

    let conf = rustls::server_config_builder_with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    t.upgrade(Arc::new(conf), TlsMode::DangerouslyUnverifiedCertificates).await?;
    assert!(!t.is_handshaking());
    psql.kill().await?;
    Ok(())
}
