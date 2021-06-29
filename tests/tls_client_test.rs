#![allow(unused_imports)]
#![allow(unused_variables)]

use tokio;
use tokio::net::TcpStream;

use std::error::Error;
use std::sync::Arc;

use riverdb::config::TlsMode;
use riverdb::server::ClientTransport;


#[tokio::test]
async fn test_tls_client() -> Result<(), Box<dyn Error>> {
    let s = TcpStream::connect("127.0.0.1:5432").await?;
    let t = ClientTransport::new(s, false);
    const SSL_REQUEST: &[u8] = &[0, 0, 0, 8, 4, 0xd2, 16, 47];
    let n = t.try_write(SSL_REQUEST).unwrap();
    assert_eq!(n, 8);
    t.readable().await?;
    let mut buf = [0u8; 1];
    let n = t.try_read(&mut buf[..]).unwrap();
    assert_eq!(n, 1);
    assert_eq!(buf[0], 'S' as u8);
    let conf = rustls::ClientConfig{};
    let snakeoil = include_bytes!("snakeoil.der");
    let root = webpki::TrustAnchor::try_from_cert_der(&snakeoil[..]).unwrap();
    conf.root_store.add_server_trust_anchors(webpki::TlsServerTrustAnchors(&[root]));
    t.upgrade(Arc::new(conf), TlsMode::Required, "localhost").await.unwrap();
    Ok(())
}
