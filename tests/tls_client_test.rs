#![allow(unused_imports)]
#![allow(unused_variables)]

use tokio;
use tokio::net::TcpStream;

use riverdb::server::ClientTransport;
use std::error::Error;

#[tokio::test]
async fn test_tls_client() -> Result<(), Box<dyn Error>> {
    let s = TcpStream::connect("127.0.0.1:5432").await?;
    let t = ClientTransport::new(s, false);
    const SSL_REQUEST: &[u8] = &[0, 0, 0, 8, 4, 0xd2, 16, 47];
    let n = t.try_write(SSL_REQUEST).unwrap();
    assert_eq!(n, 8);
    t.readable().await?;
    let buf = [0u8; 1];
    let n = t.try_read(&buf[..]).unwrap();
    assert_eq!(n, 1);
    assert_eq!(buf[0], 'S' as u8);
    Ok(())
}
