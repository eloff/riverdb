use std::io;
use std::io::Read;
use std::pin::Pin;
use std::sync::{Mutex, Arc};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;

use tokio::net::{TcpStream};
#[cfg(unix)]
use tokio::net::{UnixStream};
use tracing::{warn, info};
use rustls::{ClientConfig, ServerConfig, ClientConnection, ServerConnection, Connection, ServerName};

use crate::riverdb::common::{Result, Error};
use crate::riverdb::config::{TlsMode};
use crate::riverdb::server::transport_stream::{TransportStream, TransportStreamReader, TransportStreamWriter};
use std::convert::TryFrom;

pub struct Transport<TlsSession: Connection> {
    stream: TransportStream,
    tls: Mutex<Option<TlsSession>>,
    want_read: AtomicBool, // mirror for tls.lock().wants_read() outside of the mutex
    want_write: AtomicBool, // mirror for tls.lock().wants_write() outside of the mutex
    is_closing: AtomicBool,
    is_tls: AtomicBool,
    is_localhost: bool,
}

impl<TlsSession> Transport<TlsSession>
    where TlsSession: Connection
{
    pub fn new(stream: TcpStream, is_localhost: bool) -> Self {
        Transport{
            stream: TransportStream::new_tcp(stream),
            tls: Mutex::new(None),
            want_read: Default::default(),
            want_write: Default::default(),
            is_closing: Default::default(),
            is_tls: Default::default(),
            is_localhost,
        }
    }

    #[cfg(unix)]
    pub fn new_unix(unix_socket: UnixStream) -> Self {
        Transport{
            stream: TransportStream::new_unix(unix_socket),
            tls: Mutex::new(None),
            want_read: Default::default(),
            want_write: Default::default(),
            is_closing: Default::default(),
            is_tls: Default::default(),
            is_localhost: true,
        }
    }

    pub fn can_use_tls(&self) -> bool {
        !self.is_localhost
    }

    pub async fn readable(&self) -> Result<()> {
        if self.want_read.load(Relaxed) {
            return Ok(());
        }
        self.stream.readable().await
    }

    pub async fn writable(&self) -> Result<()> {
        // If there is buffered ciphertext, want_write is true.
        // In all cases though, we want to return only when the underlying socket is writable.
        self.stream.writable().await.map_err(Error::from)
    }

    pub fn wants_write(&self) -> bool {
        // If there is buffered ciphertext, want_write is true.
        self.want_write.load(Relaxed)
    }

    pub fn try_read(&self, buf: &mut [u8]) -> Result<usize> {
        if self.is_tls.load(Relaxed) {
            let mut _guard = self.tls.lock().map_err(Error::from)?;
            let mut session = _guard.as_mut().unwrap();
            if !session.wants_read() {
                match session.read_tls(&mut TransportStreamReader(&self.stream)) {
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            return Ok(0);
                        }
                        warn!(?e, "TLS read error");
                        return Err(Error::from(e));
                    },
                    Ok(0) => {
                        // EOF
                        info!("EOF reading from socket (remote end is closed)");
                        // relaxed because the mutex release below is a global barrier
                        self.is_closing.store(true, Relaxed);
                        return Err(Error::closed());
                    },
                    Ok(n) => {
                        // Reading some TLS data might have yielded new TLS
                        // messages to process.  Errors from this indicate
                        // TLS protocol problems and are fatal.
                        session.process_new_packets().map_err(Error::from)?;
                    }
                }
            }
            // Having read some TLS data, and processed any new messages,
            // we might have new plaintext as a result.
            let result = session.reader().read(buf).map_err(Error::from);
            // mirror this value while we hold the mutex
            // relaxed because the mutex release below is a global barrier
            self.want_read.store(session.wants_read(), Relaxed);
            return result;
        }

        self.stream.try_read(buf)
    }

    pub fn try_write(&self, buf: &[u8]) -> Result<usize> {
        if self.is_tls.load(Relaxed) {
            let mut _guard = self.tls.lock().map_err(Error::from)?;
            let mut session = _guard.as_mut().unwrap();
            let n = match session.write_tls(&mut TransportStreamWriter(&self.stream)) {
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        return Ok(0);
                    }
                    warn!(?e, "TLS write error");
                    return Err(Error::from(e));
                },
                Ok(n) => n,
            };
            // mirror this value while we hold the mutex
            // relaxed because the mutex release below is a global barrier
            self.want_write.store(session.wants_write(), Relaxed);
            return Ok(n);
        }

        self.stream.try_write(buf)
    }
}

impl Transport<ClientConnection> {
    pub async fn upgrade(&self, config: Arc<ClientConfig>, mode: TlsMode, hostname: &str) -> Result<()> {
        #[cfg(unix)]
        if self.stream.is_unix() {
            panic!("cannot use tls over a unix socket");
        }
        let server_name = ServerName::try_from("hostname").map_err(|_|Error::new("invalid dns name"))?;
        let conn = ClientConnection::new(config, server_name).map_err(Error::new)?;
        // relaxed because the mutex acquire/release below is a global barrier
        self.is_tls.store(true, Relaxed);
        *self.tls.lock().map_err(Error::from)? = Some(conn);
        Ok(())
    }
}

impl Transport<ServerConnection> {
    pub async fn upgrade(&self, config: Arc<ServerConfig>, mode: TlsMode) -> Result<()> {
        #[cfg(unix)]
        if self.stream.is_unix() {
            panic!("cannot use tls over a unix socket");
        }
        let conn = ServerConnection::new(config).map_err(Error::new)?;
        // relaxed because the mutex acquire/release below is a global barrier
        self.is_tls.store(true, Relaxed);
        *self.tls.lock().map_err(Error::from)? = Some(conn);
        Ok(())
    }
}