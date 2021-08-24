use std::io;
use std::io::{Read, Write};

use std::sync::{Mutex, Arc};
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::atomic::Ordering::{Relaxed};
use std::convert::TryFrom;

use tokio::net::{TcpStream};
#[cfg(unix)]
use tokio::net::{UnixStream};
use tokio::io::{Interest, Ready};
use tracing::{warn, debug};
use rustls::{ClientConfig, ServerConfig, ClientConnection, ServerConnection, ServerName};

use crate::riverdb::{Error, Result};
use crate::riverdb::config::{TlsMode};
use crate::riverdb::server::transport_stream::{TransportStream, StreamReaderWriter, convert_io_result};
use crate::riverdb::server::transport_tls::TransportTls;
use crate::riverdb::common;


pub struct Transport {
    stream: TransportStream,
    tls: Mutex<TransportTls>,
    want_read: AtomicBool, // mirror for tls.lock().wants_read() outside of the mutex
    want_write: AtomicBool, // mirror for tls.lock().wants_write() outside of the mutex
    is_closing: AtomicBool,
    is_tls_protected: AtomicBool,
    last_active: AtomicU32,
}

impl Transport
{
    pub fn new(stream: TcpStream) -> Self {
        Transport{
            stream: TransportStream::new_tcp(stream),
            tls: Mutex::new(TransportTls::new()),
            want_read: Default::default(),
            want_write: Default::default(),
            is_closing: Default::default(),
            is_tls_protected: Default::default(),
            last_active: Default::default(),
        }
    }

    #[cfg(unix)]
    pub fn new_unix(unix_socket: UnixStream) -> Self {
        Transport{
            stream: TransportStream::new_unix(unix_socket),
            tls: Mutex::new(Default::default()),
            want_read: Default::default(),
            want_write: Default::default(),
            is_closing: Default::default(),
            is_tls_protected: Default::default(),
            last_active: Default::default(),
        }
    }

    pub fn is_tls(&self) -> bool {
        self.is_tls_protected.load(Relaxed)
    }

    /// is_closed returns true if the connection is not closed or in the process of closing
    pub fn is_closed(&self) -> bool {
        self.is_closing.load(Relaxed)
    }

    pub fn can_use_tls(&self) -> bool {
        !self.stream.is_unix()
    }

    /// is_handshaking is for testing only, it returns true if the TLS session is performing the handshake
    pub fn is_handshaking(&self) -> bool {
        self.tls.lock().unwrap().is_handshaking()
    }

    pub async fn ready(&self, interest: Interest) -> Result<Ready> {
        if self.is_closed() {
            return Err(Error::closed());
        }
        self.stream.ready(interest).await
    }

    pub fn wants_write(&self) -> bool {
        // If there is buffered ciphertext, want_write is true.
        self.want_write.load(Relaxed)
    }

    pub fn wants_read(&self) -> bool {
        self.want_read.load(Relaxed)
    }

    /// try_read functions like tokio::TcpStream::try_read, but the underlying stream might be TLS encrypted.
    /// If the underlying stream returns WouldBlock, this returns Ok(0).
    /// If the underlying stream is closed (internally returned Ok(0)),
    /// then this returns Error::closed(), subsequent calls to self.is_closed() will return true,
    /// and subsequent calls to try_read and try_write will immediately return Error::closed().
    /// This panics if buf.len() == 0.
    pub fn try_read(&self, buf: &mut [u8]) -> Result<usize> {
        assert_ne!(buf.len(), 0);
        if self.is_closed() {
            return Err(Error::closed());
        }

        let result = if self.is_tls_protected.load(Relaxed) {
            self.tls_read(buf)
        } else {
            self.stream.try_read(buf)
        };

        if result.is_ok() {
            self.last_active.store(common::coarse_monotonic_now(), Relaxed);
        }
        result
    }

    /// try_write functions like tokio::TcpStream::try_write, but the underlying stream might be TLS encrypted.
    /// If the underlying stream returns WouldBlock, this returns Ok(0).
    /// If the underlying stream is closed (internally returned Ok(0)),
    /// then this returns Error::closed(), subsequent calls to self.is_closed() will return true,
    /// and subsequent calls to try_read and try_write will immediately return Error::closed().
    /// This panics if buf.len() == 0.
    pub fn try_write(&self, buf: &[u8]) -> Result<usize> {
        assert_ne!(buf.len(), 0);
        if self.is_closed() {
            return Err(Error::closed());
        }

        let result = if self.is_tls_protected.load(Relaxed) {
            self.tls_write(buf)
        } else {
            self.stream.try_write(buf)
        };

        if result.is_ok() {
            self.last_active.store(common::coarse_monotonic_now(), Relaxed);
        }
        result
    }

    fn tls_read(&self, buf: &mut [u8]) -> Result<usize> {
        let mut session = self.tls.lock().map_err(Error::from)?;
        if session.wants_read() {
            let n = convert_io_result(session.read_tls(&mut StreamReaderWriter::new(&self.stream)))?;
            if n > 0 {
                // Reading some TLS data might have yielded new TLS
                // messages to process.  Errors from this indicate
                // TLS protocol problems and are fatal.
                session.process_new_packets().map_err(Error::from)?;
            }
        }

        // mirror this value while we hold the mutex
        // Relaxed because the mutex release below is a global barrier
        self.want_read.store(session.wants_read(), Relaxed);

        // Having read some TLS data, and processed any new messages,
        // we might have new plaintext as a result.
        convert_io_result(session.reader().read(buf))
    }

    fn tls_write(&self, buf: &[u8]) -> Result<usize> {
        let mut session = self.tls.lock().map_err(Error::from)?;
        if session.wants_write() {
            let _n = match session.write_tls(&mut StreamReaderWriter::new(&self.stream)) {
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        return Ok(0);
                    }
                    warn!(?e, "TLS write error");
                    return Err(Error::from(e));
                },
                Ok(n) => n,
            };
        }

        let result = convert_io_result(session.writer().write(buf));
        // mirror this value while we hold the mutex
        // Relaxed because the mutex release below is a global barrier
        self.want_write.store(session.wants_write(), Relaxed);
        result
    }

    async fn do_complete_io(&self, session: &mut TransportTls) -> Result<()> {
        let mut rdwr = StreamReaderWriter::new(&self.stream);
        loop {
            return match session.complete_io(&mut rdwr) {
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        let mut interest = Interest::READABLE;
                        if session.wants_write() {
                            if session.wants_read() {
                                interest.add(Interest::WRITABLE);
                            } else {
                                interest = Interest::WRITABLE;
                            }
                        }
                        self.ready(interest).await?;
                        continue;
                    }
                    warn!(?e, "io error");
                    Err(Error::from(e))
                },
                Ok(..) => Ok(()),
            };
        }
    }

    pub async fn upgrade_client(&self, config: Arc<ClientConfig>, _mode: TlsMode, _hostname: &str) -> Result<()> {
        #[cfg(unix)]
        if self.stream.is_unix() {
            panic!("cannot use tls over a unix socket");
        }
        let server_name = ServerName::try_from("hostname").map_err(|_|Error::new("invalid dns name"))?;
        let mut conn = TransportTls::new_client(ClientConnection::new(config, server_name).map_err(Error::new)?);
        self.do_complete_io(&mut conn).await?;
        // Relaxed because the mutex acquire/release below is a global barrier
        self.is_tls_protected.store(true, Relaxed);
        *self.tls.lock().map_err(Error::from)? = conn;
        Ok(())
    }

    pub async fn upgrade_server(&self, config: Arc<ServerConfig>, _mode: TlsMode) -> Result<()> {
        #[cfg(unix)]
        if self.stream.is_unix() {
            panic!("cannot use tls over a unix socket");
        }
        let mut conn = TransportTls::new_server(ServerConnection::new(config).map_err(Error::new)?);
        self.do_complete_io(&mut conn).await?;
        // Relaxed because the mutex acquire/release below is a global barrier
        self.is_tls_protected.store(true, Relaxed);
        *self.tls.lock().map_err(Error::from)? = conn;
        Ok(())
    }

    pub fn close(&self) {
        debug!("called close() on transport");
        self.is_closing.store(true, Relaxed);
        self.stream.close();
    }
}