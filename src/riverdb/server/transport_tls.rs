use std::io;
use std::result::Result;

use rustls::{IoState, ClientConnection, ServerConnection, Connection, Reader, Writer};


pub enum TransportTls {
    NoTls,
    Client(ClientConnection),
    Server(ServerConnection),
}

impl TransportTls {
    pub const fn new() -> Self {
        Self::NoTls
    }

    pub fn new_client(conn: ClientConnection) -> Self {
        Self::Client(conn)
    }

    pub fn new_server(conn: ServerConnection) -> Self {
        Self::Server(conn)
    }

    pub fn wants_write(&self) -> bool {
        match self {
            TransportTls::NoTls => false,
            TransportTls::Client(c) => c.wants_write(),
            TransportTls::Server(c) => c.wants_write(),
        }
    }

    pub fn wants_read(&self) -> bool {
        match self {
            TransportTls::NoTls => false,
            TransportTls::Client(c) => c.wants_read(),
            TransportTls::Server(c) => c.wants_read(),
        }
    }

    pub fn is_handshaking(&self) -> bool {
        match self {
            TransportTls::NoTls => false,
            TransportTls::Client(c) => c.is_handshaking(),
            TransportTls::Server(c) => c.is_handshaking(),
        }
    }

    pub fn read_tls(&mut self, rd: &mut dyn io::Read) -> io::Result<usize> {
        match self {
            TransportTls::NoTls => panic!("not a tls connection"),
            TransportTls::Client(c) => c.read_tls(rd),
            TransportTls::Server(c) => c.read_tls(rd),
        }
    }

    pub fn write_tls(&mut self, wr: &mut dyn io::Write) -> io::Result<usize> {
        match self {
            TransportTls::NoTls => panic!("not a tls connection"),
            TransportTls::Client(c) => c.write_tls(wr),
            TransportTls::Server(c) => c.write_tls(wr),
        }
    }

    pub fn complete_io<T: io::Read + io::Write>(&mut self, rdwr: &mut T) -> io::Result<(usize, usize)> {
        match self {
            TransportTls::NoTls => panic!("not a tls connection"),
            TransportTls::Client(c) => c.complete_io(rdwr),
            TransportTls::Server(c) => c.complete_io(rdwr),
        }
    }

    pub fn reader(&mut self) -> Reader {
        match self {
            TransportTls::NoTls => panic!("not a tls connection"),
            TransportTls::Client(c) => c.reader(),
            TransportTls::Server(c) => c.reader(),
        }
    }

    pub fn writer(&mut self) -> Writer {
        match self {
            TransportTls::NoTls => panic!("not a tls connection"),
            TransportTls::Client(c) => c.writer(),
            TransportTls::Server(c) => c.writer(),
        }
    }

    pub fn process_new_packets(&mut self) -> Result<IoState, rustls::Error> {
        match self {
            TransportTls::NoTls => panic!("not a tls connection"),
            TransportTls::Client(c) => c.process_new_packets(),
            TransportTls::Server(c) => c.process_new_packets(),
        }
    }
}

impl Default for TransportTls {
    fn default() -> Self {
        TransportTls::NoTls
    }
}