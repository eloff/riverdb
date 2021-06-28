use bytes::{BytesMut, Buf};

use rustls::Connection;

use crate::riverdb::common::{Result};
use crate::riverdb::pg::protocol::Message;
use crate::riverdb::server::Transport;


pub struct MessageParser {
    pub last_bytes_read: usize
}

impl MessageParser {
    pub fn new(buf: BytesMut) -> Self {
        Self {
            last_bytes_read: 0,
        }
    }

    pub async fn next<'a, T: Connection>(&'a mut self, stream: &Transport<T>) -> Result<Option<Message<'a>>> {
        todo!()
    }
}