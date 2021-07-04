use bytes::{BytesMut, Buf};

use rustls::Connection;

use crate::riverdb::common::{Result};
use crate::riverdb::pg::protocol::Message;


pub struct MessageParser {
    pub last_bytes_read: usize
}

impl MessageParser {
    pub fn new() -> Self {
        Self {
            last_bytes_read: 0,
        }
    }

    pub fn next<'a>(&'a mut self) -> Option<Result<Message<'a>>> {
        todo!()
    }

    pub fn bytes_mut(&mut self) -> &mut BytesMut {
        todo!();
    }
}