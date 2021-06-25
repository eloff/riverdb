use crate::riverdb::common::{Result, MaybeTlsStream};
use crate::riverdb::pg::protocol::Message;
use bytes::BytesMut;

pub struct MessageParser {
    pub buf: BytesMut,
    pub last_bytes_read: usize
}

impl MessageParser {
    pub fn new(buf: BytesMut) -> Self {
        Self {
            buf,
            last_bytes_read: 0,
        }
    }

    pub fn next(&mut self, stream: &mut MaybeTlsStream) -> Result<Option<Message>> {
        todo!()
    }
}