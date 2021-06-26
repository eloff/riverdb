use crate::riverdb::common::{Result, MaybeTlsStream};
use crate::riverdb::pg::protocol::Message;
use bytes::{BytesMut, Buf};

pub struct MessageParser {
    pub last_bytes_read: usize
}

impl MessageParser {
    pub fn new(buf: BytesMut) -> Self {
        Self {
            last_bytes_read: 0,
        }
    }

    pub async fn next<'a>(&'a mut self, stream: &mut MaybeTlsStream) -> Result<Option<Message<'a>>> {
        todo!()
    }
}