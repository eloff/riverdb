use bytes::{BytesMut, Buf};
use std::num::NonZeroU32;
use std::convert::TryInto;

use rustls::Connection;

use crate::riverdb::{Error, Result};
use crate::riverdb::pg::protocol::Message;
use crate::riverdb::config::conf;
use crate::riverdb::pg::protocol::Tag;

pub const MIN_MESSAGE_LEN: u32 = 5;

pub struct Header {
    pub tag: Tag,
    pub length: NonZeroU32,
}

impl Header {
    pub fn parse(bytes: &[u8]) -> Result<Option<Self>> {
        if (bytes.len() as u32) < MIN_MESSAGE_LEN {
            return Ok(None);
        }
        let tag = Tag::new(bytes[0])?;
        let len = u32::from_be_bytes((&bytes[1..5]).try_into().unwrap());
        Ok(Some(Header{
            tag,
            length: NonZeroU32::new(len).ok_or_else(|| Error::protocol_error("length of message frame cannot be 0"))?,
        }))
    }

    pub fn len(&self) -> u32 {
        let len = self.length.get();
        if self.tag == Tag::Untagged {
            len
        } else {
            len + 1
        }
    }
}

pub struct MessageParser {
    data: BytesMut,
}

impl MessageParser {
    pub fn new() -> Self {
        Self {
            data: BytesMut::with_capacity(conf().recv_buffer_size as usize),
        }
    }

    pub fn next(&mut self) -> Option<Result<Message>> {
        match Header::parse(self.data.chunk()) {
            Err(e) => Some(Err(e)),
            Ok(None) => None,
            Ok(Some(hdr)) => {
                let msg_len = hdr.len();
                if msg_len <= self.data.len() as u32 {
                    // We have the full message, split it off and return it
                    let msg = Message::new(self.data.split_to(msg_len as usize).freeze());
                    Some(Ok(msg))
                } else {
                    // We don't have the message, make sure buffer is large enough for it
                    self.data.reserve(msg_len as usize - self.data.len());
                    None
                }
            }
        }
    }

    pub fn bytes_mut(&mut self) -> &mut BytesMut {
        &mut self.data
    }
}