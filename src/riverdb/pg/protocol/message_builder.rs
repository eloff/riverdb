use bytes::{BytesMut, Buf, BufMut};

use crate::riverdb::pg::protocol::{Tag, Messages, ServerParams};
use crate::riverdb::pg::protocol::message_parser::MIN_MESSAGE_LEN;
use crate::riverdb::common::bytes_to_slice_mut;
use std::convert::TryInto;

pub struct MessageBuilder {
    data: BytesMut,
    start: usize, // start position of current Message being built
}

impl MessageBuilder {
    pub fn new(tag: Tag) -> Self {
        let mut builder = MessageBuilder {
            data: BytesMut::with_capacity(256), // typically we build short messages
            start: 0,
        };
        builder.add_new(tag);
        builder
    }

    pub fn reserve(&mut self, additional_size: usize) {
        self.data.reserve(additional_size)
    }

    pub fn bytes_mut(&mut self) -> &mut BytesMut {
        &mut self.data
    }

    pub unsafe fn as_slice_mut(&mut self) -> &mut [u8] {
        bytes_to_slice_mut(&mut self.data)
    }

    pub unsafe fn set_len(&mut self, len: usize) {
        self.data.set_len(len)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Completes the Message by setting the message length field to the current length
    /// and returning the data as a Message, consuming self.
    pub fn finish(mut self) -> Messages {
        self.complete_message();
        Messages::new( self.data.freeze())
    }

    /// Completes the prior Message (if any) by setting the message length field
    /// and adds a new Message with tag after it.
    pub fn add_new(&mut self, tag: Tag) {
        let len = self.len();
        if len != 0 {
            self.complete_message();
            self.start = len;
        }
        if tag != Tag::UNTAGGED {
            self.data.put_u8(tag.as_u8());
        }
        self.data.put_i32(0);
    }

    fn complete_message(&mut self) {
        let mut len = self.len();
        if len - self.start < MIN_MESSAGE_LEN as usize {
            // This is possible if creating an UNTAGGED message and calling finish()
            // without writing any data first. That's not a valid use case.
            panic!("Message too short");
        }
        unsafe {
            let mut pos = self.start;
            if *self.data.get_unchecked(self.start) != Tag::UNTAGGED.as_u8() {
                pos += 1;
                len -= 1;
            }
            let mut dest = &mut self.as_slice_mut()[pos..];
            dest.put_i32(len as i32);
        }
    }

    pub fn write_byte(&mut self, b: u8) {
        self.data.put_u8(b);
    }

    pub fn write_str(&mut self, s: &str) {
        self.write_bytes(s.as_bytes());
        self.write_byte(0);
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes);
    }

    pub fn write_i16(&mut self, i: i16) {
        self.data.put_i16(i);
    }

    pub fn write_i32(&mut self, i: i32) {
        self.data.put_i32(i);
    }

    pub fn write_params(&mut self, params: &ServerParams) {
        for (k, v) in params.iter() {
            self.write_str(k);
            self.write_str(v);
        }
    }
}