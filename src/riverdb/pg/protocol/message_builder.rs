use bytes::{BytesMut, BufMut};

use crate::riverdb::pg::protocol::{Tag, Messages, ServerParams};
use crate::riverdb::pg::protocol::message_parser::MIN_MESSAGE_LEN;
use crate::riverdb::common::bytes_to_slice_mut;


/// A wrapper around a mutable byte buffer (BytesMut) for creating
/// one or more PostgreSQL wire protocol messages.
pub struct MessageBuilder {
    data: BytesMut,
    start: usize, // start position of current Message being built
}

impl MessageBuilder {
    /// Create a new builder object
    pub fn new(tag: Tag) -> Self {
        let mut builder = MessageBuilder {
            data: BytesMut::with_capacity(256), // typically we build short messages
            start: 0,
        };
        builder.add_new(tag);
        builder
    }

    /// Reserve at least additional_size bytes in the mutable buffer
    pub fn reserve(&mut self, additional_size: usize) {
        self.data.reserve(additional_size)
    }

    /// Return a mutable BytesMut reference to the internal buffer
    pub fn bytes_mut(&mut self) -> &mut BytesMut {
        &mut self.data
    }

    /// Return a mutable byte slice &[u8] up to the capacity of the buffer.
    /// This is unsafe because data from [len, capacity) may be unitialized.
    /// Do not read from any unwritten part of this returned slice.
    pub unsafe fn as_slice_mut(&mut self) -> &mut [u8] {
        bytes_to_slice_mut(&mut self.data)
    }

    /// Set the length of the internal buffer (calls BytesMut::set_len)
    /// Must have written to all bytes up to the new length.
    pub unsafe fn set_len(&mut self, len: usize) {
        self.data.set_len(len)
    }

    /// Get the length of the written part of the internal buffer.
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

    /// Complete the message by writing the length field with the current
    /// message length.
    fn complete_message(&mut self) {
        let mut len = self.len();
        if len - self.start < MIN_MESSAGE_LEN as usize {
            // This is possible if creating an UNTAGGED message and calling finish()
            // without writing any data first. That's not a valid use case.
            panic!("Message too short");
        }
        unsafe {
            let mut pos = self.start;
            len -= pos;
            if *self.data.get_unchecked(self.start) != Tag::UNTAGGED.as_u8() {
                pos += 1;
                len -= 1;
            }
            let mut dest = &mut self.as_slice_mut()[pos..];
            dest.put_i32(len as i32);
        }
    }

    /// Write a single byte.
    pub fn write_byte(&mut self, b: u8) {
        self.data.put_u8(b);
    }

    /// Write a string, including a trailing null terminating byte.
    pub fn write_str(&mut self, s: &str) {
        self.write_bytes(s.as_bytes());
        self.write_byte(0);
    }

    /// Write a slice of bytes.
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes);
    }

    /// Write a big-endian 16 bit value
    pub fn write_i16(&mut self, i: i16) {
        self.data.put_i16(i);
    }

    /// Write a big-endian 32 bit value
    pub fn write_i32(&mut self, i: i32) {
        self.data.put_i32(i);
    }

    /// Write a ServerParams object in Postgres wire protocol.
    /// Note the caller is responsible for writing the terminating null byte.
    pub fn write_params(&mut self, params: &ServerParams) {
        for (k, v) in params.iter() {
            self.write_str(k);
            self.write_str(v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_one() {
        let mut mb = MessageBuilder::new(Tag::AUTHENTICATION_OK);
        mb.write_i32(34343434);
        mb.write_i16(1212);
        mb.write_byte(250);
        mb.write_str("foo");
        mb.write_bytes("bar".as_bytes());
        let msgs = mb.finish();

        const MSG_LEN: u32 = 1+4+4+2+1+4+3;
        assert_eq!(msgs.len(), MSG_LEN);
        let msg = msgs.first().unwrap();
        assert_eq!(msg.len(), MSG_LEN);
        assert_eq!(msg.tag(), Tag::AUTHENTICATION_OK);
        let mut r = msg.reader();
        assert_eq!(r.read_i32(), 34343434);
        assert_eq!(r.read_i16(), 1212);
        assert_eq!(r.read_byte(), 250);
        assert_eq!(r.read_str().unwrap(), "foo");
        assert_eq!(r.read_bytes(3).unwrap(), "bar".as_bytes());
    }

    #[test]
    fn test_build_many() {
        let mut mb = MessageBuilder::new(Tag::AUTHENTICATION_OK);
        mb.write_i32(42);

        mb.add_new(Tag::PARAMETER_STATUS);
        mb.write_str("foo");
        mb.write_str("bar");

        mb.add_new(Tag::PARAMETER_STATUS);
        mb.write_str("some_key");
        mb.write_str("a value");

        mb.add_new(Tag::BACKEND_KEY_DATA);
        mb.write_i32(123456789);
        mb.write_i32(987654321);

        mb.add_new(Tag::READY_FOR_QUERY);
        mb.write_byte('I' as u8);
        let msgs = mb.finish();
        let mut it = msgs.iter(0);

        let mut msg = it.next().unwrap();
        assert_eq!(msg.tag(), Tag::AUTHENTICATION_OK);
        assert_eq!(msg.len(), 9);
        assert_eq!(msg.reader().read_i32(), 42);

        msg = it.next().unwrap();
        assert_eq!(msg.tag(), Tag::PARAMETER_STATUS);
        assert_eq!(msg.len(), 13);
        let mut r = msg.reader();
        assert_eq!(r.read_str().unwrap(), "foo");
        assert_eq!(r.read_str().unwrap(), "bar");

        msg = it.next().unwrap();
        assert_eq!(msg.tag(), Tag::PARAMETER_STATUS);
        assert_eq!(msg.len(), 22);
        let mut r = msg.reader();
        assert_eq!(r.read_str().unwrap(), "some_key");
        assert_eq!(r.read_str().unwrap(), "a value");

        msg = it.next().unwrap();
        assert_eq!(msg.tag(), Tag::BACKEND_KEY_DATA);
        let mut r = msg.reader();
        assert_eq!(r.read_i32(), 123456789);
        assert_eq!(r.read_i32(), 987654321);

        msg = it.next().unwrap();
        assert_eq!(msg.tag(), Tag::READY_FOR_QUERY);
        assert_eq!(msg.reader().read_byte(), 'I' as u8);
    }
}