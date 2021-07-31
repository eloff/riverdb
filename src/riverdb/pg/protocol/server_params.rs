use std::fmt::{Write, Debug, Formatter};
use std::mem::{ManuallyDrop, transmute_copy};

use bytes::{BytesMut, BufMut, Bytes, Buf};

use crate::riverdb::{Error, Result};
use crate::riverdb::pg::protocol::{MessageReader, Message, Tag};


pub struct ServerParams {
    params: Vec<Bytes>,
    buffer: Option<BytesMut>,
}

impl ServerParams {
    pub const fn new() -> Self {
        Self{params: Vec::new(), buffer: None}
    }

    pub fn from_startup_message(msg: &Message) -> Result<Self> {
        assert_eq!(msg.tag(), Tag::UNTAGGED);
        let mut start = msg.body_start() + 4;
        let mut buffer = BytesMut::from(&msg.as_slice()[start as usize..]);

        let msg = Message::new(buffer.split().freeze());
        let r = MessageReader::new_at(&msg, 0);

        let mut user: Option<&str> = None;
        let mut have_database = false;
        let mut params = Vec::new();
        start = 0;
        while start < r.len() {
            let name = r.read_str()?;
            let value = r.read_str()?;
            match name {
                "user" => user = Some(value),
                "database" => have_database = true,
                _ => (),
            }
            let end = r.tell();
            params.push(r.slice(start, end));
            start = end;
        }

        if user.is_none() {
            return Err(Error::new("user is a required parameter"));
        }

        let mut result = Self{
            params,
            buffer: Some(buffer),
        };

        if !have_database {
            result.add("database", user.unwrap());
        }

        Ok(result)
    }

    pub fn from_parameter_status_messages<Iter: Iterator<Item=Message>>(params: Iter) -> Result<Self>
    {
        let params = params.map(|m| {
            assert_eq!(m.tag(), Tag::PARAMETER_STATUS);
            let start = m.body_start();
            let r = MessageReader::new_at(&m, start);
            r.read_str()?;
            r.read_str()?;
            let mut buf = m.into_bytes();
            Ok(buf.split_off(start as usize))
        }).collect::<Result<Vec<Bytes>>>()?;
        Ok(Self{params, buffer: None})
    }

    pub fn add(&mut self, k: &str, v: &str) {
        let space_needed = k.len() + v.len() + 2;
        if self.buffer.is_none() || self.buffer.as_mut().unwrap().remaining_mut() < space_needed {
            self.buffer = Some(BytesMut::with_capacity(space_needed * 12));
        }

        // We don't write a full message, just the null-terminated key and value (message body)
        // This is fine, because we don't expose the Message at all, we only read the body.
        let buf = self.buffer.as_mut().unwrap();
        buf.write_str(k).unwrap();
        buf.write_char(0 as char).unwrap();
        buf.write_str(v).unwrap();
        buf.write_char(0 as char).unwrap();
        self.params.push(buf.split_to(space_needed).freeze());
    }

    pub fn set(&mut self, k: &str, v: &str) {
        for (i, buf) in self.params.iter().enumerate() {
            if let Some(key) = read_null_terminated_str(buf, 0) {
                if k == key {
                    // Add the new (k, v) pair to the end, and then swap it into params[i],
                    // removing the value at i.
                    self.add(k, v);
                    self.params.swap_remove(i);
                    return;
                }
            }
        }
        // The key doesn't exist, add it to the end
        self.add(k, v);
    }

    pub fn get<'a>(&'a self, k: &'_ str) -> Option<&'a str>
    {
        for buf in &self.params {
            if let Some(key) = read_null_terminated_str(buf, 0) {
                if k == key {
                    return read_null_terminated_str(buf, key.len()+1);
                }
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.params.len()
    }

    pub fn iter(&self) -> ParamsIter {
        ParamsIter::new(&self.params)
    }

    pub fn append_to(&self, bytes_vec: &mut Vec<Bytes>) {
        bytes_vec.extend(self.params.iter());
    }
}

impl Clone for ServerParams {
    fn clone(&self) -> Self {
        let mut copy = Self::default();
        for (k, v) in self.iter() {
            copy.add(k, v);
        }
        copy
    }
}

impl Default for ServerParams {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for ServerParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_char('{')?;
        let mut first = true;
        for (key, val) in self.iter() {
            if !first {
                f.write_str(", ");
            } else {
                first = true;
            }
            f.write_str(key)?;
            f.write_str(": ")?;
            f.write_str(val)?;
        }
        f.write_char('}')
    }
}

pub struct ParamsIter<'a> {
    params: &'a Vec<Bytes>,
    index: usize,
}

impl<'a> ParamsIter<'a> {
    pub fn new(params: &'a Vec<Bytes>) -> Self {
        Self{params, index: 0}
    }
}

impl<'a> Iterator for ParamsIter<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        let buf = self.params.get(self.index)?;
        let key = read_null_terminated_str(buf, 0)?;
        let val = read_null_terminated_str(buf, key.len()+1)?;
        Some((key, val))
    }
}

fn read_null_terminated_str(buf: &Bytes, start: usize) -> Option<&str> {
    let slice = &buf.chunk()[start..];
    if let Some(i) = memchr::memchr(0, slice) {
        std::str::from_utf8(&slice[..i]).ok()
    } else {
        None
    }
}