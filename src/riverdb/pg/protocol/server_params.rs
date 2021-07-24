use std::fmt::Write;
use std::mem::{ManuallyDrop, transmute_copy};

use bytes::{BytesMut, BufMut, Bytes, Buf};

use crate::riverdb::pg::protocol::Message;
use crate::riverdb::pg::protocol::MessageReader;


pub struct ServerParams {
    params: Vec<Bytes>,
    buffer: Option<BytesMut>,
}

impl ServerParams {
    pub fn new<Iter: Iterator<Item=Message>>(params: Iter) -> Self
    {
        let params = params.map(|m| {
            let start = m.body_start();
            let mut buf = m.into_bytes();
            buf.split_off(start as usize)
        }).collect();
        Self{params, buffer: None}
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

    pub fn iter(&self) -> ParamsIter {
        ParamsIter::new(&self.params)
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
        Self::new(std::iter::empty())
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