use crate::riverdb::pg::protocol::Tag;
use std::marker::PhantomData;

pub struct Message<'a> {
    data: *const u8, // start of underlying buffer
    len: u32, // initialized length of underlying buffer (cannot read beyond this)
    cap: u32, // allocated capacity of underlying buffer, if we allocated it, or 0 (we free data on drop if != 0)
    pos: u32, // track position for read_xxx methods
    read_past_end: bool, // true if we tried to read past the end of the message
    _phantom: PhantomData<&'a u8>, // for the drop checker and the borrow checker
}

impl<'a> Message<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Message{
            data: buf.as_ptr(),
            len: buf.len() as u32,
            cap: 0,
            pos: 0,
            read_past_end: false,
            _phantom: PhantomData,
        }
    }

    pub fn tag(&self) -> Tag {
        unimplemented!();
    }
}