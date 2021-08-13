use std::slice::Iter;
use std::convert::TryInto;

use crate::riverdb::common::{Error, Result};
use crate::riverdb::pg::protocol::{Messages, MessageReader, Tag};
use std::iter::{Cloned, Map};


const FIELD_DESCRIPTION_SIZE: u32 = 3*4 + 3*2;

pub struct RowDescription {
    msg: Messages,
    fields: Vec<FieldOffset>,
}

impl RowDescription {
    pub fn new(msg: Messages) -> Result<Self> {
        let m = msg.first().unwrap();
        assert_eq!(m.tag(), Tag::ROW_DESCRIPTION);

        let r = m.reader();
        let num_fields = r.read_i16();
        let mut fields = Vec::with_capacity(num_fields as usize);
        for i in 0..num_fields as usize {
            let mut offset = r.tell();
            let field_name = r.read_str()?;
            let name_len = field_name.len() as u32 + 1;
            fields.push(FieldOffset::new(offset, name_len));
            offset += name_len + FIELD_DESCRIPTION_SIZE;
            r.seek(offset)?;
        }

        Ok(Self{
            msg,
            fields,
        })
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn get(&self, index: usize) -> Option<FieldDescription> {
        self.fields.get(index).cloned().map(|off| FieldDescription::new(self.msg.as_slice(), off))
    }

    pub fn into_message(self) -> Messages {
        self.msg
    }
}

impl Default for RowDescription {
    fn default() -> Self {
        Self{
            msg: Messages::default(),
            fields: Vec::new(),
        }
    }
}

pub struct FieldDescription<'a> {
    data: &'a [u8],
    offset: FieldOffset,
}

impl<'a> FieldDescription<'a> {
    pub fn new(data: &'a [u8], offset: FieldOffset) -> Self {
        Self{
            data,
            offset
        }
    }

    /// Returns the field name.
    pub fn name(&self) -> Result<&str> {
        let name = &self.data[self.offset.offset()..self.offset.name_end()-1];
        std::str::from_utf8(name).map_err(Error::from)
    }

    /// Returns the object ID of the table if the field can be identified as a column of a specific table, otherwise 0.
    pub fn table_oid(&self) -> i32 {
        let start = self.offset.name_end();
        i32::from_be_bytes((&self.data[start..start+4]).try_into().unwrap())
    }

    /// Returns the attribute number of the column, if the field can be identified as a table column, otherwise 0.
    pub fn column_attribute_num(&self) -> i16 {
        let start = self.offset.name_end() + 4;
        i16::from_be_bytes((&self.data[start..start+2]).try_into().unwrap())
    }

    /// Returns the object ID of the field's data type.
    pub fn type_oid(&self) -> i32 {
        let start = self.offset.name_end() + 6;
        i32::from_be_bytes((&self.data[start..start+4]).try_into().unwrap())
    }

    /// Returns the data type size (see pg_type.typlen). Note that negative values denote variable-width types.
    pub fn type_len(&self) -> i16 {
        let start = self.offset.name_end() + 10;
        i16::from_be_bytes((&self.data[start..start+2]).try_into().unwrap())
    }

    /// Returns the type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
    pub fn type_mod(&self) -> i32 {
        let start = self.offset.name_end() + 12;
        i32::from_be_bytes((&self.data[start..start+4]).try_into().unwrap())
    }

    /// Returns the format code being used for the field. Currently will be zero (text) or one (binary).
    /// In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
    pub fn format_code(&self) -> FormatCode {
        let start = self.offset.name_end() + 16;
        match self.data.get(start).unwrap() {
            0 => FormatCode::Text,
            1 => FormatCode::Binary,
            _ => FormatCode::Binary,
        }
    }
}

#[derive(Copy, Clone)]
pub struct FieldOffset(u32);

impl FieldOffset {
    /// Constructs a new FieldOffset. offset is the offset to the start of the field description
    /// from the beginning of the message (from the tag byte). name_len is the length of the field
    /// name at offset, including the terminating null byte.
    /// Panics if offset is greater than 2^24 or name_len is > 255.
    pub fn new(offset: u32, name_len: u32) -> Self {
        assert!(offset < (1<<24));
        assert!(name_len < 256);
        Self((name_len << 24) | offset)
    }

    /// Returns the offset of the field description in a RowDescription message
    pub fn offset(&self) -> usize {
        (self.0 & 0xffffff) as usize
    }

    /// Returns the length of the field name, including the terminating null byte.
    pub fn name_len(&self) -> usize {
        (self.0 >> 24) as usize
    }

    pub fn name_end(&self) -> usize {
        self.offset() + self.name_len()
    }
}

#[repr(u8)]
pub enum FormatCode {
    Text = 0,
    Binary = 1
}