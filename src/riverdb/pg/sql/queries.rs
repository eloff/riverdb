use std::fmt::{Debug, Formatter};

use crate::riverdb::Result;
use crate::riverdb::pg::protocol::{Tag, Messages};
use crate::riverdb::pg::sql::QueryType;
use std::ops::Range;
use crate::riverdb::pg::sql::normalize::QueryNormalizer;

// TODO the type of object targeted by ALTER, DROP, CREATE queries
pub enum ObjectType {}

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub enum LiteralType {
    Null,
    String,
    EscapeString,
    UnicodeString,
    DollarString,
    Integer,
    Numeric,
    BitString,
    Boolean
}

#[derive(Eq, PartialEq, Debug)]
pub struct QueryParam {
    pub pos: u32, // start position in buffer
    pub len: u32, // length
    pub ty: LiteralType,
    pub negated: bool,
    // pub target_type: &'a str, // type 'string', 'string'::type, and CAST ( 'string' AS type )
}

#[derive(Clone, Copy)]
pub struct QueryTag {
    pub key_pos: u32,
    pub key_len: u32,
    pub val_pos: u32,
    pub val_len: u32,
}

impl QueryTag {
    pub const fn new() -> Self {
        Self{
            key_pos: 0,
            key_len: 0,
            val_pos: 0,
            val_len: 0
        }
    }

    pub fn key_eq_ignore_ascii_case(&self, bytes: &[u8], key: &str) -> bool {
        if self.key_len() == key.len() {
            let this_key = &bytes[self.key_range()];
            // Safety: we checked msg was valid utf8 when we normalized it in new()
            key.eq_ignore_ascii_case(unsafe { std::str::from_utf8_unchecked(this_key) })
        } else {
            false
        }
    }
    
    pub fn key_len(&self) -> usize {
        self.key_len as usize
    }

    pub fn key_range(&self) -> Range<usize> {
        Range{ start: self.key_pos as usize, end: (self.key_pos + self.key_len) as usize }
    }

    pub fn value_range(&self) -> Range<usize> {
        Range{ start: self.val_pos as usize, end: (self.val_pos + self.val_len) as usize }
    }
}

pub struct QueryInfo {
    pub params_buf: String,
    pub normalized: String,
    pub ty: QueryType,
    pub params: Vec<QueryParam>
}

impl QueryInfo {
    pub const fn new() -> Self {
        Self{
            params_buf: String::new(),
            normalized: String::new(),
            ty: QueryType::Other,
            params: Vec::new(),
        }
    }
}

pub struct Query {
    msgs: Messages,
    query: QueryInfo,
    tags: Vec<QueryTag>, // indices that point into msgs.as_slice()
}

impl Query {
    pub fn new(msgs: Messages) -> Result<Self> {
        debug_assert_eq!(msgs.count(), 1);

        let msg = msgs.first().unwrap();
        let (query, tags) = if msg.tag() == Tag::QUERY {
            let normalizer = QueryNormalizer::new(&msg);
            normalizer.normalize()?
        } else {
            (QueryInfo::new(), Vec::new())
        };

        Ok(Self{msgs, query, tags })
    }

    pub fn query_type(&self) -> QueryType {
        self.query.ty
    }

    /// Returns the object type affected for ALTER, CREATE, or DROP queries
    pub fn object_type(&self) -> ObjectType {
        todo!()
    }

    pub fn into_messages(self) -> Messages {
        self.msgs
    }

    pub fn normalized(&self) -> &str {
        &self.query.normalized
    }

    pub fn params(&self) -> &Vec<QueryParam> {
        &self.query.params
    }

    /// Returns the value of the specified QueryParam which must have been returned by self.params()
    pub fn param(&self, param: QueryParam) -> &str {
        todo!()
    }

    /// Returns the value of the named tag (ascii case-insensitive) or None
    pub fn tag(&self, name: &str) -> Option<&str> {
        let bytes = self.msgs.as_slice();
        for tag in &self.tags {
            if tag.key_eq_ignore_ascii_case(bytes, name) {
                let val = &bytes[tag.value_range()];
                // Safety: we checked msg was valid utf8 when we normalized it in new()
                return Some(unsafe { std::str::from_utf8_unchecked(val) });
            }
        }
        None
    }
}

impl Debug for Query {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.msgs, f)
    }
}