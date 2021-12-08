use std::fmt::{Debug, Formatter};
use std::ops::Range;

use crate::riverdb::Result;
use crate::riverdb::pg::protocol::{Tag, Messages};
use crate::riverdb::pg::sql::QueryType;
use crate::riverdb::pg::sql::normalize::QueryNormalizer;
use crate::riverdb::common::Range32;

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
    pub value: Range32, // range in buffer
    pub ty: LiteralType,
    pub negated: bool,
    pub target_type: Range32, // type name in casts: type 'string', 'string'::type, and CAST ( 'string' AS type )
}

impl QueryParam {
    pub fn value<'a>(&self, src: &'a [u8]) -> &'a str {
        let val = &src[self.value.as_range()];
        // Safety: we checked this was valid utf8 when constructing the QueryParam
        unsafe { std::str::from_utf8_unchecked(val) }
    }

    pub fn target_type<'a>(&self, src: &'a [u8]) -> &'a str {
        let target_ty = &src[self.target_type.as_range()];
        // Safety: we checked this was valid utf8 when constructing the QueryParam
        unsafe { std::str::from_utf8_unchecked(target_ty) }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct QueryTag {
    pub key: Range32,
    pub val: Range32,
}

impl QueryTag {
    pub const fn new() -> Self {
        Self{
            key: Range32::default(),
            val: Range32::default(),
        }
    }

    pub fn key_eq_ignore_ascii_case(&self, bytes: &[u8], key: &str) -> bool {
        if self.key_len() == key.len() {
            let this_key = &bytes[self.key_range()];
            // Safety: we checked msg was valid utf8 when we normalized it in new()
            let stored_key = unsafe { std::str::from_utf8_unchecked(this_key) };
            key.eq_ignore_ascii_case(stored_key)
        } else {
            false
        }
    }
    
    pub fn key_len(&self) -> usize {
        debug_assert!(self.key.end >= self.key.start);
        (self.key.end - self.key.start) as usize
    }

    pub fn key_range(&self) -> Range<usize> {
        self.key.as_range()
    }

    pub fn value_range(&self) -> Range<usize> {
        self.val.as_range()
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
    pub tags: Vec<QueryTag>, // indices that point into msgs.as_slice()
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