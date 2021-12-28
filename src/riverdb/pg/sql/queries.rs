use std::fmt::{Debug, Formatter};
use std::ops::Range;

use crate::riverdb::Result;
use crate::riverdb::pg::protocol::{Tag, Messages};
use crate::riverdb::pg::sql::QueryType;
use crate::riverdb::pg::sql::normalize::QueryNormalizer;
use crate::riverdb::common::Range32;

/// The type of object targeted by DDL queries like ALTER, DROP, CREATE
#[derive(Eq, PartialEq, Debug, Clone, Copy)]
#[repr(u8)]
pub enum ObjectType {
    Other,
    Table,
    Database,
    Schema,
    Index,
    Sequence,
    Function,
}

impl ObjectType {
    /// Return the ObjectType affected by the query
    /// given it's normalized form and QueryType.
    /// Not Implemented (always returns Other.)
    pub fn parse(normalized_query: &str, ty: QueryType) -> ObjectType {
        match ty {
            QueryType::Alter | QueryType::Create | QueryType::Drop => ObjectType::Other, // TODO
            _ => ObjectType::Other, // TODO mostly Table with some exceptions
        }
    }
}

/// Represents type of a SQL literal value (string, null, numeric, integer, boolean)
#[derive(Eq, PartialEq, Debug, Clone, Copy)]
#[repr(u8)]
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

/// A QueryParam represents a query parameter or literal value
/// It's stored as offsets into QueryInfo params_buf, not the query itself.
#[derive(Eq, PartialEq, Debug)]
pub struct QueryParam {
    pub value: Range32, // range in buffer
    pub ty: LiteralType,
    pub negated: bool,
    pub target_type: Range32, // type name in casts: type 'string', 'string'::type, and CAST ( 'string' AS type )
}

impl QueryParam {
    /// Get the parameter value as a string from params_buf
    pub fn value<'a>(&self, params_buf: &'a str) -> &'a str {
        &params_buf[self.value.as_range()]
    }

    /// If there's a target type, get it as a string from the normalized query
    /// TODO Not implemented, always returns ""
    pub fn target_type<'a>(&self, normalized: &'a str) -> &'a str {
        if self.target_type.is_empty() {
            ""
        } else {
            &normalized[self.target_type.as_range()]
        }
    }
}

/// A QueryTag represents a key=value pair that can be
/// included inside a c-style /* comment */ in the query to
/// provide information to middleware tools like riverdb
/// or for logging. It stores offsets to the key and value
/// in the query body.
#[derive(Clone, Copy, Debug)]
pub struct QueryTag {
    pub key: Range32,
    pub val: Range32,
}

impl QueryTag {
    /// Create a new, empty QueryTag
    pub const fn new() -> Self {
        Self{
            key: Range32::default(),
            val: Range32::default(),
        }
    }

    /// Check if the QueryTag key in msg_body matches the passed key, ascii case-insensitively
    pub fn key_eq_ignore_ascii_case(&self, msg_body: &[u8], key: &str) -> bool {
        if self.key_len() == key.len() {
            let stored_key = self.key(msg_body);
            key.eq_ignore_ascii_case(stored_key)
        } else {
            false
        }
    }

    /// The length of the key
    pub fn key_len(&self) -> usize {
        debug_assert!(self.key.end >= self.key.start);
        (self.key.end - self.key.start) as usize
    }

    /// Get the key from the given message body
    pub fn key<'a>(&self, msg_body: &'a [u8]) -> &'a str {
        // Safety: we checked msg was valid utf8 when we normalized it in Query::new()
        unsafe {
            std::str::from_utf8_unchecked(&msg_body[self.key.as_range()])
        }
    }

    /// Get the value from the given message body
    pub fn value<'a>(&self, msg_body: &'a [u8]) -> &'a str {
        // Safety: we checked msg was valid utf8 when we normalized it in QUery::new()
        unsafe {
            std::str::from_utf8_unchecked(&msg_body[self.val.as_range()])
        }
    }
}

impl Default for QueryTag {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents info about a parsed SQL query.
/// It's normalized form, parameters, and type.
pub struct Query {
    pub params_buf: String,
    pub normalized: String,
    pub ty: QueryType,
    /// Not Implemented (always is set to Other.)
    pub object_ty: ObjectType,
    pub params: Vec<QueryParam>,
    pub next: Option<Box<Query>>
}

impl Query {
    /// Create a new, empty QueryInfo
    pub const fn new() -> Self {
        Self{
            params_buf: String::new(),
            normalized: String::new(),
            ty: QueryType::Other,
            object_ty: ObjectType::Other,
            params: Vec::new(),
            next: None,
        }
    }

    /// Return the query type.
    pub fn query_type(&self) -> QueryType { self.ty }

    /// Returns the object type affected for ALTER, CREATE, or DROP queries
    /// Not Implemented (always returns Other.)
    pub fn object_type(&self) -> ObjectType {
        self.object_ty
    }

    /// Returns the normalized query. Keywords are made uppercase
    /// and query parameters are replaced with $N placeholders.
    /// All whitespace is collapsed to single spaces.
    ///
    /// Note: currently the algorithm is not perfect, it uppercases
    /// tables, columns, and other identifiers, and it can confuse
    /// a unary - with subtraction in some cases if whitespace is unusual.
    /// These are known limitations that will be addressed in a future release.
    pub fn normalized(&self) -> &str {
        &self.normalized
    }

    /// Get a Vec of the QueryParams for the query parameters and constants
    pub fn params(&self) -> &Vec<QueryParam> {
        &self.params
    }

    /// Returns the value of the specified QueryParam which must have been returned by self.params()
    pub fn param(&self, param: &QueryParam) -> &str {
        param.value(self.params_buf.as_str())
    }
}

/// Represents a single wire message containing one or more SQL queries
pub struct QueryMessage {
    msgs: Messages,
    query: Query,
    pub tags: Vec<QueryTag>, // indices that point into msgs.as_slice()
}

impl QueryMessage {
    /// Create a new Query object from a Messages buffer where the first
    /// message contains the SQL query.
    pub fn new(msgs: Messages) -> Result<Self> {
        debug_assert_eq!(msgs.count(), 1);

        let msg = msgs.first().unwrap();
        let mut tags: Vec<QueryTag> = Vec::new();
        let query = if msg.tag() == Tag::QUERY {
            let normalizer = QueryNormalizer::new(&msg);
            normalizer.normalize(&mut tags)?
        } else {
            Query::new()
        };

        Ok(Self{msgs, query, tags})
    }

    /// Return true if this query is actually multiple queries separated by ;
    /// See 53.2.2.1 in https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4
    pub fn is_multi_query(&self) -> bool {
        self.query.next.is_some()
    }

    /// Return the query.
    pub fn query(&self) -> &Query {
        &self.query
    }

    /// Return the underlying Messages buffer containing the query
    pub fn into_messages(self) -> Messages {
        self.msgs
    }

    /// Returns the value of the named tag (ascii case-insensitive) or None
    pub fn tag(&self, name: &str) -> Option<&str> {
        let msg_body = self.msgs.as_slice();
        for tag in &self.tags {
            if tag.key_eq_ignore_ascii_case(msg_body, name) {
                return Some(tag.value(msg_body));
            }
        }
        None
    }
}

impl Debug for QueryMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.msgs, f)
    }
}