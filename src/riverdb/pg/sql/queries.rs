use std::fmt::{Debug, Formatter};

use crate::riverdb::Result;
use crate::riverdb::pg::protocol::{Tag, Messages};
use crate::riverdb::pg::sql::QueryType;

// TODO the type of object targeted by ALTER, DROP, CREATE queries
pub enum ObjectType {}

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

pub struct QueryParam {
    pub value: &'static str, // 'static is a lie, this is the lifetime of the parent Query
    pub ty: LiteralType,
    pub negated: bool,
    pub target_type: &'static str, // type 'string', 'string'::type, and CAST ( 'string' AS type )
}

pub struct Query {
    msgs: Messages,
    params_buf: String,
    normalized_query: String,
    query_type: QueryType,
    params: Vec<QueryParam>,
}

impl Query {
    pub fn new(msgs: Messages) -> Result<Self> {
        debug_assert_eq!(msgs.count(), 1);

        let mut normalized_query = String::new();
        let msg = msgs.first().unwrap();
        if msg.tag() == Tag::QUERY {
            let r = msg.reader();
            if let Ok(query) = r.read_str() {
                // TODO the real query normalization algorithm
                normalized_query = query.to_string().to_uppercase();
            }
        }

        let query_type = QueryType::from(normalized_query.trim());

        Ok(Self{msgs, params_buf: "".to_string(), normalized_query, query_type, params: vec![] })
    }

    pub fn query_type(&self) -> QueryType {
        self.query_type
    }

    /// Returns the object type affected for ALTER, CREATE, or DROP queries
    pub fn object_type(&self) -> ObjectType {
        todo!()
    }

    pub fn into_messages(self) -> Messages {
        self.msgs
    }

    pub fn normalized(&self) -> &str {
        &self.normalized_query
    }

    pub fn params(&self) -> &Vec<QueryParam> {
        &self.params
    }
}

impl Debug for Query {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.msgs, f)
    }
}