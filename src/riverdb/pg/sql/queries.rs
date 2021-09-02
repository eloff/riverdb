use std::fmt::{Debug, Formatter};

use crate::riverdb::pg::protocol::{Tag, Messages};
use crate::riverdb::pg::sql::QueryType;

pub enum ObjectType {}

pub struct Query {
    msgs: Messages,
    normalized_query: String,
    query_type: QueryType,
}

impl Query {
    pub fn new(msgs: Messages) -> Self {
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

        Self{msgs, normalized_query, query_type}
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
}

impl Debug for Query {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.msgs, f)
    }
}