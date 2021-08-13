use std::fmt::{Debug, Formatter};

use crate::riverdb::pg::protocol::{Tag, Messages, MessageReader};
use crate::riverdb::pg::sql::QueryType;

pub struct Query {
    msgs: Messages,
    normalized_query: String,
    query_type: QueryType,
}

impl Query {
    pub fn new(msgs: Messages) -> Self {
        let mut normalized_query = String::new();
        let msg = msgs.first().unwrap();
        if msg.tag() == Tag::QUERY {
            let r = msg.reader();
            if let Ok(query) = r.read_str() {
                // TODO the real query normalization algorithm
                normalized_query = query.to_string().to_uppercase();
            }
        }

        // TODO figure out the actual query type here
        Self{msgs, normalized_query, query_type: QueryType::Other}
    }

    pub fn query_type(&self) -> QueryType {
        self.query_type
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