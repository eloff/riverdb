use std::fmt::{Debug, Formatter};

use crate::riverdb::pg::protocol::{Tag, Message, MessageReader};

pub struct Query {
    msg: Message,
    normalized_query: String,
}

impl Query {
    pub fn new(msg: Message) -> Self {
        let mut normalized_query = String::new();
        if msg.tag() == Tag::QUERY {
            let r = MessageReader::new(&msg);
            if let Ok(query) = r.read_str() {
                // TODO the real query normalization algorithm
                normalized_query = query.to_string().to_uppercase();
            }
        }

        Self{msg, normalized_query}
    }

    pub fn message(&self) -> &Message {
        &self.msg
    }

    pub fn into_message(self) -> Message {
        self.msg
    }

    pub fn normalized(&self) -> &str {
        &self.normalized_query
    }
}

impl Debug for Query {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.msg, f)
    }
}