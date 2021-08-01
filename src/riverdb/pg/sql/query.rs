use std::fmt::{Debug, Formatter};

use crate::riverdb::pg::protocol::Message;

pub struct Query {
    msg: Message
}

impl Query {
    pub fn new(msg: Message) -> Self {
        Self{msg}
    }

    pub fn get_message(&self) -> &Message {
        &self.msg
    }

    pub fn into_message(self) -> Message {
        self.msg
    }
}

impl Debug for Query {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.msg, f)
    }
}