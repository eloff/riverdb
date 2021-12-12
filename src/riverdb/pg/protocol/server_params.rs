use std::fmt::{Write, Debug, Formatter};
use std::slice::Iter;

use crate::riverdb::{Error, Result};
use crate::riverdb::pg::protocol::{MessageReader, Message, Tag};


/// A collection of server parameters as sent in the startup message on connect
pub struct ServerParams {
    params: Vec<(String, String)>,
}

impl ServerParams {
    pub const fn new() -> Self {
        Self{params: Vec::new()}
    }

    /// Parse the connection parameters from the startup message. Note the startup message
    /// is the first message sent on connection and doesn't have a tag byte.
    pub fn from_startup_message(msg: &Message<'_>) -> Result<Self> {
        assert_eq!(msg.tag(), Tag::UNTAGGED);
        let mut r = msg.reader();
        r.seek(r.tell() + 4)?; // skip the version number
        let start = msg.body_start() + 4;
        let mut r = MessageReader::new_at(&msg, start as u32);

        let mut result = Self::new();
        let mut user: Option<&str> = None;
        let mut have_database = false;
        while let Ok(name) = r.read_str() {
            if name.is_empty() {
                break; // the null-terminator at the end of the message
            }
            let value = r.read_str()?;
            match name {
                "user" => user = Some(value),
                "database" => have_database = true,
                _ => (),
            }
            result.add(name.to_string(), value.to_string());
        }

        if user.is_none() {
            return Err(Error::new("user is a required parameter"));
        }

        if !have_database {
            result.add("database".to_string(), user.unwrap().to_string());
        }

        Ok(result)
    }

    /// Add a new parameter to the collection, without checking if it exists first
    pub fn add(&mut self, k: String, v: String) {
        self.params.push((k, v));
    }

    /// Add or overwrite a parameter
    pub fn set(&mut self, k: String, v: String) {
        for (i, (key, _)) in self.params.iter().enumerate() {
            if &k == key {
                self.params.get_mut(i).unwrap().1 = v;
                return;
            }
        }
        // The key doesn't exist, add it to the end
        self.add(k, v);
    }

    /// Get a parameter (or None if it doesn't exist in the collection.)
    /// The passed key is compared case sensitively and should be lower case.
    pub fn get<'a>(&'a self, k: &'_ str) -> Option<&'a str>
    {
        for (key, val) in &self.params {
            if k == key.as_str() {
                return Some(val);
            }
        }
        None
    }

    /// Return the number of parameters
    pub fn len(&self) -> usize {
        self.params.len()
    }

    /// Return an iterator over the parameters
    pub fn iter(&self) -> Iter<(String, String)> {
        self.params.iter()
    }
}

impl Clone for ServerParams {
    /// Make a deep-copy of the ServerParams collection
    fn clone(&self) -> Self {
        Self{params: self.params.clone()}
    }
}

impl Default for ServerParams {
    /// Create a new empty collection
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for ServerParams {
    /// Format the ServerParams object as a map literal {key: value, ...}
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_char('{')?;
        let mut first = true;
        for (key, val) in self.iter() {
            if !first {
                f.write_str(", ")?;
            } else {
                first = false;
            }
            f.write_str(key)?;
            f.write_str(": ")?;
            f.write_str(val)?;
        }
        f.write_char('}')
    }
}