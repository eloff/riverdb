use crate::riverdb::pg::protocol::Message;
use crate::riverdb::pg::protocol::MessageReader;

#[derive(Clone)]
pub struct StartupParams {
    pub params: Vec<Message>,
}

impl StartupParams {
    pub fn new(params: Vec<Message>) -> Self {
        Self{params}
    }

    pub fn get(&self, k: &str) -> Option<&str>
    {
        for msg in &self.params {
            let reader = MessageReader::new(&msg);
            if let Ok(key) = reader.read_str() {
                if k == key {
                    return Some(""); //reader.read_str().ok();
                }
            }
        }
        None
    }

    // TODO add a (&str, &str) iterator
}

impl Default for StartupParams {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}