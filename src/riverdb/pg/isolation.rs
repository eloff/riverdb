use std::fmt::Debug;

use strum::Display;

#[derive(Display, Debug, Copy, Clone)]
#[repr(u8)]
pub enum IsolationLevel {
    None,
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        IsolationLevel::None
    }
}

