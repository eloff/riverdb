use std::fmt::Debug;

use strum::Display;

/// An enum of SQL transaction isolation modes
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

