use strum::Display;
use std::str::FromStr;

#[derive(Display, Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum TransactionType {
    None = 0,
    Default = 1,
    ReadOnly = 2,
    Snapshot = 3,
    ReadUncommitted = 4,
    ReadCommitted = 5,
    RepeatableRead = 6,
    Serializable = 7,
}

impl TransactionType {
    pub fn parse_from_query(normalized_query: &str) -> Self {
        if normalized_query.contains("READ ONLY") {
            Self::ReadOnly
        } else if let Some(i) = normalized_query.find("COMMITTED") {
            if (&normalized_query[..i]).ends_with("UN") {
                Self::ReadUncommitted
            } else {
                Self::ReadCommitted
            }
        } else if normalized_query.contains("REPEATABLE READ") {
            Self::RepeatableRead
        } else if normalized_query.contains("SERIALIZABLE") {
            Self::Serializable
        } else if normalized_query.contains("SNAPSHOT") {
            Self::Snapshot
        } else {
            Self::Default
        }
    }
}

impl Default for TransactionType {
    fn default() -> Self {
        TransactionType::None
    }
}