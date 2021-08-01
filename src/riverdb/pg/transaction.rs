use strum::Display;

#[derive(Display, Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum TransactionType {
    None = 0,
    ReadOnly = 1,
    ReadUncommitted = 2,
    ReadCommitted = 3,
    RepeatableRead = 4,
    Serializable = 5,
}

impl Default for TransactionType {
    fn default() -> Self {
        TransactionType::None
    }
}