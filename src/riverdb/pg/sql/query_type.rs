use strum::Display;

#[derive(Display, Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum QueryType {
    Other,
    Select,
    SelectInto,
    SelectWithLocking,
    Insert,
    Update,
    Delete,
    With,
    Begin, // includes START
    Rollback, // includes ABORT
    Commit,
    Show,
    Set, // SET ROLE, SET SESSION AUTHORIZATION, SET CONSTRAINTS
    SetSession,
    SetLocal,
    SetTransaction,
    Alter,
    Create,
    Call,
    Drop,
    Execute,
    Prepare,
    Cursor, // includes DECLARE, FETCH, MOVE, CLOSE
    Listen,
    Lock,
    Notify,
    Savepoint,
    ReleaseSavepoint,
    RollbackSavepoint,
    Truncate,
}