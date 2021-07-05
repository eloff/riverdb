pub use crate::riverdb::pg::protocol::{ErrorSeverity, ErrorCode, ErrorFieldTag};

pub struct PostgresError {
    pub code: ErrorCode,
    pub severity: ErrorSeverity,
    column_name: u32,
    constraint_name: u32,
    data_type_name: u32,
    detail: u32,
    file: u32,
    hint: u32,
    internal_position: u32,
    internal_query: u32,
    line: u32,
    message: u32,
    position: u32,
    routine: u32,
    schema_name: u32,
    table_name: u32,
    context: u32, // traceback, one entry per line, most recent first
}