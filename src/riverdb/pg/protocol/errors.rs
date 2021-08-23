use std::fmt;
use std::fmt::{Display, Formatter};


use strum::{EnumString};

use crate::riverdb::{Error, Result};

#[derive(EnumString, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug)]
#[strum(serialize_all = "UPPERCASE")]
#[repr(u8)]
pub enum ErrorSeverity {
    Log,
    Info,
    Debug,
    Notice,
    Warning,
    Error,
    Panic,
    Fatal,
}

impl ErrorSeverity {
    pub fn as_str(&self) -> &'static str {
        match *self {
            ErrorSeverity::Fatal => "Fatal",
            ErrorSeverity::Panic => "Panic",
            ErrorSeverity::Error => "Error",
            ErrorSeverity::Warning => "Warning",
            ErrorSeverity::Notice => "Notice",
            ErrorSeverity::Debug => "Debug",
            ErrorSeverity::Info => "Info",
            ErrorSeverity::Log => "Log",
        }
    }
}

impl Display for ErrorSeverity {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Default for ErrorSeverity {
    fn default() -> Self {
        ErrorSeverity::Log
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct ErrorFieldTag(u8);

impl ErrorFieldTag {
    pub const NULL_TERMINATOR: ErrorFieldTag = ErrorFieldTag::new_unchecked(0);
    pub const LOCALIZED_SEVERITY: ErrorFieldTag = ErrorFieldTag::new_unchecked('S' as u8);
    pub const SEVERITY: ErrorFieldTag = ErrorFieldTag::new_unchecked('V' as u8);
    pub const CODE: ErrorFieldTag = ErrorFieldTag::new_unchecked('C' as u8);
    pub const MESSAGE: ErrorFieldTag = ErrorFieldTag::new_unchecked('M' as u8);
    pub const MESSAGE_DETAIL: ErrorFieldTag = ErrorFieldTag::new_unchecked('D' as u8);
    pub const MESSAGE_HINT: ErrorFieldTag = ErrorFieldTag::new_unchecked('H' as u8);
    pub const POSITION: ErrorFieldTag = ErrorFieldTag::new_unchecked('P' as u8);
    pub const INTERNAL_POSITION: ErrorFieldTag = ErrorFieldTag::new_unchecked('p' as u8);
    pub const INTERNAL_QUERY: ErrorFieldTag = ErrorFieldTag::new_unchecked('q' as u8);
    pub const WHERE: ErrorFieldTag = ErrorFieldTag::new_unchecked('W' as u8);
    pub const SCHEMA_NAME: ErrorFieldTag = ErrorFieldTag::new_unchecked('s' as u8);
    pub const TABLE_NAME: ErrorFieldTag = ErrorFieldTag::new_unchecked('t' as u8);
    pub const COLUMN_NAME: ErrorFieldTag = ErrorFieldTag::new_unchecked('c' as u8);
    pub const DATA_TYPE_NAME: ErrorFieldTag = ErrorFieldTag::new_unchecked('d' as u8);
    pub const CONSTRAINT_NAME: ErrorFieldTag = ErrorFieldTag::new_unchecked('n' as u8);
    pub const FILE: ErrorFieldTag = ErrorFieldTag::new_unchecked('F' as u8);
    pub const LINE: ErrorFieldTag = ErrorFieldTag::new_unchecked('L' as u8);
    pub const ROUTINE: ErrorFieldTag = ErrorFieldTag::new_unchecked('R' as u8);

    pub fn new(b: u8) -> Result<Self> {
        let tag = Self::new_unchecked(b);
        tag.check().and(Ok(tag))
    }

    pub const fn new_unchecked(b: u8) -> Self {
        ErrorFieldTag(b)
    }

    pub fn check(&self) -> Result<()> {
        match *self {
            ErrorFieldTag::NULL_TERMINATOR |
            ErrorFieldTag::LOCALIZED_SEVERITY |
            ErrorFieldTag::SEVERITY |
            ErrorFieldTag::CODE |
            ErrorFieldTag::MESSAGE |
            ErrorFieldTag::MESSAGE_DETAIL |
            ErrorFieldTag::MESSAGE_HINT |
            ErrorFieldTag::POSITION |
            ErrorFieldTag::INTERNAL_POSITION |
            ErrorFieldTag::INTERNAL_QUERY |
            ErrorFieldTag::WHERE |
            ErrorFieldTag::SCHEMA_NAME |
            ErrorFieldTag::TABLE_NAME |
            ErrorFieldTag::COLUMN_NAME |
            ErrorFieldTag::DATA_TYPE_NAME |
            ErrorFieldTag::CONSTRAINT_NAME |
            ErrorFieldTag::FILE |
            ErrorFieldTag::LINE |
            ErrorFieldTag::ROUTINE => Ok(()),
            _ => Err(Error::protocol_error(format!("unknown error field tag {}", self.0))),
        }
    }

    pub fn as_u8(&self) -> u8 {
        self.0
    }
}

impl Display for ErrorFieldTag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let name = match *self {
            ErrorFieldTag::NULL_TERMINATOR => "null terminator",
            ErrorFieldTag::LOCALIZED_SEVERITY => "localized severity",
            ErrorFieldTag::SEVERITY => "severity",
            ErrorFieldTag::CODE => "code",
            ErrorFieldTag::MESSAGE => "message",
            ErrorFieldTag::MESSAGE_DETAIL => "message detail",
            ErrorFieldTag::MESSAGE_HINT => "message hint",
            ErrorFieldTag::POSITION => "position",
            ErrorFieldTag::INTERNAL_POSITION => "internal position",
            ErrorFieldTag::INTERNAL_QUERY => "internal query",
            ErrorFieldTag::WHERE => "where",
            ErrorFieldTag::SCHEMA_NAME => "schema name",
            ErrorFieldTag::TABLE_NAME => "table name",
            ErrorFieldTag::COLUMN_NAME => "column name",
            ErrorFieldTag::DATA_TYPE_NAME => "data type name",
            ErrorFieldTag::CONSTRAINT_NAME => "constraint name",
            ErrorFieldTag::FILE => "file",
            ErrorFieldTag::LINE => "line",
            ErrorFieldTag::ROUTINE => "routine",
            _ => { return f.write_fmt(format_args!("unknown error field tag {}", self.0)); },
        };
        f.write_str(name)
    }
}