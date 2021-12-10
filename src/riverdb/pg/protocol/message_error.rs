use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

use crate::riverdb::common::change_lifetime;
use crate::riverdb::{Error, Result};
use crate::riverdb::pg::protocol::{Tag, Messages, MessageReader};
use crate::riverdb::pg::protocol::{ErrorSeverity, ErrorFieldTag};

/// PostgresError represents a decoded error from a Postgres server
/// It provides efficient access to each of the optional error fields.
pub struct PostgresError {
    msg: Messages,
    severity: ErrorSeverity,
    _severity: u32,
    _code: u32,
    _column_name: u32,
    _constraint_name: u32,
    _data_type_name: u32,
    _detail: u32,
    _file: u32,
    _hint: u32,
    _internal_position: u32,
    _internal_query: u32,
    _line: u32,
    _message: u32,
    _position: u32,
    _routine: u32,
    _schema_name: u32,
    _table_name: u32,
    _where: u32, // traceback, one entry per line, most recent first
}

impl PostgresError {
    /// Decode a Postgres error from the first Message in the
    /// given Messages buffer. Takes ownership of the buffer.
    pub fn new(msg: Messages) -> Result<Self> {
        let mut err = Self{
            msg,
            severity: ErrorSeverity::Error,
            _severity: 0,
            _code: 0,
            _column_name: 0,
            _constraint_name: 0,
            _data_type_name: 0,
            _detail: 0,
            _file: 0,
            _hint: 0,
            _internal_position: 0,
            _internal_query: 0,
            _line: 0,
            _message: 0,
            _position: 0,
            _routine: 0,
            _schema_name: 0,
            _table_name: 0,
            _where: 0
        };

        let m = err.msg.first().unwrap();
        match m.tag() {
            Tag::ERROR_RESPONSE | Tag::NOTICE_RESPONSE => (),
            _ => { return Err(Error::protocol_error("message not an error message")); }
        }

        let mut r = m.reader();
        loop {
            let field = ErrorFieldTag::new(r.read_byte())?;
            if field == ErrorFieldTag::NULL_TERMINATOR {
                // Is this a real null terminator, or did we read past the end?
                r.error()?;
                break;
            }
            let pos = r.tell();
            let val = r.read_str()?;
            if val.is_empty() {
                continue;
            }
            match field {
                ErrorFieldTag::NULL_TERMINATOR => unreachable!(),
                ErrorFieldTag::LOCALIZED_SEVERITY => (),
                ErrorFieldTag::SEVERITY => {
                    err._severity = pos;
                    err.severity = ErrorSeverity::from_str(val).unwrap_or(ErrorSeverity::default());
                },
                ErrorFieldTag::CODE => { err._code = pos; },
                ErrorFieldTag::MESSAGE => { err._message = pos; },
                ErrorFieldTag::MESSAGE_DETAIL => { err._detail = pos; },
                ErrorFieldTag::MESSAGE_HINT => { err._hint = pos; },
                ErrorFieldTag::POSITION => { err._position = pos; },
                ErrorFieldTag::INTERNAL_POSITION => { err._internal_position = pos; },
                ErrorFieldTag::INTERNAL_QUERY => { err._internal_query = pos; },
                ErrorFieldTag::WHERE => { err._where = pos; },
                ErrorFieldTag::SCHEMA_NAME => { err._schema_name = pos; },
                ErrorFieldTag::TABLE_NAME => { err._table_name = pos; },
                ErrorFieldTag::COLUMN_NAME => { err._column_name = pos; },
                ErrorFieldTag::DATA_TYPE_NAME => { err._data_type_name = pos; },
                ErrorFieldTag::CONSTRAINT_NAME => { err._constraint_name = pos; },
                ErrorFieldTag::FILE => { err._file = pos; },
                ErrorFieldTag::LINE => { err._line = pos; },
                ErrorFieldTag::ROUTINE => { err._routine = pos; },
                _ => (),
            }
        }
        Ok(err)
    }

    /// The error severity
    pub fn severity(&self) -> ErrorSeverity {
        self.severity
    }

    fn read_str_at(&self, pos: u32) -> &str {
        if pos == 0 {
            return "";
        }
        let m = &self.msg.first().unwrap();
        let mut r = MessageReader::new_at(m, pos);
        let s = r.read_str().expect("expected null-terminated string");
        // Safety: s isn't borrowed from m here, it's borrowed from self.msg
        unsafe { change_lifetime(s) }
    }

    /// The error severity name
    pub fn severity_name(&self) -> &str {
        self.read_str_at(self._severity)
    }

    /// The Postgres error code. See errors.rs in this package.
    pub fn code(&self) -> &str {
        self.read_str_at(self._code)
    }

    /// The column name
    pub fn column_name(&self) -> &str
    {
        self.read_str_at(self._column_name)
    }

    /// The constraint name
    pub fn constraint_name(&self) -> &str {
        self.read_str_at(self._constraint_name)
    }

    /// The data type name
    pub fn data_type_name(&self) -> &str {
        self.read_str_at(self._data_type_name)
    }

    /// The error detail
    pub fn detail(&self) -> &str {
        self.read_str_at(self._detail)
    }

    /// The file (Postgres C source file)
    pub fn file(&self) -> &str {
        self.read_str_at(self._file)
    }

    /// An error hint
    pub fn hint(&self) -> &str {
        self.read_str_at(self._hint)
    }

    /// The internal position
    pub fn internal_position(&self) -> &str {
        self.read_str_at(self._internal_position)
    }

    /// The internal query
    pub fn internal_query(&self) -> &str {
        self.read_str_at(self._internal_query)
    }

    /// The line number in the Postgres source file.
    pub fn line(&self) -> &str {
        self.read_str_at(self._line)
    }

    /// The error message
    pub fn message(&self) -> &str {
        self.read_str_at(self._message)
    }

    /// The error position
    pub fn position(&self) -> &str {
        self.read_str_at(self._position)
    }

    /// The routine (function) with the error
    pub fn routine(&self) -> &str {
        self.read_str_at(self._routine)
    }

    /// The schema name
    pub fn schema_name(&self) -> &str {
        self.read_str_at(self._schema_name)
    }

    /// The table name
    pub fn table_name(&self) -> &str {
        self.read_str_at(self._table_name)
    }

    /// The error context
    pub fn context(&self) -> &str {
        self.read_str_at(self._where)
    }

    /// Return the underlying Messages buffer
    pub fn into_messages(self) -> Messages {
        self.msg
    }
}

impl Display for PostgresError {
    /// Format the error as "$severity $code: $message"
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{} {}: {}", self.severity_name(), self.code(), self.message()))
    }
}

impl Debug for PostgresError {
    /// Format the error as "$severity $code: $message"
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl std::error::Error for PostgresError {}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_parse_error() {
        let error = &[0x45u8,0x00,0x00,0x00,0x85,0x53,0x46,0x41,0x54,0x41,0x4c,0x00,0x56,0x46,0x41,0x54,
            0x41,0x4c,0x00,0x43,0x30,0x38,0x50,0x30,0x31,0x00,0x4d,0x69,0x6e,0x76,0x61,0x6c,
            0x69,0x64,0x20,0x73,0x74,0x61,0x72,0x74,0x75,0x70,0x20,0x70,0x61,0x63,0x6b,0x65,
            0x74,0x20,0x6c,0x61,0x79,0x6f,0x75,0x74,0x3a,0x20,0x65,0x78,0x70,0x65,0x63,0x74,
            0x65,0x64,0x20,0x74,0x65,0x72,0x6d,0x69,0x6e,0x61,0x74,0x6f,0x72,0x20,0x61,0x73,
            0x20,0x6c,0x61,0x73,0x74,0x20,0x62,0x79,0x74,0x65,0x00,0x46,0x70,0x6f,0x73,0x74,
            0x6d,0x61,0x73,0x74,0x65,0x72,0x2e,0x63,0x00,0x4c,0x32,0x31,0x39,0x39,0x00,0x52,
            0x50,0x72,0x6f,0x63,0x65,0x73,0x73,0x53,0x74,0x61,0x72,0x74,0x75,0x70,0x50,0x61,
            0x63,0x6b,0x65,0x74,0x00,0x00];

        let err_msg = Messages::new(Bytes::from_static(error));
        let err = PostgresError::new(err_msg).expect("parsed error message");
        assert_eq!(err.severity(), ErrorSeverity::Fatal);
        assert_eq!(err.severity_name(), "FATAL");
        assert_eq!(err.code(), "08P01");
        assert_eq!(err.message(), "invalid startup packet layout: expected terminator as last byte");
        assert_eq!(err.file(), "postmaster.c");
        assert_eq!(err.line(), "2199");
        assert_eq!(err.routine(), "ProcessStartupPacket");
    }
}