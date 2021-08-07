use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

pub use crate::riverdb::{Error, Result};
pub use crate::riverdb::pg::protocol::{Tag, Message, MessageReader};
pub use crate::riverdb::pg::protocol::{ErrorSeverity, ErrorFieldTag};


pub struct PostgresError {
    pub msg: Message,
    pub severity: ErrorSeverity,
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
    pub fn new(msg: Message) -> Result<Self> {
        match msg.tag() {
            Tag::ERROR_RESPONSE | Tag::NOTICE_RESPONSE => (),
            _ => { return Err(Error::protocol_error("message not an error message")); }
        }

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

        let r = MessageReader::new(&err.msg);
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

    fn read_str_at(&self, pos: u32) -> &str {
        if pos == 0 {
            return "";
        }
        let r = MessageReader::new(&self.msg);
        r.seek(pos);
        r.read_str().expect("expected null-terminated string")
    }

    pub fn severity_name(&self) -> &str {
        self.read_str_at(self._severity)
    }

    pub fn code(&self) -> &str {
        self.read_str_at(self._code)
    }

    pub fn column_name(&self) -> &str
    {
        self.read_str_at(self._column_name)
    }

    pub fn constraint_name(&self) -> &str {
        self.read_str_at(self._constraint_name)
    }

    pub fn data_type_name(&self) -> &str {
        self.read_str_at(self._data_type_name)
    }

    pub fn detail(&self) -> &str {
        self.read_str_at(self._detail)
    }

    pub fn file(&self) -> &str {
        self.read_str_at(self._file)
    }

    pub fn hint(&self) -> &str {
        self.read_str_at(self._hint)
    }

    pub fn internal_position(&self) -> &str {
        self.read_str_at(self._internal_position)
    }

    pub fn internal_query(&self) -> &str {
        self.read_str_at(self._internal_query)
    }

    pub fn line(&self) -> &str {
        self.read_str_at(self._line)
    }

    pub fn message(&self) -> &str {
        self.read_str_at(self._message)
    }

    pub fn position(&self) -> &str {
        self.read_str_at(self._position)
    }

    pub fn routine(&self) -> &str {
        self.read_str_at(self._routine)
    }

    pub fn schema_name(&self) -> &str {
        self.read_str_at(self._schema_name)
    }

    pub fn table_name(&self) -> &str {
        self.read_str_at(self._table_name)
    }

    pub fn context(&self) -> &str {
        self.read_str_at(self._where)
    }

    pub fn into_message(self) -> Message {
        self.msg
    }
}

impl Display for PostgresError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{} {}: {}", self.severity_name(), self.code(), self.message()))
    }
}

impl Debug for PostgresError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl std::error::Error for PostgresError {}