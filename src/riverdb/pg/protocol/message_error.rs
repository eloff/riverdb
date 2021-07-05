use std::str::FromStr;

pub use crate::riverdb::{Error, Result};
pub use crate::riverdb::pg::protocol::{Tag, Message, MessageReader};
pub use crate::riverdb::pg::protocol::{ErrorSeverity, ErrorCode, ErrorFieldTag};

pub struct PostgresError {
    pub msg: Message,
    pub code: ErrorCode,
    pub severity: ErrorSeverity,
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
        if msg.tag()? != Tag::ExecuteOrError {
            return Err(Error::protocol_error("message not an error message"));
        }

        let mut err = Self{
            msg: msg,
            code: ErrorCode::SuccessfulCompletion,
            severity: ErrorSeverity::Error,
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
            if field == ErrorFieldTag::NullTerminator {
                // Is this a real null terminator, or did we read past the end?
                r.error()?;
                break;
            }
            let pos = r.tell();
            let val = r.read_str()?;
            match field {
                ErrorFieldTag::NullTerminator => unreachable!(),
                ErrorFieldTag::LocalizedSeverity => (),
                ErrorFieldTag::Severity => { err.severity = ErrorSeverity::from_str(val)?; }
                ErrorFieldTag::Code => { err.code = ErrorCode::from_str(val)?; }
                ErrorFieldTag::Message => { err._message = pos; }
                ErrorFieldTag::MessageDetail => { err._detail = pos; }
                ErrorFieldTag::MessageHint => { err._hint = pos; }
                ErrorFieldTag::Position => { err._position = pos; }
                ErrorFieldTag::InternalPosition => { err._internal_position = pos; }
                ErrorFieldTag::InternalQuery => { err._internal_query = pos; }
                ErrorFieldTag::Where => { err._where = pos; }
                ErrorFieldTag::SchemaName => { err._schema_name = pos; }
                ErrorFieldTag::TableName => { err._table_name = pos; }
                ErrorFieldTag::ColumnName => { err._column_name = pos; }
                ErrorFieldTag::DataTypeName => { err._data_type_name = pos; }
                ErrorFieldTag::ConstraintName => { err._constraint_name = pos; }
                ErrorFieldTag::File => { err._file = pos; }
                ErrorFieldTag::Line => { err._line = pos; }
                ErrorFieldTag::Routine => { err._routine = pos; }
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

    pub fn column_name(&self) -> &str {
        self.read_str_at(self._column_name)
    }

    /*pub fn constraint_name(&self) -> &str {

    }

    pub fn data_type_name(&self) -> &str {

    }

    pub fn detail(&self) -> &str {

    }

    pub fn file(&self) -> &str {

    }

    pub fn hint(&self) -> &str {

    }

    pub fn internal_position(&self) -> &str {

    }

    pub fn internal_query(&self) -> &str {

    }

    pub fn line(&self) -> &str {

    }

    pub fn message(&self) -> &str {

    }

    pub fn position(&self) -> &str {

    }

    pub fn routine(&self) -> &str {

    }

    pub fn schema_name(&self) -> &str {

    }

    pub fn table_name(&self) -> &str {

    }

    pub fn context(&self) -> &str {

    }*/

    pub fn into_message(self) -> Message {
        self.msg
    }
}