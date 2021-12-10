use crate::riverdb::pg::protocol::{Tag, MessageBuilder, ErrorSeverity, ErrorFieldTag, Messages};

/// A builder for constructing Postgres wire protocol error messages.
pub struct MessageErrorBuilder(MessageBuilder);

impl MessageErrorBuilder {
    /// Construct a new message builder for Postgres errors
    pub fn new(severity: ErrorSeverity, code: &str, msg: &str) -> Self {
        let tag = if severity <= ErrorSeverity::Warning { Tag::NOTICE_RESPONSE } else { Tag::ERROR_RESPONSE };
        let mut builder = MessageErrorBuilder(MessageBuilder::new(tag));
        builder
            .write_field(ErrorFieldTag::SEVERITY, severity.as_str())
            .write_field(ErrorFieldTag::CODE, code)
            .write_field(ErrorFieldTag::MESSAGE, msg);
        builder
    }

    /// Write an error field with the given tag and value
    pub fn write_field(&mut self, field: ErrorFieldTag, s: &str) -> &mut Self {
        self.0.write_byte(field.as_u8());
        self.0.write_str(s);
        self
    }

    /// Complete the message and return it
    pub fn finish(mut self) -> Messages {
        self.0.write_byte(ErrorFieldTag::NULL_TERMINATOR.as_u8());
        self.0.finish()
    }
}