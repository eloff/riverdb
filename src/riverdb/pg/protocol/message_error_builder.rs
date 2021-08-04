use crate::riverdb::pg::protocol::{Tag, MessageBuilder, ErrorSeverity, ErrorFieldTag, Message};

pub struct MessageErrorBuilder(MessageBuilder);

impl MessageErrorBuilder {
    pub fn new(severity: ErrorSeverity, code: &str, msg: &str) -> Self {
        let tag = if severity <= ErrorSeverity::Warning { Tag::NOTICE_RESPONSE } else { Tag::ERROR_RESPONSE };
        let mut builder = MessageErrorBuilder(MessageBuilder::new(tag));
        builder
            .write_field(ErrorFieldTag::SEVERITY, severity.as_str())
            .write_field(ErrorFieldTag::CODE, code)
            .write_field(ErrorFieldTag::MESSAGE, msg);
        builder
    }

    pub fn write_field(&mut self, field: ErrorFieldTag, s: &str) -> &mut Self {
        self.0.write_byte(field.as_u8());
        self.0.write_str(s);
        self
    }

    pub fn finish(mut self) -> Message {
        self.0.write_byte(ErrorFieldTag::NULL_TERMINATOR.as_u8());
        self.0.finish()
    }
}