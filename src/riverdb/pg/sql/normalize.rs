use tracing::{debug};

use crate::riverdb::{Error, Result};
use crate::riverdb::pg::sql::{Query, QueryType, QueryParam};
use crate::riverdb::pg::protocol::{Message};

// A list of operators which we don't format with a following space
const TOKENS_WITHOUT_FOLLOWING_WHITESPACE: &'static str = ".([:";
// A list of operators which we don't format with a preceding space
const TOKENS_WITHOUT_PRECEDING_WHITESPACE: &'static str = ",.()[]:";

// All characters allowed in operators
const ALL_OPERATORS: &'static str = "+-*/<>~=!@#%^&|`?";
// Characters required in an operator if it ends in + or -
const REQUIRED_IF_OPERATOR_ENDS_IN_PLUS_MINUS: &'static str = "~!@#%^&|`?";

pub(crate) struct QueryNormalizer<'a> {
    src: &'a [u8],
    pos: usize,
    params_buf: String,
    normalized_query: String,
    query_type: QueryType,
    params: Vec<QueryParam<'static>>, // 'static is a lie here, it's 'self
    tags: Vec<(&'static str, &'static str)>, // 'static is a lie here, it's 'self
}

impl<'a> QueryNormalizer<'a> {
    pub fn normalize(msg: &Message<'a>) -> Result<Query> {
        let src = msg.reader().read_to_end()?;
        let mut this = Self {
            src,
            pos: 0,
            params_buf: "".to_string(),
            normalized_query: "".to_string(),
            query_type: QueryType::Other,
            params: Vec::new(),
            tags: Vec::new(),
        };

        loop {
            let c = this.next()?.get();
            debug!("main loop c='{}'", c); // TODO char format code

            let mut res = Ok(());
            if is_whitespace(c) {
                this.whitespace(c);
            } else if c == '\'' {
                res = this.single_quoted_string(c);
            } else if c == '"' {
                res = this.quoted_identifier(c);
            } else if c == '$' {
                res = this.maybe_dollar_string(c);
            } else if c == '.' || is_digit(c) {
                res = this.numeric(c);
            } else if (c == 'N' || c == 'n') && this.match_fold("ull") {
                res = this.null();
            } else if (c == 'B' || c == 'b') && this.peek() == '\'' {
                res = this.bit_string(c);
            } else if (c == 'E' || c == 'e') && this.peek() == '\'' {
                res = this.escape_string(c);
            } else if (c == 'U' || c == 'u') && this.peek() == '&' {
                res = this.unicode_string(c);
            } else if (c == 'T' || c == 't') && this.match_fold("rue") {
                res = this.bool(true);
            } else if (c == 'F' || c == 'f') && this.match_fold("alse") {
                res = this.bool(false);
            } else if c == '/' && this.peek() == '*' {
                res = this.c_style_comment(c);
            } else if c == '-' && this.peek() == '-' {
                res = this.sql_comment(c);
            } else if is_letter(c) || c == '_' {
                res = this.keyword_or_identifier(c);
            } else if c == '(' || c == ')' || c == '[' || c == ']' || c == ',' {
                this.append_byte(c as u8);
            } else if c == ';' {
                // Ignore ; if it occurs at the end of the query
                this.whitespace(this.next()?);
                if this.peek() == 0 {
                    break
                } else {
                    // TODO verify this is correct and add test case for this too
                    res = this.operator(c);
                }
            } else if c < 128 {
                res = this.operator(c)
            } else {
                res = Err(Error::new(format!("unexpected char {} in query", c))); // TODO char format code
            }

            res?;
        }

        let query_type = QueryType::from(&this.normalized_query);

        return Ok(Query{
            normalized_query: this.normalized_query,
            query_type,
            params_buf: this.params_buf,
            params: this.params,
            tags: this.tags,
        })
    }

    fn peek(&mut self) -> Result<char> {
        let (c, _) = util::decode_utf8_char(self.tail())?;
        Ok(c)
    }

    fn next(&mut self) -> Result<char> {
        let (c, size) = util::decode_utf8_char(self.tail())?;
        Ok(c)
    }

    /// backup one character and return it. Panics if at start.
    fn backup(&mut self) -> char {
        assert!(self.pos != 0, "can't backup before start");
        todo!()
    }

    /// last returns the previously read character, without changing the position.
    fn last(&mut self) -> char {
        todo!()
    }

    /// tail returns the remaining part of the string as a slice
    fn tail(&mut self) -> &'a str {
        unsafe {
            // Safety: we already checked this is valid utf8 in the constructor
            std::str::from_utf8_unchecked(&self.src[self.pos..])
        }
    }

    /// matches str s at tail() case-insensitively
    fn match_fold(&mut self, s: &'static str) -> bool {
        let len = s.len();
        let tail = self.tail();
        if tail.len() < len {
            false
        } else {
            &tail[..len].eq_ignore_ascii_case(s)
        }
    }

    // appends a space to the normalized query, if it doesn't end in TOKENS_WITHOUT_FOLLOWING_WHITESPACE
    fn write_space(&mut self) {
        if !self.normalized_query.is_empty() {
            let last_byte = self.normalized_query.as_bytes()[self.normalized_query.len()-1];
            if TOKENS_WITHOUT_FOLLOWING_WHITESPACE.as_bytes().contains(&last_byte) {
                self.normalized_query.push(' ');
            }
        }
    }


}