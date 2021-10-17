use tracing::{debug};

use crate::riverdb::pg::sql::{Query, QueryType, QueryParam}
use crate::riverdb::pg::protocol::{Message};

pub(crate) struct QueryNormalizer<'a> {
    src: &'a [u8],
    pos: usize,
    chars: Iter<'a>,
    params_buf: String,
    normalized_query: String,
    query_type: QueryType,
    params: Vec<QueryParam<'static>>, // 'static is a lie here, it's 'self
    tags: Vec<(&'static str, &'static str)>, // 'static is a lie here, it's 'self
}

impl<'a> QueryNormalizer<'a> {
    pub fn new(msg: &Message<'a>) -> Self {
        QueryNormalizer {
            src: msg.reader().read_body(),
            pos: 0,
            params_buf: "".to_string(),
            normalized_query: "".to_string(),
            query_type: QueryType::Other,
            params: Vec::new(),
            tags: Vec::new(),
        }
    }

    pub fn normalize(self) -> Result<Query> {
        loop {
            let c = self.next()?.get();
            debug!("main loop c='{}'", c); // TODO char format code

            let mut res = Ok(());
            if is_whitespace(c) {
                self.whitespace(c);
            } else if c == '\'' {
                res = self.single_quoted_string(c);
            } else if c == '"' {
                res = self.quoted_identifier(c);
            } else if c == '$' {
                res = self.maybe_dollar_string(c);
            } else if c == '.' || is_digit(c) {
                res = self.numeric(c);
            } else if (c == 'N' || c == 'n') && self.match_fold("ull") {
                res = self.null();
            } else if (c == 'B' || c == 'b') && self.peek() == '\'' {
                res = self.bit_string(c);
            } else if (c == 'E' || c == 'e') && self.peek() == '\'' {
                res = self.escape_string(c);
            } else if (c == 'U' || c == 'u') && self.peek() == '&' {
                res = self.unicode_string(c);
            } else if (c == 'T' || c == 't') && self.match_fold("rue") {
                res = self.bool(true);
            } else if (c == 'F' || c == 'f') && self.match_fold("alse") {
                res = self.bool(false);
            } else if c == '/' && self.peek() == '*' {
                res = self.c_style_comment(c);
            } else if c == '-' && self.peek() == '-' {
                res = self.sql_comment(c);
            } else if is_letter(c) || c == '_' {
                res = self.keyword_or_identifier(c);
            } else if c == '(' || c == ')' || c == '[' || c == ']' || c == ',' {
                self.append_byte(c as u8);
            } else if c == ';' {
                // Ignore ; if it occurs at the end of the query
                self.whitespace(self.next()?);
                if self.peek() == 0 {
                    break
                } else {
                    // TODO verify this is correct and add test case for this too
                    res = self.operator(c);
                }
            } else if c < 128 {
                res = self.operator(c)
            } else {
                res = Err(Error::new(format!("unexpected char {} in query", c))); // TODO char format code
            }

            res?;
        }

        let query_type = QueryType::from(&self.normalized_query);

        return Ok(Query{
            normalized_query: self.normalized_query,
            query_type,
            params_buf: self.params_buf,
            params: self.params,
            tags: self.tags,
        })
    }

    fn peek(&mut self) -> i32 {
        todo!()
    }

    fn next(&mut self) -> Result<NonZeroI32> {
        todo!()
    }

    /// backup one character and return it. Panics if at start.
    fn backup(&mut self) -> i32 {
        assert!(self.pos != 0, "can't backup before start");
        todo!()
    }

    /// last returns the previously read character, without changing the position.
    fn last(&mut self) -> i32 {
        todo!()
    }

    /// tail returns the remaining bytes as a slice
    fn tail(&mut self) -> &'a [u8] {
        &self.src[self.pos..]
    }

    fn match_fold(&mut self) -> bool {
        todo!()
    }

    fn write_space(&mut self) {
        if !self.normalized_query.is_empty() {

        }
    }
}