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

        let query_type = this.do_normalize()?;

        return Ok(Query{
            normalized_query: this.normalized_query,
            query_type,
            params_buf: this.params_buf,
            params: this.params,
            tags: this.tags,
        })
    }

    fn do_normalize(&mut self) -> Result<QueryType> {
        loop {
            let c = self.next()?.get();
            debug!("main loop c='{}'", c); // TODO char format code

            let mut res = Ok(());
            if c.is_ascii_whitespace() {
                self.consume_whitespace(c);
            } else if c == '\'' {
                res = self.single_quoted_string(c);
            } else if c == '"' {
                res = self.quoted_identifier(c);
            } else if c == '$' {
                res = self.maybe_dollar_string(c);
            } else if c == '.' || c.is_ascii_digit() {
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
            } else if c.is_alphabetic() || c == '_' {
                res = self.keyword_or_identifier(c);
            } else if c == '(' || c == ')' || c == '[' || c == ']' || c == ',' {
                self.append_byte(c as u8);
            } else if c == ';' {
                // Ignore ; if it occurs at the end of the query
                this.consume_whitespace(this.next()?);
                if self.peek() == 0 {
                    break
                } else {
                    // TODO verify this is correct and add test case for this too
                    res = self.operator(c);
                }
            } else if c < 128 {
                res = self.operator(c);
            } else {
                res = Err(Error::new(format!("unexpected char {} in query", c))); // TODO char format code
            }

            res?;
        }

        Ok(QueryType::from(&this.normalized_query))
    }

    fn peek(&mut self) -> Result<char> {
        let (c, _) = util::decode_utf8_char(self.tail())?;
        Ok(c)
    }

    fn next(&mut self) -> Result<char> {
        let (c, size) = util::decode_utf8_char(self.tail())?;
        // TODO maybe we don't need this condition
        if size != 0 {
            self.last_char = c;
            self.last_char_size = size;
        }
        Ok(c)
    }

    /// backup one character. Panics if at start.
    /// Can only be called exactly once after a call to next().
    fn backup(&mut self) {
        assert!(self.pos != 0, "can't backup before start");
        assert!(self.last_char_size != 0, "must call next() before backup()");
        debug_assert!(self.pos >= self.last_char_size);
        self.pos -= self.last_char_size;
        self.last_char_size = 0;
    }

    /// last returns the previously read character, without changing the position.
    fn last(&self) -> char {
        self.last_char
    }

    /// tail returns the remaining part of the source bytes from the current position.
    fn tail(&mut self) -> &'a [u8] {
        &self.src[self.pos..]
    }

    /// matches str s at tail() case-insensitively
    fn match_fold(&mut self, s: &'static str) -> bool {
        let len = s.len();
        let tail = self.tail();
        if tail.len() < len {
            false
        } else {
            let mut i = 0;
            for c in s {
                if !tail.get(i).unwrap().eq_ignore_ascii_case(c) {
                    return false;
                }
                i += 1;
            }
            true
        }
    }

    /// appends a space to the normalized query, if it doesn't end in TOKENS_WITHOUT_FOLLOWING_WHITESPACE
    fn write_space(&mut self) {
        if !self.normalized_query.is_empty() {
            let last_byte = self.normalized_query.as_bytes()[self.normalized_query.len()-1];
            if TOKENS_WITHOUT_FOLLOWING_WHITESPACE.as_bytes().contains(&last_byte) {
                self.normalized_query.push(' ');
            }
        }
    }

    /// consumes all whitespace characters, starting with c
    fn consume_whitespace(&mut self, c: char) {
        while c.is_ascii_whitespace() {
            c = self.next();
        }
        self.backup();
    }

    /// returns true if a string continuation is found immediately prior to pos
    /// A string continuation is a ' followed by a newline, optionally followed by whitespace
    fn look_behind_for_string_continuation(&self, pos: usize) -> bool {
        let mut found_newline = false;
        let mut i = pos - 1;
        while i >= 0 {
            let c = self.src.get(i).unwrap();
            match c {
                ' ' | '\t' | '\x0c' => (),
                '\n' | '\r' => { found_newline = true; },
                '\'' => return found_newline,
                _ => return false,
            }
            i -= 1;
        }
        // unreachable because there *has* to be a preceding ' because we only call this if the last literal was a string literal
        panic!("look_behind_for_string_continuation can only be called after finding a string literal");
    }

    fn replace_literal(&mut self, start: usize, ty: LiteralType, uppercase: bool) {
        let tok = &self.src[start..self.pos];

        // Any string except a dollar string may be combined with a plain string literal
        // if separated with only whitespace including at least one newline.
        if ty == LiteralType::String && !n.params.is_empty() {
            let prev_param = self.params.last_mut();
            match prev_param.ty {
                LiteralType::String | LiteralType::EscapeString | LiteralType::UnicodeString | LiteralType::BitString => {
                    // Check that there is only whitespace separating them, and it includes a newline
                    if self.look_behind_for_string_continuation(start) {
                        // Cut off the terminating single quote
                        self.param_buf.pop().unwrap();
                        // Append the string token, minus the starting single quote
                        // Safety: We already decoded this as utf8.
                        unsafe {
                            self.param_buf.push_str(std::str::from_utf8_unchecked(&tok[1..]))
                        }
                        return
                    }
                },
                _ = (),
            }
        }

        let mut negated = false;
        if ty == LiteralType::Integer || ty == LiteralType::Numeric {
            negated = self.is_negative_number(start, self.pos);
            // Remove the - from the end of the normalized string
            if negated {
                let trim = self.no
            }
        }
    }

    /// appends a NULL literal to params
    fn null(&mut self) {
        self.replace_literal(self.pos - 4, LiteralType::Null, true);
    }

    /// appends a TRUE or FALSE literal to params depending on the value of b.
    fn bool(&mut self, b: bool) {
        let mut start = self.pos - 5;
        if b {
            start += 1;
        }
        n.replace_literal(start, LiteralType::Boolean, true);
    }
}