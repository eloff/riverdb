use tracing::{debug};

use crate::riverdb::{Error, Result};
use crate::riverdb::pg::sql::{Query, QueryType, QueryParam, LiteralType, QueryTag, QueryInfo};
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
    last_char_size: usize,
    last_char: char,
    start_offset_in_msg: u32,
    query_type: QueryType,
    params_buf: String,
    normalized_query: String,
    params: Vec<QueryParam>,
    tags: Vec<QueryTag>,
}

impl<'a> QueryNormalizer<'a> {
    pub fn new(msg: &Message<'a>) -> Self {
        let reader = msg.reader();
        let start_offset_in_msg = reader.tell();
        let src = reader.read_to_end()?;

        Self {
            src,
            pos: 0,
            last_char_size: 0,
            last_char: 0,
            start_offset_in_msg,
            query_type: QueryType::Other,
            params_buf: String::new(),
            normalized_query: String::new(),
            params: Vec::new(),
            tags: Vec::new(),
        }
    }

    pub fn normalize(mut self) -> Result<(QueryInfo, Vec<QueryTag>)> {
        loop {
            let c = self.next()?.get();
            debug!("main loop c='{}'", c); // TODO char format code

            let mut res = Ok(());
            if c.is_ascii_whitespace() {
                res = self.consume_whitespace(c);
            } else if c == '\'' {
                res = self.single_quoted_string(c);
            } else if c == '"' {
                res = self.quoted_identifier(c);
            } else if c == '$' {
                res = self.maybe_dollar_string(c);
            } else if c == '.' || c.is_ascii_digit() {
                res = self.numeric(c);
            } else if (c == 'N' || c == 'n') && self.match_fold("ull") {
                self.null();
            } else if (c == 'B' || c == 'b') && self.peek() == '\'' {
                res = self.bit_string(c);
            } else if (c == 'E' || c == 'e') && self.peek() == '\'' {
                res = self.escape_string(c);
            } else if (c == 'U' || c == 'u') && self.peek() == '&' {
                res = self.unicode_string(c);
            } else if (c == 'T' || c == 't') && self.match_fold("rue") {
                self.bool(true);
            } else if (c == 'F' || c == 'f') && self.match_fold("alse") {
                self.bool(false);
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
                res = Err(Error::new(format!("unexpected char '{}' in query", c)));
            }

            res?;
        }

        let ty = QueryType::from(&this.normalized_query);
        Ok((QueryInfo{
            params_buf: self.params_buf,
            normalized: self.normalized_query,
            ty,
            params: self.params
        }, self.tags))
    }

    fn peek(&mut self) -> char {
        let (c, _) = util::decode_utf8_char(self.tail()).unwrap_or((0, 0));
        c
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
        assert_ne!(self.pos, 0, "can't backup before start");
        assert_ne!(self.last_char_size, 0, "must call next() before backup()");
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
    fn consume_whitespace(&mut self, mut c: char) -> Result<()> {
        while c.is_ascii_whitespace() {
            c = self.next()?;
        }
        self.backup();
    }

    /// returns true if a string continuation is found immediately prior to pos
    /// A string continuation is a ' followed by a newline, optionally followed by whitespace
    fn look_behind_for_string_continuation(&self, pos: usize) -> bool {
        let mut found_newline = false;
        let mut i = pos - 1;
        while i >= 0 {
            let c = self.src.get(i).unwrap() as char;
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

    /// Write a $N placeholder to the normalized query and push the literal value onto the params vec.
    /// Combines string continuations into a single literal. It may include a leading - as part of a numeric literal.
    /// Converts NULL and BOOLEAN literals to uppercase.
    fn replace_literal(&mut self, start: usize, ty: Litee) {
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
                _ => (),
            }
        }

        let mut negated = false;
        if ty == LiteralType::Integer || ty == LiteralType::Numeric {
            negated = self.is_negative_number(start, self.pos);
            // Remove the - from the end of the normalized string.
            // We believe it to be part of the numeric literal.
            if negated {
                // Remove the - from the end of the normalized string
                assert_eq!(self.normalized_query.last(), Ok('-'));
                self.normalized_query.pop();
                // Remove the space we added too
                if self.normalized_query.last() == Ok(' ') {
                    self.normalized_query.pop();
                }
            }
        }

        let ascii_uppercase = ty == LiteralType::Null || ty == LiteralType::Boolean;
        for b in tok {
            let mut c = b as char;
            if ascii_uppercase {
                c = c.to_ascii_uppercase();
            }
            self.params_buf.push(c);
        }

        self.params.push(QueryParam{
            pos: start as u32,
            len: tok.len() as u32,
            ty,
            negated,
        });

        self.normalized_query.push('$');
        write!(&mut self.normalized_query, "{}", self.params.len());
    }

    /// appends a NULL literal to params
    fn null(&mut self) {
        self.replace_literal(self.pos - 4, LiteralType::Null);
    }

    /// appends a TRUE or FALSE literal to params depending on the value of b.
    fn bool(&mut self, b: bool) {
        let mut start = self.pos - 5;
        if b {
            start += 1;
        }
        n.replace_literal(start, LiteralType::Boolean);
    }

    /// parses the numeric literal and adds it to params
    fn numeric(&mut self, mut c: char) -> Result<()> {
        debug_assert!(c == '.' || c.is_ascii_digit(), "c must start a number");
        debug_assert_ne!(self.pos, 0);

        let start = self.pos - 1;
        let mut decimal = false;
        loop {
            match c {
                '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' | 'e' | 'E' => (),
                '+' | '-' => {
                    let prev = self.last();
                    if prev.to_ascii_lowercase() != 'e' {
                        return Err(Error::new(format!("unexpected '{}' in numeric value following '{}'", c, prev)));
                    }
                },
                '.' => {
                    if decimal {
                        return Err(Error::new("cannot have two decimals in numeric value"));
                    }
                    // Only valid if there are digits on at least one side
                    if !self.peek().is_ascii_digit() && !self.last().is_ascii_digit() {
                        // Not actually a number, must be part of a dotted identifier (with a preceding space - why?!?)
                        assert_eq!(self.pos, start + 1, ". without digits not at the start of the literal");
                        return self.operator(c);
                    }
                    decimal = true
                },
                '\0' => {
                    break; // EOF
                },
                _ => {
                    if c.is_alphabetic() {
                        return Err(Error::new(format!("unexpected '{}' in numeric value", c)));
                    }
                    break;
                },
            }
            c = self.next()?;
        }

        // backup to position of last char so we don't include the terminating char
        let prev = n.backup();
        if prev == 'e' || prev == 'E' || prev == '+' || prev == '-' {
            // Can't end in an exponent symbol
            return Err(Error::new(format!("numeric constant cannot end in exponent '{}'", prev)));
        }

        let mut ty = LiteralType::Integer;
        if decimal && prev != '.' {
            // If the number included, but did not end in a decimal,
            // then it's a numeric type instead of an integer.
            // => SELECT .1, 2., 3.0;
            // 0.1  |    2 |  3.0
            ty = LiteralType::Numeric;
        }

        // We have a number. It may be prefixed with a - or + unary operator.
        // Unlike Postgres where that's treated as an operator, we need to treat it as part of
        // the constant, otherwise queries with negative numbers won't have the same form as
        // queries with positive numbers and this can cause an exponential explosion in the
        // number of query forms which we use for cache keys in various caches.
        self.replace_literal(start, ty);

        // If it was an integer but ended in ., then strip the ending . from the literal value
        if decimal && prev == '.' {
            assert_eq!(self.params_buf.pop().unwrap(), '.');
            self.params.last_mut().len -= 1;
        }

        Ok(())
    }

    /// parses the c-style /* comment */ including possible tags
    fn c_style_comment(&mut self, mut c: char) -> Result<()> {
        debug_assert_eq!(c, '/', "c must start a c-style comment");

        let start = self.pos;
        let mut tag = QueryTag::new();

        loop {
            if c == '/' && self.peek() == '*' {
                self.next().unwrap();
            } else if c == '*' && self.peek() == '/' {
                if tag.val_pos != 0 {
                    tag.val_len = self.pos as u32 - tag.val_pos;
                    self.append_tag(tag);
                }
                self.next().unwrap();

            } else if c == '=' {
                // This might be part of a tag.
                // Scan backward for a dotted identifier A-Za-z0-9-_.
            } else if c.is_ascii_whitespace() || c == '"' {
                // Don't permit double-quotes in a tag, we may want to allow quoted values later
                if tag.val_pos != 0 {
                    tag.val_len = self.pos as u32 - tag.val_pos;
                    self.append_tag(tag);
                }
            }

        }
    }
}