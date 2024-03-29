use tracing::{debug};
use std::fmt::Write; // this is used don't remove it

use memmem::{TwoWaySearcher, Searcher};

use crate::riverdb::{Error, Result};
use crate::riverdb::pg::sql::{QueryType, QueryParam, LiteralType, QueryTag, Query, ObjectType};
use crate::riverdb::pg::protocol::{Message};
use crate::riverdb::common::{decode_utf8_char, Range32};


// A list of operators which we don't format with a following space
const TOKENS_WITHOUT_FOLLOWING_WHITESPACE: &'static str = ".([:";
// A list of operators which we don't format with a preceding space
const TOKENS_WITHOUT_PRECEDING_WHITESPACE: &'static str = ",.()[]:";

// All characters allowed in operators
const ALL_OPERATORS: &'static str = "+-*<>/=~!@#%^&|`?";
// Characters that must be present in an operator if it ends in + or -
const REQUIRED_IF_OPERATOR_ENDS_IN_PLUS_OR_MINUS: &'static str = "~!@#%^&|`?";
// OTHER_OPERATOR_CHARS = ALL_OPERATORS - REQUIRED_IF_OPERATOR_ENDS_IN_PLUS_OR_MINUS
const OTHER_OPERATOR_CHARS: &'static str = "+-*<>/=";

pub(crate) struct QueryNormalizer<'a> {
    src: &'a [u8],
    pos: usize,
    current_char: char,
    last_char: char,
    current_char_size: u8,
    last_char_size: u8,
    comment_level: u8,
    query: Query,
}

impl<'a> QueryNormalizer<'a> {
    pub fn new(msg: &'a Message<'a>) -> Self {
        let mut reader = msg.reader();
        let start_offset_in_msg = reader.tell();
        let src = msg.as_slice();

        Self::new_at(src, start_offset_in_msg as usize)
    }

    pub const fn new_at(src: &'a [u8], offset: usize) -> Self {
        Self {
            src,
            pos: offset,
            last_char: '\0',
            last_char_size: 0,
            current_char: '\0',
            current_char_size: 0,
            comment_level: 0,
            query: Query::new(),
        }
    }

    pub fn normalize(mut self, tags: &mut Vec<QueryTag>) -> Result<Query> {
        loop {
            let mut c = self.next()?;
            //println!("c {}", c);

            let mut res = Ok(());
            if c == '\0' {
                break;
            } else if c.is_ascii_whitespace() {
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
                res = self.c_style_comment(c, tags);
            } else if c == '-' && self.peek() == '-' {
                res = self.sql_comment(c);
            } else if c.is_alphabetic() || c == '_' {
                res = self.keyword_or_identifier(c);
            } else if c == '(' || c == ')' || c == '[' || c == ']' || c == ',' {
                self.append_char(c);
            } else if c == ';' {
                self.end_of_query(c, tags)?;
                break;
            } else if c < (128 as char) {
                res = self.operator(c);
            } else {
                res = Err(Error::new(format!("unexpected char '{}' in query", c)));
            }

            res?;
        }

        self.query.ty = QueryType::from(self.query.normalized.as_str());
        self.query.object_ty = ObjectType::parse(self.query.normalized.as_str(), self.query.ty);
        Ok(self.query)
    }

    fn peek(&mut self) -> char {
        let (c, _) = decode_utf8_char(self.tail()).unwrap_or(('\0', 0));
        c
    }

    fn next(&mut self) -> Result<char> {
        let (c, size) = decode_utf8_char(self.tail())?;
        self.last_char = self.current_char;
        self.last_char_size = self.current_char_size;
        self.current_char = c;
        self.current_char_size = size as u8;
        self.pos += size;
        Ok(c)
    }

    /// backup one character and return the character before the new position.
    /// If at EOF, this is the last character in the input. Otherwise it's the
    /// same as would be returned by calling self.second_last() prior to this.
    /// Panics if at start. Can only be called exactly once after a call to next().
    fn backup(&mut self) -> char {
        assert_ne!(self.pos, 0, "can't backup before start");
        // If we're at EOF, we don't backup, we just return the last char read (self.last_char)
        if self.current_char_size != 0 {
            let backup_by = self.current_char_size as usize;
            assert_ne!(backup_by, 0, "must call next() before backup()");
            debug_assert!(self.pos >= backup_by);
            self.pos -= backup_by;
            self.current_char_size = self.last_char_size;
            self.current_char = self.last_char;
        };
        let result = self.last_char;
        self.last_char = '\0';
        self.last_char_size = 0;
        result
    }

    /// second_last returns the char returned in the previous (second most recent) call to next()
    /// without changing the position.
    fn second_last(&self) -> char {
        self.last_char
    }

    /// current returns the char returned in the most recent call to next()
    /// without changing the position.
    #[allow(unused)]
    fn current(&self) -> char {
        self.current_char
    }

    /// tail returns the remaining part of the source bytes from the current position.
    fn tail(&mut self) -> &'a [u8] {
        &self.src[self.pos..]
    }

    /// append a char to the normalized query, inserting a space first, if required
    fn append_char(&mut self, c: char) {
        if !TOKENS_WITHOUT_PRECEDING_WHITESPACE.contains(c) {
            self.write_space();
        }
        self.query.normalized.push(c);
    }

    /// append a token to the normalized query, inserting a space first, if required
    fn append_token(&mut self, tok: &[u8]) {
        if tok.len() == 1 {
            self.append_char(*tok.get(0).unwrap() as char);
        } else {
            self.write_space();
            self.query.normalized.push_str(
                // Safety: we already parsed this as valid utf8
                unsafe { std::str::from_utf8_unchecked(tok) }
            );
        }
    }

    /// append a token to the normalized query, converting to uppercase,
    /// and inserting a space first, if required.
    fn append_token_uppercase(&mut self, tok: &[u8]) {
        self.write_space();
        // Safety: we already parsed this as valid utf8
        for c in unsafe { std::str::from_utf8_unchecked(tok) }.chars() {
            self.query.normalized.push(c.to_ascii_uppercase());
        }
    }

    /// matches str s at tail() case-insensitively
    fn match_fold(&mut self, s: &'static str) -> bool {
        let len = s.len();
        let tail = self.tail();
        if tail.len() < len {
            false
        } else {
            for (sc, tc) in s.chars().zip(tail) {
                if !(*tc as char).eq_ignore_ascii_case(&sc) {
                    return false;
                }
            }
            self.pos += s.len();
            true
        }
    }

    /// appends a space to the normalized query, if it doesn't end in TOKENS_WITHOUT_FOLLOWING_WHITESPACE
    fn write_space(&mut self) {
        if !self.query.normalized.is_empty() {
            let last_byte = self.query.normalized.as_bytes()[self.query.normalized.len()-1];
            if !TOKENS_WITHOUT_FOLLOWING_WHITESPACE.as_bytes().contains(&last_byte) {
                self.query.normalized.push(' ');
            }
        }
    }

    /// consumes all whitespace characters, starting with c
    fn consume_whitespace(&mut self, mut c: char) -> Result<()> {
        while c.is_ascii_whitespace() {
            c = self.next()?;
        }
        self.backup();
        Ok(())
    }

    /// returns true if a string continuation is found immediately prior to pos
    /// A string continuation is a ' followed by a newline, optionally followed by whitespace
    fn look_behind_for_string_continuation(&self, pos: usize) -> bool {
        let mut found_newline = false;
        let mut i = pos - 1;
        loop {
            // Safety: checked bounds above
            let c = unsafe { *self.src.get_unchecked(i) } as char;
            match c {
                ' ' | '\t' | '\x0c' => (),
                '\n' | '\r' => { found_newline = true; },
                '\'' => return found_newline,
                _ => return false,
            }
            if i == 0 {
                return false;
            }
            i -= 1;
        }
    }

    /// Write a $N placeholder to the normalized query and push the literal value onto the params vec.
    /// Combines string continuations into a single literal. It may include a leading - as part of a numeric literal.
    /// Converts NULL and BOOLEAN literals to uppercase.
    fn replace_literal(&mut self, start: usize, ty: LiteralType) {
        let tok = &self.src[start..self.pos];
        let param_start = self.query.params_buf.len();

        // Any string except a dollar string may be combined with a plain string literal
        // if separated with only whitespace including at least one newline.
        if ty == LiteralType::String && !self.query.params.is_empty() {
            match self.query.params.last().unwrap().ty {
                LiteralType::String | LiteralType::EscapeString | LiteralType::UnicodeString | LiteralType::BitString => {
                    // Check that there is only whitespace separating them, and it includes a newline
                    if self.look_behind_for_string_continuation(start) {
                        // Cut off the terminating single quote
                        assert_eq!(self.query.params_buf.pop(), Some('\''));
                        // Append the string token, minus the starting single quote
                        // Safety: We already decoded this as utf8.
                        let continued_s = unsafe { std::str::from_utf8_unchecked(&tok[1..]) };
                        self.query.params_buf.push_str(continued_s);
                        self.query.params.last_mut().unwrap().value.end += continued_s.len() as u32 - 1;
                        return
                    }
                },
                _ => (),
            }
        }

        let mut negated = false;
        if ty == LiteralType::Integer || ty == LiteralType::Numeric {
            negated = self.is_negative_number(start);
            // Remove the - from the end of the normalized string.
            // We believe it to be part of the numeric literal.
            if negated {
                // Remove the - from the end of the normalized string
                assert_eq!(self.query.normalized.pop(), Some('-'));
                // Remove the space we added too
                match self.query.normalized.pop() {
                    Some(' ') => (),
                    None => (),
                    Some(c) => {
                        // Whoops, it wasn't a space, put it back
                        self.query.normalized.push(c);
                    },
                }
            }
        }

        let ascii_uppercase = ty == LiteralType::Null || ty == LiteralType::Boolean;
        for b in tok {
            let mut c = *b as char;
            if ascii_uppercase {
                c = c.to_ascii_uppercase();
            }
            self.query.params_buf.push(c);
        }

        self.query.params.push(QueryParam{
            value: Range32::new(param_start, self.query.params_buf.len()),
            ty,
            negated,
            target_type: Range32::default()
        });

        self.append_char('$');
        write!(&mut self.query.normalized, "{}", self.query.params.len()).unwrap();
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
        self.replace_literal(start, LiteralType::Boolean);
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
                    let prev = self.second_last();
                    if prev.to_ascii_lowercase() != 'e' {
                        // This must be an binary + operator
                        break;
                    }
                },
                '.' => {
                    if decimal {
                        return Err(Error::new("cannot have two decimals in numeric value"));
                    }
                    // Only valid if there are digits on at least one side
                    if !self.peek().is_ascii_digit() && !self.second_last().is_ascii_digit() {
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
        let prev = self.backup();
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
            // We just pushed to params in replace_literal so it's not empty
            assert_eq!(self.query.params_buf.pop().unwrap(), '.');
            self.query.params.last_mut().unwrap().value.end -= 1;
        }

        Ok(())
    }

    /// parses ; followed optionally by whitespace, /* c-style */ or sql -- comment(s), or additional queries.
    fn end_of_query(&mut self, mut c: char, tags: &mut Vec<QueryTag>) -> Result<()> {
        debug_assert_eq!(c, ';');
        loop {
            c = self.next()?;
            if c == '\0' {
                break;
            } else if c.is_ascii_whitespace() {
                self.consume_whitespace(c)
            } else if c == '/' && self.peek() == '*' {
                self.c_style_comment(c, tags)
            } else if c == '-' && self.peek() == '-' {
                self.sql_comment(c)
            } else {
                // Normalize the next query, and link to it from this one
                // This process continues recursively until the entire query has been parsed.
                self.backup();
                let next = Self::new_at(self.src, self.pos);
                self.query.next = Some(Box::new(next.normalize(tags)?));
                break;
            }?;
        }
        Ok(())
    }

    /// parses the c-style /* comment */ including possible tags
    fn c_style_comment(&mut self, mut c: char, tags: &mut Vec<QueryTag>) -> Result<()> {
        debug_assert_eq!(c, '/', "c must start a c-style comment");

        let start = self.pos;
        let mut tag = QueryTag::new();

        loop {
            if c == '/' && self.peek() == '*' {
                // A tag can never legitimately start at index 0, since it must be inside a comment
                if tag.val.start != 0 {
                    tag.val.end = self.pos as u32 - 1;
                    append_tag(tags, &mut tag);
                }
                self.next().unwrap();
                self.comment_level += 1;
            } else if c == '*' && self.peek() == '/' {
                // A tag can never legitimately start at index 0, since it must be inside a comment
                if tag.val.start != 0 {
                    tag.val.end = self.pos as u32 - 1;
                    append_tag(tags, &mut tag);
                }
                self.next().unwrap();
                self.comment_level -= 1;
                if self.comment_level == 0 {
                    break;
                }
            } else if c == '=' {
                // This might be part of a tag.
                // Scan backward for a dotted identifier A-Za-z0-9-_.
                // pos is currently after =, backup 2 to get index of char before =
                debug_assert!(self.pos > 2);
                let mut i = self.pos - 2;
                while i > start {
                    // Safety: we just checked the bounds above
                    c = unsafe { *self.src.get_unchecked(i) } as char;
                    if c.is_ascii_alphabetic() || c == '.' || c == '-' || c == '_' {
                        i -= 1;
                    } else {
                        tag.key.start = (i + 1) as u32;
                        tag.key.end = self.pos as u32 - 1;
                        tag.val.start = self.pos as u32;
                        break;
                    }
                }
            } else if c.is_ascii_whitespace() || c == '"' {
                // Don't permit double-quotes in a tag, we may want to allow quoted values later
                // A tag can never legitimately start at index 0, since it must be inside a comment
                if tag.val.start != 0 {
                    tag.val.end = self.pos as u32 - 1;
                    append_tag(tags, &mut tag);
                }
            }

            c = self.next()?;
            if c == '\0' {
                return Err(Error::new("unexpected eof while parsing c-style comment"));
            }
        }

        Ok(())
    }

    fn sql_comment(&mut self, mut c: char) -> Result<()> {
        let c2 = self.next()?;
        // Guaranteed by caller
        debug_assert_eq!(c, '-');
        debug_assert_eq!(c2, '-');

        // Look for a newline or EOF indicating the end of the comment
        // (it's always possible to scan to the end, so this always succeeds)
        loop {
            c = self.next()?;
            if c == '\r' || c == '\n' || c == '\0' {
                break;
            }
        }

        Ok(())
    }

    fn string(&mut self, mut c: char, ty: LiteralType) -> Result<()> {
        debug_assert_eq!(c, '\'', "c must start a single quoted string");

        let mut start = self.pos - 1;
        // Adjust start for literal prefix length
        if ty == LiteralType::EscapeString {
            start -= 1;
        } else if ty == LiteralType::UnicodeString {
            start -= 2;
        }

        let mut backslashes = 0;
        loop {
            c = self.next()?;
            match c {
                '\0' => {
                    return Err(Error::new("unexpected eof parsing string"));
                },
                '\'' => {
                    // This is the end of the string, unless it's an escape string
                    // and it was preceded by an odd number of backslashes.
                    if ty == LiteralType::EscapeString && backslashes%2 != 0 {
                        backslashes = 0;
                    } else {
                        break;
                    }
                },
                '\\' => {
                    backslashes += 1;
                },
                _ => {
                    backslashes = 0;
                }
            }
        }

        self.replace_literal(start, ty);
        Ok(())
    }

    fn quoted_identifier(&mut self, mut c: char) -> Result<()> {
        debug_assert_eq!(c, '"', "c must start a double quoted identifier");

        let start = self.pos - 1;
        loop {
            c = self.next()?;
            if c == '"' {
                if self.peek() == '"' {
                    // This is an escaped ", not the end of the identifier
                    self.next()?;
                } else {
                    break; // end of the identifier
                }
            } else if c == '\0' {
                return Err(Error::new("unexpected eof parsing quoted identifier"));
            }
        }

        self.append_token(&self.src[start..self.pos]);
        Ok(())
    }

    fn maybe_dollar_string(&mut self, c: char) -> Result<()> {
        debug_assert_eq!(c, '$', "c must start a single quoted string");

        let start = self.pos - 1;
        return match self.tail().iter().position(|b| *b == '$' as u8) {
            Some(mut i) => {
                i += 1; // include the $
                let tag_end = start + i + 1;
                let tag = &self.src[start..tag_end];
                let search = TwoWaySearcher::new(tag);
                match search.search_in(&self.src[tag_end..]) {
                    Some(j) => {
                        self.pos = tag_end + j + tag.len();
                        // Verify it's valid utf8, we didn't parse it
                        std::str::from_utf8(&self.src[start..self.pos])?;
                        self.replace_literal(start, LiteralType::DollarString);
                        Ok(())
                    },
                    None => {
                        Err(Error::new(format!("missing ending \"{}\" for $ quoted string", std::str::from_utf8(tag)?)))
                    }
                }
            },
            None => {
                // not a $ string, this is an error.
                // If we didn't enter this function, normally this would fall under operator,
                // so call operator to ensure the error path is consistent.
                self.operator(c)
            }
        };
    }

    fn single_quoted_string(&mut self, c: char) -> Result<()> {
        self.string(c, LiteralType::String)
    }

    fn bit_string(&mut self, mut c: char) -> Result<()> {
        let start = self.pos - 1;
        let c2 = self.next()?;
        debug_assert!((c == 'b' || c == 'B') && c2 == '\'', "c must start a bit string");

        loop {
            c = self.next()?;
            match c {
                '0' | '1' => (),
                '\'' => {
                    self.replace_literal(start, LiteralType::BitString);
                    return Ok(());
                },
                '\0' => {
                    return Err(Error::new("unexpected eof while parsing bit string"));
                },
                _ => {
                    return Err(Error::new(format!("unexpected char '{}' in bit string literal", c)));
                }
            }
        }
    }

    fn escape_string(&mut self, c: char) -> Result<()> {
        let c2 = self.next()?;
        debug_assert!((c == 'e' || c == 'E') && c2 == '\'', "c must start an escape string");

        self.string(c2, LiteralType::EscapeString)
    }

    fn unicode_string(&mut self, c: char) -> Result<()> {
        let c2 = self.next()?;
        debug_assert!((c == 'u' || c == 'U') && c2 == '&', "c must start a unicode string");
        let c3 = self.next()?;
        if c3 != '\'' {
            // It wasn't a unicode string
            // That means u was an identifier, and & was an operator
            self.backup();
            self.pos -= 1; // backup one more (we know we had an ascii &, so this is ok.)
            self.keyword_or_identifier(c)
        } else {
            self.string(c3, LiteralType::UnicodeString)
        }
    }

    fn operator(&mut self, mut c: char) -> Result<()> {
        if c == '.' {
            self.append_char(c);
            return Ok(());
        }

        // From https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-PRECEDENCE
        // An operator name is a sequence of up to NAMEDATALEN-1 (63 by default) characters from the following list:
        //
        // + - * / < > = ~ ! @ # % ^ & | ` ?
        //
        // There are a few restrictions on operator names, however:
        //
        //    -- and /* cannot appear anywhere in an operator name, since they will be taken as the start of a comment.
        //
        //    A multiple-character operator name cannot end in + or -, unless the name also contains at least one of these characters:
        //
        //    ~ ! @ # % ^ & | ` ?
        //
        //    For example, @- is an allowed operator name, but *- is not. This restriction allows PostgreSQL to parse SQL-compliant queries without requiring spaces between tokens.

        let start = self.pos - 1;
        while ALL_OPERATORS.as_bytes().contains(&(c as u8)) {
            c = self.next()?;
        }

        // We already checked for comments, so check that second restriction above applies here.
        let prev_c = self.backup();

        // First character was not a valid operator
        if self.pos == start {
            return Err(Error::new(format!("invalid char '{}' for operator", c)));
        }

        // Technically if an operator ends in a + or _ it should contain one of ~!@#%^&|`?
        // However, we can't tell if a + or - is actually a unary + or -, so it's
        // Possible to have a valid query that breaks that rule but which Postgres will accept.
        // It's better that we accept an invalid query than reject a valid one, so we ignore this.
        // As per the notes in is_negative_number, when we lookup the actual AST
        // We'll replace this normalized query with the actual normalized query from the AST.

        self.append_token(&self.src[start..self.pos]);
        Ok(())
    }

    /// parses and appends a keyword or identifier to the normalized query
    fn keyword_or_identifier(&mut self, mut c: char) -> Result<()> {
        debug_assert!(c.is_alphabetic() || c == '_', "a keyword/identifier must start with a letter or underscore");

        // Key words and identifiers have the same lexical structure, meaning that one cannot know whether a token is an identifier or a key word without knowing the language.
        // It's also context dependant, something could be a keyword in some context, but an identifier in another context:
        //
        // e.g. in SELECT 55 AS CHECK; check is an identifier, despite being a reserved keyword.
        //
        // We don't distinguish here, that's something we use the AST for.
        //
        // SQL identifiers and key words must begin with a letter or an underscore (_).
        // Letter here also includes letters with diacritical marks and non-Latin letters, so a letter in the unicode sense.
        // Subsequent characters in an identifier or key word can be letters, underscores, digits (0-9), or dollar signs ($).
        // Note that dollar signs are not allowed in identifiers according to the letter of the SQL standard, so their use might render applications less portable.
        // We also include an internal '.' here as part of the keyword/identifier rather than treat it as an operator.
        // That means we parse `SELECT foo. bar` as two identifiers "foo." and "bar", and keep the weird whitespace.
        // That's odd, but it's not an error to treat that as a separate query form, so that's what we do. Not worth the code to fix it.

        let start = self.pos - 1;
        loop {
            c = self.next()?;
            if c.is_alphabetic() || c.is_ascii_digit() || c == '_' || c == '$' || c == '.' {
                continue;
            }
            break;
        }

        self.backup();
        self.append_token_uppercase(&self.src[start..self.pos]);

        Ok(())
    }

    /// isNegativeNumber checks if a '-' preceded the numeric constant
    /// and returns true if it is believed to be a unary -, the start of a negative number.
    /// This is not 100% accurate, so we have to verify it after using the normalized query to load the AST.
    fn is_negative_number(&self, start: usize) -> bool {
        debug_assert!(start <= self.src.len());

        // Is this an infix + or - operator? Or is it a unary operator.
        // See: https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-OPERATORS
        //
        // It's not possible to solve without a full contextual aware parse tree - the AST.
        // Since we're using the normalized query as a cache key to speed up parsing to an AST,
        // this puts us in a catch-22. But we're spared because we don't have to answer
        // this question correctly - we merely have to guess, then lookup the actual AST
        // and normalized query - and negate our numeric parameters that we extracted if we got it wrong.
        //
        // It's still mildy important to guess correctly, because multiple wrong guesses per
        // query lead to a exponential explosion in "bad guesses" which alias the same
        // AST. Because for each numeric constant that can be positive or negative, we end up
        // with a version of the query with or without the - sign in front of the parameter.
        // If you have N paramters like that in one query, you have 2^N possible distinct queries.
        //
        // We just ignore a unary +, since it's meaningless and unlikely to be common.
        // For -, we use these heuristics:
        //  a) If there's a space before the -, but not between the - and the constant, assume it's unary -
        //  b) If there's a space between both, assume binary
        //  c) If there's no space before or after, and it's preceded by other operator chars,
        //     assume unary unless it includes one of ~!@#%^&|`?
        //  d) If there's a ( or [ before the -, it's unary
        //  e) If there's an alpha-numeric char, ), or ] before the -, assume binary
        //  f) It's not a -, it's an empty -- comment on the preceding line
        //  otherwise assume it's a binary -

        let mut signed = false;
        let mut whitespace_after = false;
        let mut operator = false;
        let mut i = start - 1;
        loop {
            // Safety: We check the bounds here ourself
            let c = unsafe { *self.src.get_unchecked(i) } as char;
            if c.is_ascii_whitespace() {
                if signed {
                    // Case c if operator (but wasn't allowed to end in -) assume unary
                    if operator {
                        return true;
                    }
                    // Case b if whitespace after (binary), otherwise case a (unary)
                    return !whitespace_after;
                } else {
                    whitespace_after = true;
                }
            } else if c == '-' {
                // If this is the second '-', this could technically be an empty comment followed by whitespace (including at least one newline)
                // Otherwise, if it's an actual second '-', there had to be whitespace between them, so the case
                // above for a or b was triggered and execution never reached here. So it can only have been an empty comment.
                if signed {
                    // Case f, not a '-' at all
                    break;
                }
                signed = true;
            } else if c == '(' || c == '[' {
                // Case d, this is unary - (if there was a -, otherwise we return false)
                return signed;
            } else if signed {
                if REQUIRED_IF_OPERATOR_ENDS_IN_PLUS_OR_MINUS.as_bytes().contains(&(c as u8)) {
                    // Case c, assume the - is part of a multiple character operator
                    break;
                } else if OTHER_OPERATOR_CHARS.as_bytes().contains(&(c as u8)) {
                    operator = true;
                } else if operator {
                    return true;
                } else {
                    // Case e), assume binary -
                    break;
                }
            } else {
                // There wasn't a -, return false
                break;
            }
            if i == 0 {
                return signed;
            }
            i -= 1;
        }

        false
    }
}

fn append_tag(tags: &mut Vec<QueryTag>, tag: &mut QueryTag) {
    debug_assert_ne!(tag.key_len(), 0);
    debug_assert!(tag.val.start > tag.key.end);
    debug_assert!(tag.val.end >= tag.val.start);

    tags.push(std::mem::take(tag));
}