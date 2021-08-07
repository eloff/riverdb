use std::fmt::{Display, Write};
use std::any::Any;

use bytes::{BytesMut, Buf, BufMut};


pub fn check_formatting_placeholders_consumed(s: &str) {
    let mut open_pos = -1;
    let mut i = 0;
    for b in s.as_bytes().iter().cloned() {
        if b == '{' as u8 {
            if open_pos < 0 {
                open_pos = i;
            } else if open_pos == i - 1 {
                open_pos = -1;
            }
        }
        i += 1;
    }
    if open_pos >= 0 {
        panic!("too few arguments for the number of formatting placeholders");
    }
}

fn partition_fmt_str(s: &str) -> (&str, &str) {
    let mut open_pos = -1;
    let mut i = 0;
    for b in s.as_bytes().iter().cloned() {
        if b == '{' as u8 {
            if open_pos < 0 {
                open_pos = i;
            } else if open_pos == i - 1 {
                open_pos = -1;
            } else {
                panic!("{}", "expected closing }, got open {");
            }
        }
        i += 1;
        if b == '}' as u8 && open_pos >= 0 {
            return (&s[..open_pos as usize], &s[i as usize..]);
        }
    }
    panic!("{}", "expected format placeholder {...}");
}

pub fn write_escaped<'a, 'b, 'c, T: Any + Display>(out: &'b mut BytesMut, fmt_str: &'a str, value: &'c T) -> &'a str {
    let value_any = value as &dyn Any;
    let (prefix, fmt_remainder) = partition_fmt_str(fmt_str);
    let _ = out.write_str(prefix);
    if let Some(s) = value_any.downcast_ref::<&str>() {
        escape_str(out, s);
    } else if let Some(s) = value_any.downcast_ref::<String>() {
        escape_str(out, s.as_str());
    } else {
        let _ = out.write_fmt(format_args!("{}", value));
    }
    fmt_remainder
}

/// Writes s to f as a safely escaped single-quoted SQL string
pub fn escape_str(out: &mut BytesMut, s: &str) {
    // Escape all single quotes by doubling them up '' to escape them, and wrap the string in single quotes
    const SQ: u8 = '\'' as u8;
    out.put_u8(SQ);
    for c in s.as_bytes().iter().cloned() {
        if c == SQ {
            out.put_u8(SQ); // double it up to escape it
        }
        out.put_u8(c);
    }
    out.put_u8(SQ);
}

#[macro_export]
macro_rules! query {
    ($f: expr, $($args: expr),+) => {
        {
            let mut mb = crate::riverdb::pg::protocol::MessageBuilder::new(crate::riverdb::pg::protocol::Tag::QUERY);
            let out_ref = mb.bytes_mut();
            query!(@out_ref, $f, $($args),+);
            mb.finish()
        }
    };
    (@$out: ident, $f: expr,) => {};
    (@$out: ident, $f: expr, $arg: expr) => {
        let tail = crate::riverdb::pg::sql::write_escaped($out, $f, &$arg);
        crate::riverdb::pg::sql::check_formatting_placeholders_consumed(tail);
        let _ = std::fmt::Write::write_str($out, tail);
    };
    (@$out: ident, $f: expr, $arg: expr, $($args: expr),*) => {
        let tmp = crate::riverdb::pg::sql::write_escaped($out, $f, &$arg);
        query!(@$out, tmp, $($args),*);
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape() {
        let buf = query!("a {} b {} c {}{} d", "fo'o", "ba'r".to_string(), 42, 12.56);
        let result = std::str::from_utf8(&buf.as_slice()[buf.body_start() as usize..]).unwrap();
        assert_eq!(result, "a 'fo''o' b 'ba''r' c 4212.56 d");
    }

    #[test]
    #[should_panic(expected = "too few arguments for the number of formatting placeholders")]
    fn test_too_few_args() {
        query!("{} {}", 42);
    }

    #[test]
    #[should_panic(expected = "expected format placeholder {...}")]
    fn test_too_many_args() {
        query!("{}", 42, "foo");
    }

    #[test]
    #[should_panic(expected = "expected closing }, got open {")]
    fn test_malformed_placeholder() {
        query!("{ {", 12);
    }
}