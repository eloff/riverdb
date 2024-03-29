use std::fmt::{Display, Write};
use std::any::Any;

use bytes::{BytesMut, BufMut};


/// Verify that all formatting placeholders in the input string have been replaced.
/// This is public because it's referenced in the generated code from the query! macro.
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

/// Write a value to out BytesMut buffer using Display::fmt or
/// escaping it if it's a string.
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

/// Construct a Messages object containing the query with it's formatted
/// arguments properly escaped for PostgreSQL. Note that only Strings
/// and numeric/boolean primitive types are supported. Other types can
/// be used if they implement Any+Display, but must do their own escaping.
///
/// ```
/// use riverdb::query;
///
/// let escaped = query!(
///     "select * from students where name = {}",
///     "Robert'); DROP TABLE students;--"
/// );
/// let msg = escaped.first().unwrap();
/// let text = msg.reader().read_str().unwrap();
/// assert_eq!(text, "select * from students where name = 'Robert''); DROP TABLE students;--'");
/// ```
#[macro_export]
macro_rules! query {
    ($f: expr, $($args: expr),*) => {
        {
            let mut mb = crate::riverdb::pg::protocol::MessageBuilder::new(crate::riverdb::pg::protocol::Tag::QUERY);
            let out_ref = mb.bytes_mut();
            query!(@out_ref, $f, $($args),*);
            mb.write_byte(0);
            mb.finish()
        }
    };
    (@$out: ident, $f: expr, ) => {
        crate::riverdb::pg::sql::check_formatting_placeholders_consumed($f);
        let _ = std::fmt::Write::write_str($out, $f);
    };
    (@$out: ident, $f: expr, $arg: expr) => {
        let tail = crate::riverdb::pg::sql::write_escaped($out, $f, &$arg);
        query!(@$out, tail, );
    };
    (@$out: ident, $f: expr, $arg: expr, $($args: expr),*) => {
        let tmp = crate::riverdb::pg::sql::write_escaped($out, $f, &$arg);
        query!(@$out, tmp, $($args),*);
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_escape() {
        let buf = query!("a {} b {} c {} d {}{} e", "fo'o", "ba'r".to_string(), "no quotes", 42, 12.56);
        let msg = buf.first().expect("no message returned");
        let result = msg.reader().read_str().unwrap();
        assert_eq!(result, "a 'fo''o' b 'ba''r' c 'no quotes' d 4212.56 e");
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