use bytes::{Bytes};

use crate::riverdb::{Result};
use crate::riverdb::pg::protocol::{Messages, MessageBuilder, Tag};
use crate::riverdb::pg::sql::{QueryMessage, LiteralType};

#[derive(Debug)]
struct QueryParamTest {
    value: &'static str,
    ty: LiteralType,
    negated: bool,
    target_type: &'static str,
}

fn make_query(query: &'static [u8]) -> Result<QueryMessage> {
    let mut mb = MessageBuilder::new(Tag::QUERY);
    mb.write_bytes(query);
    let msgs = mb.finish();
    QueryMessage::new(msgs)
}

#[test]
fn test_normalize_ok() {
    let tests = &[
        (
            "select 1",
            "SELECT $1",
            vec![QueryParamTest { value: "1", ty: LiteralType::Integer, negated: false, target_type: "" }],
        ),
        (
            "select +1",
            "SELECT + $1",
            vec![QueryParamTest { value: "1", ty: LiteralType::Integer, negated: false, target_type: "" }],
        ),
        (
            "select -1",
            "SELECT $1",
            vec![QueryParamTest { value: "1", ty: LiteralType::Integer, negated: true, target_type: "" }],
        ),
        (
            "select - 1",
            "SELECT - $1",
            vec![QueryParamTest { value: "1", ty: LiteralType::Integer, negated: false, target_type: "" }],
        ),
        (
            "select coalesce(null,'ByteScout', null ,'Byte')",
            "SELECT COALESCE($1, $2, $3, $4)",
            vec![
                QueryParamTest { value: "NULL", ty: LiteralType::Null, negated: false, target_type: "" },
                QueryParamTest { value: "'ByteScout'", ty: LiteralType::String, negated: false, target_type: "" },
                QueryParamTest { value: "NULL", ty: LiteralType::Null, negated: false, target_type: "" },
                QueryParamTest { value: "'Byte'", ty: LiteralType::String, negated: false, target_type: "" },
            ],
        ),
        (
            "SELECT STDDEV(salary) AS stddev_salary,     STDDEV_POP(salary) AS pop_salary,\nSTDDEV_SAMP(salary) AS samp_salary\n    FROM\t\temployee;",
            "SELECT STDDEV(SALARY) AS STDDEV_SALARY, STDDEV_POP(SALARY) AS POP_SALARY, STDDEV_SAMP(SALARY) AS SAMP_SALARY FROM EMPLOYEE",
            vec![],
        ),
        (
            r#"select true, FALSE, null, .12, -4.0e3, -5, 'foo',"bar" from baz"#,
            r#"SELECT $1, $2, $3, $4, $5, $6, $7, "bar" FROM BAZ"#,
            vec![
                QueryParamTest { value: "TRUE", ty: LiteralType::Boolean, negated: false, target_type: "" },
                QueryParamTest { value: "FALSE", ty: LiteralType::Boolean, negated: false, target_type: "" },
                QueryParamTest { value: "NULL", ty: LiteralType::Null, negated: false, target_type: "" },
                QueryParamTest { value: ".12", ty: LiteralType::Numeric, negated: false, target_type: "" },
                QueryParamTest { value: "4.0e3", ty: LiteralType::Numeric, negated: true, target_type: "" },
                QueryParamTest { value: "5", ty: LiteralType::Integer, negated: true, target_type: "" },
                QueryParamTest { value: "'foo'", ty: LiteralType::String, negated: false, target_type: "" },
            ],
        ),
        (
            "selECT $$quoted$$, $tag$quoted with tag$tag$, b'1010', e'\\n', U&'\\0441\\043B\\043E\\043D'",
            "SELECT $1, $2, $3, $4, $5",
            vec![
                QueryParamTest { value: "$$quoted$$", ty: LiteralType::DollarString, negated: false, target_type: "" },
                QueryParamTest { value: "$tag$quoted with tag$tag$", ty: LiteralType::DollarString, negated: false, target_type: "" },
                QueryParamTest { value: "b'1010'", ty: LiteralType::BitString, negated: false, target_type: "" },
                QueryParamTest { value: "e'\\n'", ty: LiteralType::EscapeString, negated: false, target_type: "" },
                QueryParamTest { value: "U&'\\0441\\043B\\043E\\043D'", ty: LiteralType::UnicodeString, negated: false, target_type: "" },
            ],
        ),
        (
            "SELECT -.4e+32, -.4E-32",
            "SELECT $1, $2",
            vec![
                QueryParamTest { value: ".4e+32", ty: LiteralType::Numeric, negated: true, target_type: "" },
                QueryParamTest { value: ".4E-32", ty: LiteralType::Numeric, negated: true, target_type: "" },
            ],
        ),
        (
            "SELECT +.0",
            "SELECT + $1",
            vec![
                QueryParamTest { value: ".0", ty: LiteralType::Numeric, negated: false, target_type: "" },
            ],
        ),
        (
            "SELECT +1.",
            "SELECT + $1",
            vec![QueryParamTest { value: "1", ty: LiteralType::Integer, negated: false, target_type: "" }],
        ),
        (
            "SELECT 1 -- foo=bar",
            "SELECT $1",
            vec![QueryParamTest { value: "1", ty: LiteralType::Integer, negated: false, target_type: "" }],
        ),
        (
            "select fal",
            "SELECT FAL",
            vec![],
        ),
        (
            " select leading space",
            "SELECT LEADING SPACE",
            vec![],
        ),
        (
            "select trailing space ",
            "SELECT TRAILING SPACE",
            vec![],
        ),
        (
            "\tselect leading tab",
            "SELECT LEADING TAB",
            vec![],
        ),
        (
            "select trailing tab\t",
            "SELECT TRAILING TAB",
            vec![],
        ),
        (
            "\nselect leading newline",
            "SELECT LEADING NEWLINE",
            vec![],
        ),
        (
            "select trailing newline\r",
            "SELECT TRAILING NEWLINE",
            vec![],
        ),
        // string continuations require a newline
        (
            "select 'combine'\n'strings'",
            "SELECT $1",
            vec![QueryParamTest { value: "'combinestrings'", ty: LiteralType::String, negated: false, target_type: "" }],
        ),
        // string continuations require a newline
        (
            "select 'no combine', 'strings'",
            "SELECT $1, $2",
            vec![
                QueryParamTest { value: "'no combine'", ty: LiteralType::String, negated: false, target_type: "" },
                QueryParamTest { value: "'strings'", ty: LiteralType::String, negated: false, target_type: "" },
            ],
        ),
        (
            "select foo.bar from foo",
            "SELECT FOO.BAR FROM FOO",
            vec![],
        ),
        (
            "select foo . bar from foo",
            "SELECT FOO.BAR FROM FOO",
            vec![],
        ),
        (
            "select foo. bar from foo",
            "SELECT FOO.BAR FROM FOO",
            vec![],
        ),
        (
            "select foo .bar from foo",
            "SELECT FOO.BAR FROM FOO",
            vec![],
        ),
        (
            "select e'foo\\''",
            "SELECT $1",
            vec![
                QueryParamTest { value: "e'foo\\''", ty: LiteralType::EscapeString, negated: false, target_type: "" },
            ],
        ),
        (
            r#"select "fo""o" from bar"#,
            r#"SELECT "fo""o" FROM BAR"#,
            vec![],
        ),
        (
            "select u&1 from bar",
            "SELECT U & $1 FROM BAR",
            vec![
                QueryParamTest { value: "1", ty: LiteralType::Integer, negated: false, target_type: "" },
            ],
        ),
        (
            "select foo && true from bar",
            "SELECT FOO && $1 FROM BAR",
            vec![
                QueryParamTest { value: "TRUE", ty: LiteralType::Boolean, negated: false, target_type: "" },
            ],
        ),
        (
            "select fOo#>>'{a,2}' from bar",
            "SELECT FOO #>> $1 FROM BAR",
            vec![
                QueryParamTest { value: "'{a,2}'", ty: LiteralType::String, negated: false, target_type: "" },
            ],
        ),
        (
            "select foo #- from bar",
            "SELECT FOO #- FROM BAR",
            vec![],
        ),
        (
            "select --\n12",
            "SELECT $1",
            vec![
                QueryParamTest { value: "12", ty: LiteralType::Integer, negated: false, target_type: "" },
            ],
        ),
        (
            "select-1",
            "SELECT - $1", // this is wrong, but we don't know that until we get the ast
            vec![
                QueryParamTest { value: "1", ty: LiteralType::Integer, negated: false, target_type: "" },
            ],
        ),
        (
            "select+1",
            "SELECT + $1", // this is wrong, but we don't know that until we get the ast
            vec![
                QueryParamTest { value: "1", ty: LiteralType::Integer, negated: false, target_type: "" },
            ],
        ),
        (
            "-1",
            "- $1", // this is not valid sql, but it tests an otherwise unreachable edge case
            vec![
                QueryParamTest { value: "1", ty: LiteralType::Integer, negated: false, target_type: "" },
            ],
        ),
        (
            "select arr[-1] from foo",
            "SELECT ARR[$1] FROM FOO",
            vec![
                QueryParamTest { value: "1", ty: LiteralType::Integer, negated: true, target_type: "" },
            ],
        ),
        (
            "select (-1)",
            "SELECT($1)",
            vec![
                QueryParamTest { value: "1", ty: LiteralType::Integer, negated: true, target_type: "" },
            ],
        ),
    ];

    for (query, normalized, params) in tests {
        println!("{} of len {}", query, query.len());
        let res = make_query(query.as_bytes()).expect("expected Ok(Query)");
        let query = res.query();
        assert_eq!(query.normalized(), *normalized);
        for (param, expected) in query.params().iter().zip(params) {
            assert_eq!(param.ty, expected.ty);
            assert_eq!(param.negated, expected.negated);
            assert_eq!(query.param(param), expected.value);
        }
    }
}

#[test]
fn test_normalize_many_ok() {
    let tests = &[
        (
            "select 1; select -2",
            vec![
                (
                    "SELECT $1",
                    vec![
                        QueryParamTest { value: "1", ty: LiteralType::Integer, negated: false, target_type: "" },
                    ],
                ), (
                     "SELECT $1",
                     vec![
                         QueryParamTest { value: "2", ty: LiteralType::Integer, negated: true, target_type: "" },
                     ],
                )
            ],
        ),
        (
            "select 1*2; -- foo\nselect 2+-2; /* comment */\n",
            vec![
                (
                    "SELECT $1 * $2",
                    vec![
                        QueryParamTest { value: "1", ty: LiteralType::Integer, negated: false, target_type: "" },
                        QueryParamTest { value: "2", ty: LiteralType::Integer, negated: false, target_type: "" },
                    ],
                ), (
                    "SELECT $1 + $2",
                    vec![
                        QueryParamTest { value: "2", ty: LiteralType::Integer, negated: false, target_type: "" },
                        QueryParamTest { value: "2", ty: LiteralType::Integer, negated: true, target_type: "" },
                    ],
                )
            ],
        ),
    ];

    for (query, expected_vec) in tests {
        println!("{} of len {}", query, query.len());
        let res = make_query(query.as_bytes()).expect("expected Ok(Query)");
        let mut query = res.query();
        for (normalized, params) in expected_vec {
            assert_eq!(query.normalized(), *normalized);
            for (param, expected) in query.params.iter().zip(params) {
                assert_eq!(param.ty, expected.ty);
                assert_eq!(param.negated, expected.negated);
                assert_eq!(query.param(param), expected.value);
            }
            if let Some(next_query) = query.next.as_deref() {
                query = next_query;
            }
        }
    }
}

#[test]
fn test_normalize_tags() {
    let tests = &[
        (
            "SELECT /* foo=bar dotted.and-dashed_baz=1 */ 1",
            vec!["SELECT $1"],
            vec![("FOO", "bar"), ("DOTTED.AND-DASHED_BAZ", "1")],
        ),
        (
            "SELECT /* foo=bar */ 1; -- baz=12\n SELECT /* mux=3.3e9 */ 'str';",
            vec!["SELECT $1", "SELECT $1"],
            vec![("FOO", "bar"), ("MUX", "3.3e9")],
        ),
    ];

    for (query, normalized_vec, tags) in tests {
        let res = make_query(query.as_bytes()).expect("expected Ok(Query)");
        for &(key, val) in tags {
            assert_eq!(res.tag(key), Some(val));
        }

        let mut query = res.query();
        for normalized in normalized_vec {
            assert_eq!(query.normalized.as_str(), *normalized);
            if let Some(next_query) = query.next.as_deref() {
                query = next_query;
            }
        }
    }
}

#[test]
fn test_normalize_utf8_err() {
    const TESTS: &[(&'static [u8], &'static str)] = &[
        (&[0xff, 0xff], "invalid utf8"),
        (&['1' as u8, 0xff, 0xff], "invalid utf8"),
        (&['1' as u8, 0xff, 0xff], "invalid utf8"),
        (&['b' as u8, '1' as u8, 0xff, 0xff], "invalid utf8"),
        (&['/' as u8, '*' as u8, 0xff, 0xff], "invalid utf8"),
        (&['"' as u8, 0xff, 0xff], "invalid utf8"),
        (&['#' as u8, 0xff, 0xff], "invalid utf8"),
        (&['s' as u8, 'e' as u8, 'l' as u8, 0xff, 0xff], "invalid utf8"),
    ];

    for &(bytes, err) in TESTS {
        let res = make_query(bytes);
        let err_msg = res.expect_err("expected an error").to_string();
        assert!(err_msg.contains(err), "expected {} in err {}", err, err_msg);
    }
}

#[test]
fn test_normalize_err() {
    const TESTS: &[(&'static str, &'static str)] = &[
        ("select 'unterminated string", "unexpected eof parsing string"),
        (r#"select "foo"#, "unexpected eof parsing quoted identifier"),
        ("select $tag$foo$tag", r#"missing ending "$tag$" for $ quoted string"#),
        ("select b'101", "unexpected eof while parsing bit string"),
        ("b'12'", "unexpected char '2' in bit string literal"),
        ("(¯)", "unexpected char '¯' in query"),
        ("select /* foo", "unexpected eof while parsing c-style comment"),
        ("select /* foo /", "unexpected eof while parsing c-style comment"),
        ("select /* foo *", "unexpected eof while parsing c-style comment"),
        ("select /* /* foo */", "unexpected eof while parsing c-style comment"),
        ("select 1e+", "numeric constant cannot end in exponent '+'"),
        ("select 1x1", "unexpected 'x' in numeric value"),
        ("select 1..1", "cannot have two decimals in numeric value"),
        ("select $", "invalid char '$' for operator"),
    ];

    for &(query, err) in TESTS {
        let res = make_query(query.as_bytes());
        let err_msg = res.expect_err("expected an error").to_string();
        assert!(err_msg.contains(err), "expected {} in err {}", err, err_msg);
    }
}