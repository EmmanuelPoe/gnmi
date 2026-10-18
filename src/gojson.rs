//! JSON encoding that matches Go's encoding/json byte for byte for the values
//! gnmic emits: float formatting, optional HTML escaping, U+2028/U+2029
//! escaping and json.MarshalIndent layout.

use std::io::{self, Write};

use serde::Serialize;
use serde_json::ser::Formatter;

pub struct GoFormatter<'a> {
    indent: Option<&'a [u8]>,
    escape_html: bool,
    level: usize,
    has_value: bool,
}

impl GoFormatter<'_> {
    fn newline<W: ?Sized + Write>(&self, w: &mut W) -> io::Result<()> {
        if let Some(indent) = self.indent {
            w.write_all(b"\n")?;
            for _ in 0..self.level {
                w.write_all(indent)?;
            }
        }
        Ok(())
    }
}

/// Go's float encoding: shortest representation, exponent notation only
/// outside [1e-6, 1e21), and exponents without leading zeros ("1e-7", "1e+21").
pub fn write_go_float<W: ?Sized + Write>(w: &mut W, f: f64) -> io::Result<()> {
    let abs = f.abs();
    if abs == 0.0 || (1e-6..1e21).contains(&abs) {
        return write!(w, "{f}");
    }
    let s = format!("{f:e}");
    match s.split_once('e') {
        Some((mant, exp)) if !exp.starts_with('-') => write!(w, "{mant}e+{exp}"),
        _ => w.write_all(s.as_bytes()),
    }
}

impl Formatter for GoFormatter<'_> {
    fn write_f64<W: ?Sized + Write>(&mut self, w: &mut W, v: f64) -> io::Result<()> {
        write_go_float(w, v)
    }

    fn write_f32<W: ?Sized + Write>(&mut self, w: &mut W, v: f32) -> io::Result<()> {
        // Go formats float32 with 32-bit shortest representation.
        let shortest: f64 = format!("{v}").parse().unwrap_or(v as f64);
        write_go_float(w, shortest)
    }

    fn write_string_fragment<W: ?Sized + Write>(&mut self, w: &mut W, frag: &str) -> io::Result<()> {
        let mut start = 0;
        for (i, c) in frag.char_indices() {
            let esc: &[u8] = match c {
                '<' if self.escape_html => b"\\u003c",
                '>' if self.escape_html => b"\\u003e",
                '&' if self.escape_html => b"\\u0026",
                '\u{2028}' => b"\\u2028",
                '\u{2029}' => b"\\u2029",
                _ => continue,
            };
            w.write_all(&frag.as_bytes()[start..i])?;
            w.write_all(esc)?;
            start = i + c.len_utf8();
        }
        w.write_all(&frag.as_bytes()[start..])
    }

    fn begin_array<W: ?Sized + Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.level += 1;
        self.has_value = false;
        w.write_all(b"[")
    }

    fn end_array<W: ?Sized + Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.level -= 1;
        if self.has_value {
            self.newline(w)?;
        }
        w.write_all(b"]")
    }

    fn begin_array_value<W: ?Sized + Write>(&mut self, w: &mut W, first: bool) -> io::Result<()> {
        if !first {
            w.write_all(b",")?;
        }
        self.newline(w)
    }

    fn end_array_value<W: ?Sized + Write>(&mut self, _: &mut W) -> io::Result<()> {
        self.has_value = true;
        Ok(())
    }

    fn begin_object<W: ?Sized + Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.level += 1;
        self.has_value = false;
        w.write_all(b"{")
    }

    fn end_object<W: ?Sized + Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.level -= 1;
        if self.has_value {
            self.newline(w)?;
        }
        w.write_all(b"}")
    }

    fn begin_object_key<W: ?Sized + Write>(&mut self, w: &mut W, first: bool) -> io::Result<()> {
        if !first {
            w.write_all(b",")?;
        }
        self.newline(w)
    }

    fn begin_object_value<W: ?Sized + Write>(&mut self, w: &mut W) -> io::Result<()> {
        w.write_all(if self.indent.is_some() { b": " } else { b":" })
    }

    fn end_object_value<W: ?Sized + Write>(&mut self, _: &mut W) -> io::Result<()> {
        self.has_value = true;
        Ok(())
    }
}

/// Appends the Go encoding of `v` to `out`. `indent` selects MarshalIndent.
pub fn write<T: Serialize + ?Sized>(
    out: &mut Vec<u8>,
    v: &T,
    indent: Option<&str>,
    escape_html: bool,
) -> serde_json::Result<()> {
    let f = GoFormatter {
        indent: indent.map(str::as_bytes),
        escape_html,
        level: 0,
        has_value: false,
    };
    v.serialize(&mut serde_json::Serializer::with_formatter(out, f))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn enc(v: &serde_json::Value, indent: Option<&str>, html: bool) -> String {
        let mut b = Vec::new();
        write(&mut b, v, indent, html).unwrap();
        String::from_utf8(b).unwrap()
    }

    #[test]
    fn floats_like_go() {
        let cases = [
            (1.0, "1"),
            (0.1, "0.1"),
            (-2.5, "-2.5"),
            (1e20, "100000000000000000000"),
            (1e21, "1e+21"),
            (1.5e-7, "1.5e-7"),
            (0.000001, "0.000001"),
            (123456789.125, "123456789.125"),
            (0.0, "0"),
        ];
        for (f, want) in cases {
            assert_eq!(enc(&json!(f), None, false), want);
        }
        let mut b = Vec::new();
        write(&mut b, &0.1f32, None, false).unwrap();
        assert_eq!(b, b"0.1");
    }

    #[test]
    fn escaping_and_layout() {
        let v = json!({"b": [1, {"x": "<&>\u{2028}"}], "a": {}, "c": []});
        assert_eq!(
            enc(&v, None, false),
            r#"{"a":{},"b":[1,{"x":"<&>\u2028"}],"c":[]}"#
        );
        assert_eq!(
            enc(&v, None, true),
            r#"{"a":{},"b":[1,{"x":"\u003c\u0026\u003e\u2028"}],"c":[]}"#
        );
        assert_eq!(
            enc(&v, Some("  "), false),
            "{\n  \"a\": {},\n  \"b\": [\n    1,\n    {\n      \"x\": \"<&>\\u2028\"\n    }\n  ],\n  \"c\": []\n}"
        );
        assert_eq!(enc(&json!("a\"\\\n\u{1}"), None, false), r#""a\"\\\n\u0001""#);
    }
}
