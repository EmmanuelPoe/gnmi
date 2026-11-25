// Portions derived from gnmic (https://github.com/openconfig/gnmic),
// Copyright Nokia and the gNMIc contributors, Apache-2.0.
// Translated from Go to Rust and modified for gnmic-rs. See NOTICE.

//! XPath <-> gNMI Path conversion, ported from gnmic's pkg/api/path.

use std::collections::BTreeMap;
use std::fmt::Write;

use anyhow::{Result, bail};

use crate::proto::gnmi::{Path, PathElem};

/// Parses an xpath such as `openconfig:/interfaces/interface[name=Gi1]/state`.
pub fn parse(p: &str) -> Result<Path> {
    let mut origin = String::new();
    let mut rest = p;
    if let Some(idx) = p.find(':') {
        let after = &p[idx + 1..];
        if !p.starts_with('/') && !p[..idx].contains('/') && (after.is_empty() || after.starts_with('/')) {
            origin = p[..idx].to_string();
            rest = after;
        }
    }
    Ok(Path {
        origin,
        elem: path_elems(rest)?,
        ..Default::default()
    })
}

fn path_elems(p: &str) -> Result<Vec<PathElem>> {
    // Split on '/' outside of [...] key sections; '\[' and '\]' are escapes.
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut in_key = false;
    let mut prev = '\0';
    for c in p.chars() {
        match c {
            '[' if prev != '\\' => {
                if in_key {
                    bail!("malformed xpath {p:?}");
                }
                in_key = true;
            }
            ']' if prev != '\\' => {
                if !in_key {
                    bail!("malformed xpath {p:?}");
                }
                in_key = false;
            }
            '/' if !in_key => {
                parts.push(std::mem::take(&mut cur));
                prev = c;
                continue;
            }
            _ => {}
        }
        cur.push(c);
        prev = c;
    }
    if in_key {
        bail!("malformed xpath {p:?}");
    }
    parts.push(cur);
    parts
        .into_iter()
        .filter(|s| !s.is_empty())
        .map(|s| path_elem(&s))
        .collect()
}

fn path_elem(s: &str) -> Result<PathElem> {
    let mut prev = '\0';
    let idx = s.char_indices().find_map(|(i, c)| {
        let hit = c == '[' && prev != '\\';
        prev = c;
        hit.then_some(i)
    });
    let (name, key) = match idx {
        Some(0) => bail!("empty path element name in {s:?}"),
        Some(i) => (&s[..i], parse_keys(&s[i..])?),
        None => (s, BTreeMap::new()),
    };
    Ok(PathElem {
        name: name.to_string(),
        key,
    })
}

fn parse_keys(s: &str) -> Result<BTreeMap<String, String>> {
    let unescape = |x: &str| x.replace("\\]", "]").replace("\\[", "[");
    let mut kvs = BTreeMap::new();
    let mut in_key = false;
    let mut start = 0;
    let mut prev = '\0';
    for (i, c) in s.char_indices() {
        match c {
            '[' | ']' if prev == '\\' => {}
            '[' => {
                if in_key {
                    bail!("malformed xpath key {s:?}");
                }
                in_key = true;
                start = i + 1;
            }
            ']' => {
                if !in_key {
                    bail!("malformed xpath key {s:?}");
                }
                let kv = &s[start..i];
                let Some((k, v)) = kv.split_once('=') else {
                    bail!("malformed xpath key {s:?}");
                };
                if k.is_empty() || v.is_empty() {
                    bail!("malformed xpath key {s:?}");
                }
                kvs.insert(unescape(k), unescape(v));
                in_key = false;
            }
            _ if !in_key => bail!("malformed xpath key {s:?}"),
            _ => {}
        }
        prev = c;
    }
    if in_key {
        bail!("malformed xpath key {s:?}");
    }
    Ok(kvs)
}

/// gnmic's GnmiPathToXPath(p, false): no leading '/', sorted keys, `origin:/` prefix.
pub fn to_xpath(p: Option<&Path>) -> String {
    let Some(p) = p else { return String::new() };
    let mut s = String::new();
    if !p.origin.is_empty() {
        s.push_str(&p.origin);
        s.push_str(":/");
    }
    for (i, e) in p.elem.iter().enumerate() {
        if i > 0 {
            s.push('/');
        }
        s.push_str(&e.name);
        for (k, v) in &e.key {
            let _ = write!(s, "[{k}={v}]");
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn elems(p: &Path) -> Vec<(&str, Vec<(&str, &str)>)> {
        p.elem
            .iter()
            .map(|e| {
                let keys = e.key.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                (e.name.as_str(), keys)
            })
            .collect()
    }

    #[test]
    fn parse_paths() {
        let p = parse("/interfaces/interface[name=Gi1/0/1]/state/counters").unwrap();
        assert_eq!(p.origin, "");
        assert_eq!(
            elems(&p),
            vec![
                ("interfaces", vec![]),
                ("interface", vec![("name", "Gi1/0/1")]),
                ("state", vec![]),
                ("counters", vec![]),
            ]
        );
        let p = parse("openconfig:/a[k1=v1][k2=v2]").unwrap();
        assert_eq!(p.origin, "openconfig");
        assert_eq!(elems(&p), vec![("a", vec![("k1", "v1"), ("k2", "v2")])]);
        let p = parse("rfc7951:").unwrap();
        assert_eq!(p.origin, "rfc7951");
        assert!(p.elem.is_empty());
        // A colon that is not followed by '/' is part of the element name.
        let p = parse("Cisco-IOS-XE-process-cpu-oper:cpu-usage/cpu-utilization").unwrap();
        assert_eq!(p.origin, "");
        assert_eq!(p.elem[0].name, "Cisco-IOS-XE-process-cpu-oper:cpu-usage");
        let p = parse(r"/a[k=x\]y]").unwrap();
        assert_eq!(elems(&p), vec![("a", vec![("k", "x]y")])]);
        assert!(parse("").unwrap().elem.is_empty());
        for bad in ["/a[k=v", "/a]", "/[k=v]", "/a[k]", "/a[=v]", "/a[k=]", "/a[k=v]x"] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn xpath_round_trip() {
        for s in [
            "interfaces/interface[name=Gi1/0/1]/state",
            "openconfig:/a[k1=v1][k2=v2]/b",
        ] {
            assert_eq!(to_xpath(Some(&parse(s).unwrap())), s);
        }
        assert_eq!(to_xpath(Some(&parse("/a/b").unwrap())), "a/b");
        assert_eq!(to_xpath(None), "");
    }
}
