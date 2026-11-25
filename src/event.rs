// Portions derived from gnmic (https://github.com/openconfig/gnmic),
// Copyright Nokia and the gNMIc contributors, Apache-2.0.
// Translated from Go to Rust and modified for gnmic-rs. See NOTICE.

//! gnmic "event" model: one event per update/delete with path keys as tags
//! and flattened leaf values (port of pkg/formatters/event.go).

use std::collections::BTreeMap;

use anyhow::Result;
use base64::Engine;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::output::Meta;
use crate::proto::gnmi::{Notification, Path, TypedValue, typed_value};

#[derive(Debug, Default, Serialize)]
pub struct Event {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(skip_serializing_if = "is_zero")]
    pub timestamp: i64,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub values: BTreeMap<String, Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub deletes: Vec<String>,
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

fn b64(b: &[u8]) -> Value {
    Value::String(base64::engine::general_purpose::STANDARD.encode(b))
}

pub fn f64_value(f: f64) -> Value {
    serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number)
}

/// float32 values keep their 32-bit shortest representation (as Go prints them).
pub fn f32_value(f: f32) -> Value {
    f64_value(format!("{f}").parse().unwrap_or(f as f64))
}

/// gnmic's getValue: the value as found in the JSON output format.
#[allow(deprecated)]
pub fn typed_value(tv: Option<&TypedValue>) -> Result<Value> {
    use typed_value::Value as V;
    let Some(v) = tv.and_then(|tv| tv.value.as_ref()) else {
        return Ok(Value::Null);
    };
    Ok(match v {
        V::StringVal(s) | V::AsciiVal(s) => Value::String(s.clone()),
        V::IntVal(i) => (*i).into(),
        V::UintVal(u) => (*u).into(),
        V::BoolVal(b) => (*b).into(),
        V::BytesVal(b) | V::ProtoBytes(b) => b64(b),
        V::FloatVal(f) => f32_value(*f),
        V::DoubleVal(f) => f64_value(*f),
        V::DecimalVal(d) => {
            // Go marshals the *gnmi.Decimal64 struct.
            let mut m = Map::new();
            if d.digits != 0 {
                m.insert("digits".into(), d.digits.into());
            }
            if d.precision != 0 {
                m.insert("precision".into(), d.precision.into());
            }
            Value::Object(m)
        }
        V::LeaflistVal(l) => {
            // Go marshals the *gnmi.ScalarArray struct.
            let mut m = Map::new();
            if !l.element.is_empty() {
                let elems = l.element.iter().map(go_typed_value_struct).collect();
                m.insert("element".into(), Value::Array(elems));
            }
            Value::Object(m)
        }
        V::AnyVal(a) => any_value(a),
        V::JsonVal(b) | V::JsonIetfVal(b) => {
            if b.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(b)?
            }
        }
    })
}

fn any_value(a: &prost_types::Any) -> Value {
    let mut m = Map::new();
    if !a.type_url.is_empty() {
        m.insert("type_url".into(), a.type_url.clone().into());
    }
    if !a.value.is_empty() {
        m.insert("value".into(), b64(&a.value));
    }
    Value::Object(m)
}

/// encoding/json rendering of a Go *gnmi.TypedValue struct: {"Value":{"IntVal":1}}.
#[allow(deprecated)]
fn go_typed_value_struct(tv: &TypedValue) -> Value {
    use typed_value::Value as V;
    let inner = tv.value.as_ref().map(|v| {
        let (k, val) = match v {
            V::StringVal(s) => ("StringVal", Value::String(s.clone())),
            V::AsciiVal(s) => ("AsciiVal", Value::String(s.clone())),
            V::IntVal(i) => ("IntVal", (*i).into()),
            V::UintVal(u) => ("UintVal", (*u).into()),
            V::BoolVal(b) => ("BoolVal", (*b).into()),
            V::BytesVal(b) => ("BytesVal", b64(b)),
            V::ProtoBytes(b) => ("ProtoBytes", b64(b)),
            V::JsonVal(b) => ("JsonVal", b64(b)),
            V::JsonIetfVal(b) => ("JsonIetfVal", b64(b)),
            V::FloatVal(f) => ("FloatVal", f32_value(*f)),
            V::DoubleVal(f) => ("DoubleVal", f64_value(*f)),
            V::DecimalVal(_) | V::LeaflistVal(_) => {
                let k = if matches!(v, V::DecimalVal(_)) {
                    "DecimalVal"
                } else {
                    "LeaflistVal"
                };
                let tv = TypedValue {
                    value: Some(v.clone()),
                };
                (k, typed_value(Some(&tv)).unwrap_or(Value::Null))
            }
            V::AnyVal(a) => ("AnyVal", any_value(a)),
        };
        let mut m = Map::new();
        m.insert(k.into(), val);
        Value::Object(m)
    });
    let mut m = Map::new();
    m.insert("Value".into(), inner.unwrap_or(Value::Null));
    Value::Object(m)
}

/// gnmic's getValueFlat: leaf values keyed by path, JSON objects flattened.
#[allow(deprecated)]
fn flat_values(prefix: &str, tv: Option<&TypedValue>) -> Result<BTreeMap<String, Value>> {
    use typed_value::Value as V;
    let mut out = BTreeMap::new();
    let Some(v) = tv.and_then(|tv| tv.value.as_ref()) else {
        return Ok(out);
    };
    let value = match v {
        V::DecimalVal(d) => f64_value(d.digits as f64 / 10f64.powi(d.precision as i32)),
        V::LeaflistVal(l) => Value::Array(
            l.element
                .iter()
                .map(|e| typed_value(Some(e)))
                .collect::<Result<_>>()?,
        ),
        V::JsonVal(b) | V::JsonIetfVal(b) => {
            if b.is_empty() {
                return Ok(out);
            }
            match normalize_empty(serde_json::from_slice(b)?) {
                Value::Object(m) => {
                    for (k, v) in m {
                        flatten(&mut out, format!("{prefix}/{k}"), v);
                    }
                    return Ok(out);
                }
                other => other,
            }
        }
        _ => typed_value(tv)?,
    };
    out.insert(prefix.to_string(), value);
    Ok(out)
}

/// RFC 7951 encodes the YANG `empty` type as [null]. Like gnmic's
/// normalizeEmptyRFC7951, every null (and [null]) becomes true.
fn normalize_empty(v: Value) -> Value {
    match v {
        Value::Null => Value::Bool(true),
        Value::Array(a) if a.len() == 1 && a[0].is_null() => Value::Bool(true),
        Value::Array(a) => Value::Array(a.into_iter().map(normalize_empty).collect()),
        Value::Object(m) => Value::Object(m.into_iter().map(|(k, v)| (k, normalize_empty(v))).collect()),
        v => v,
    }
}

/// go-map-flattener: nested keys joined with '/', list indexes with '.'.
fn flatten(out: &mut BTreeMap<String, Value>, prefix: String, v: Value) {
    match v {
        Value::Object(m) => {
            for (k, v) in m {
                flatten(out, format!("{prefix}/{k}"), v);
            }
        }
        Value::Array(a) => {
            for (i, v) in a.into_iter().enumerate() {
                flatten(out, format!("{prefix}.{i}"), v);
            }
        }
        v => {
            out.insert(prefix, v);
        }
    }
}

/// Path without keys (origin as "origin:") and its keys as tags.
fn tags_from_path(p: Option<&Path>) -> (String, BTreeMap<String, String>) {
    let mut name = String::new();
    let mut tags = BTreeMap::new();
    let Some(p) = p else { return (name, tags) };
    if !p.origin.is_empty() {
        name.push_str(&p.origin);
        name.push(':');
    }
    for e in &p.elem {
        if !e.name.is_empty() {
            name.push('/');
            name.push_str(&e.name);
        }
        let short = e.name.rsplit(':').next().unwrap_or_default();
        for (k, v) in &e.key {
            let tag = if e.name.is_empty() {
                k.clone()
            } else {
                format!("{short}_{k}")
            };
            tags.insert(tag, v.clone());
        }
    }
    if !p.target.is_empty() {
        tags.insert("target".into(), p.target.clone());
    }
    (name, tags)
}

fn join_path(prefix: &str, path: &str) -> String {
    format!(
        "{}/{}",
        prefix.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn merge_path_tags(e: &mut Event, path_name: &str, tags: BTreeMap<String, String>) {
    for (k, v) in tags {
        match e.tags.get(&k) {
            Some(existing) if *existing != v => {
                e.tags.insert(format!("{path_name}_{k}"), v);
            }
            Some(_) => {}
            None => {
                e.tags.insert(k, v);
            }
        }
    }
}

fn add_meta_tags(e: &mut Event, meta: &Meta) {
    for (k, v) in meta.iter() {
        if k == "format" {
            continue;
        }
        if e.tags.contains_key(k) {
            e.tags.insert(format!("meta_{k}"), v.clone());
        } else {
            e.tags.insert(k.clone(), v.clone());
        }
    }
}

/// Converts a notification into events: updates first, then deletes.
pub fn notification_events(name: &str, n: &Notification, meta: &Meta) -> Result<Vec<Event>> {
    let (prefix, prefix_tags) = tags_from_path(n.prefix.as_ref());
    let mut evs = Vec::with_capacity(n.update.len() + n.delete.len());
    for u in &n.update {
        let (pname, ptags) = tags_from_path(u.path.as_ref());
        let path_name = join_path(&prefix, &pname);
        let mut e = Event {
            name: name.to_string(),
            timestamp: n.timestamp,
            tags: prefix_tags.clone(),
            ..Default::default()
        };
        merge_path_tags(&mut e, &path_name, ptags);
        e.values = flat_values(&path_name, u.val.as_ref())?;
        add_meta_tags(&mut e, meta);
        evs.push(e);
    }
    for d in &n.delete {
        let (pname, ptags) = tags_from_path(Some(d));
        let path_name = join_path(&prefix, &pname);
        let mut e = Event {
            name: name.to_string(),
            timestamp: n.timestamp,
            tags: prefix_tags.clone(),
            ..Default::default()
        };
        merge_path_tags(&mut e, &path_name, ptags);
        e.deletes.push(path_name);
        add_meta_tags(&mut e, meta);
        evs.push(e);
    }
    Ok(evs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path;
    use crate::proto::gnmi::Update;

    fn json_ietf(s: &str) -> Option<TypedValue> {
        Some(TypedValue {
            value: Some(typed_value::Value::JsonIetfVal(s.as_bytes().to_vec())),
        })
    }

    #[test]
    fn events_from_notification() {
        let mut prefix = path::parse("openconfig:/interfaces/interface[name=Gi1]").unwrap();
        prefix.target = "r1".into();
        let n = Notification {
            timestamp: 42,
            prefix: Some(prefix),
            update: vec![
                Update {
                    path: Some(path::parse("state/counters").unwrap()),
                    val: json_ietf(r#"{"in-octets":"100","errors":{"crc":1},"list":[1,2],"flag":[null]}"#),
                    ..Default::default()
                },
                Update {
                    path: Some(path::parse("/subinterfaces/subinterface[index=0]/state/name").unwrap()),
                    val: Some(TypedValue {
                        value: Some(typed_value::Value::StringVal("x".into())),
                    }),
                    ..Default::default()
                },
            ],
            delete: vec![path::parse("state/description").unwrap()],
            ..Default::default()
        };
        let meta: Meta = [
            ("source", "r1"),
            ("subscription-name", "sub1"),
            ("format", "event"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let evs = notification_events("sub1", &n, &meta).unwrap();
        assert_eq!(evs.len(), 3);
        let p = "openconfig:/interfaces/interface/state/counters";
        assert_eq!(evs[0].values[&format!("{p}/in-octets")], Value::from("100"));
        assert_eq!(evs[0].values[&format!("{p}/errors/crc")], Value::from(1));
        assert_eq!(evs[0].values[&format!("{p}/list.1")], Value::from(2));
        assert_eq!(evs[0].values[&format!("{p}/flag")], Value::from(true));
        assert_eq!(evs[0].tags["interface_name"], "Gi1");
        assert_eq!(evs[0].tags["target"], "r1");
        assert_eq!(evs[0].tags["source"], "r1");
        assert_eq!(evs[0].tags["subscription-name"], "sub1");
        assert!(!evs[0].tags.contains_key("format"));
        assert_eq!(evs[1].tags["subinterface_index"], "0");
        assert_eq!(
            evs[2].deletes,
            vec!["openconfig:/interfaces/interface/state/description"]
        );
        let mut b = Vec::new();
        crate::gojson::write(&mut b, &evs[1], None, false).unwrap();
        assert_eq!(
            String::from_utf8(b).unwrap(),
            r#"{"name":"sub1","timestamp":42,"tags":{"interface_name":"Gi1","source":"r1","subinterface_index":"0","subscription-name":"sub1","target":"r1"},"values":{"openconfig:/interfaces/interface/subinterfaces/subinterface/state/name":"x"}}"#
        );
    }

    #[test]
    fn conflicting_tags_and_meta() {
        let n = Notification {
            prefix: Some(path::parse("/a[name=x]").unwrap()),
            update: vec![Update {
                path: Some(path::parse("/a[name=y]/b").unwrap()),
                val: Some(TypedValue {
                    value: Some(typed_value::Value::IntVal(-1)),
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let meta: Meta = [("a_name".to_string(), "m".to_string())].into_iter().collect();
        let evs = notification_events("s", &n, &meta).unwrap();
        assert_eq!(evs[0].tags["a_name"], "x");
        assert_eq!(evs[0].tags["/a/a/b_a_name"], "y");
        assert_eq!(evs[0].tags["meta_a_name"], "m");
        assert_eq!(evs[0].values["/a/a/b"], Value::from(-1));
    }

    #[test]
    #[allow(deprecated)]
    fn scalar_values() {
        use typed_value::Value as V;
        let tv = |v| Some(TypedValue { value: Some(v) });
        let f = |v| flat_values("/p", tv(v).as_ref()).unwrap()["/p"].clone();
        assert_eq!(
            f(V::DecimalVal(crate::proto::gnmi::Decimal64 {
                digits: 1234,
                precision: 2
            })),
            Value::from(12.34)
        );
        assert_eq!(f(V::BytesVal(b"hi".to_vec())), Value::from("aGk="));
        assert_eq!(f(V::FloatVal(0.1)), f64_value(0.1));
        assert_eq!(f(V::JsonIetfVal(b"\"up\"".to_vec())), Value::from("up"));
        let leaflist = crate::proto::gnmi::ScalarArray {
            element: vec![TypedValue {
                value: Some(V::StringVal("a".into())),
            }],
        };
        assert_eq!(f(V::LeaflistVal(leaflist.clone())), serde_json::json!(["a"]));
        assert_eq!(
            typed_value(tv(V::LeaflistVal(leaflist)).as_ref()).unwrap(),
            serde_json::json!({"element": [{"Value": {"StringVal": "a"}}]})
        );
        assert!(flat_values("/p", None).unwrap().is_empty());
    }
}
