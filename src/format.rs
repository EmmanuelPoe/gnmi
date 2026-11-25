// Portions derived from gnmic (https://github.com/openconfig/gnmic),
// Copyright Nokia and the gNMIc contributors, Apache-2.0.
// Translated from Go to Rust and modified for gnmic-rs. See NOTICE.

//! Message formatting shared by outputs (gnmic's formatters.MarshalOptions).

use std::collections::BTreeMap;
use std::sync::OnceLock;

use anyhow::{Result, bail};
use chrono::{Local, TimeZone};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde::Serialize;
use serde_json::Value;

use crate::event::{self, Event};
use crate::gojson;
use crate::output::Msg;
use crate::path;
use crate::proto::gnmi::{SubscribeResponse, subscribe_response};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Json,
    Event,
    ProtoJson,
    ProtoText,
}

impl Format {
    pub fn parse(s: &str) -> Result<Format> {
        Ok(match s {
            "" | "json" => Format::Json,
            "event" => Format::Event,
            "protojson" => Format::ProtoJson,
            "prototext" => Format::ProtoText,
            "proto" => bail!("proto format not supported in output type 'file'"),
            other => bail!("unknown format {other:?}, expected json, event, protojson or prototext"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddTarget {
    Overwrite,
    IfNotPresent,
}

impl AddTarget {
    pub fn parse(s: &str) -> Result<Option<AddTarget>> {
        Ok(match s {
            "" => None,
            "overwrite" => Some(AddTarget::Overwrite),
            "if-not-present" => Some(AddTarget::IfNotPresent),
            other => {
                bail!("invalid add-target value {other:?}: expected \"overwrite\" or \"if-not-present\"")
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct Marshaler {
    pub format: Format,
    pub indent: Option<String>,
    pub split_events: bool,
    pub override_timestamps: bool,
    pub calculate_latency: bool,
    pub add_target: Option<AddTarget>,
}

/// Unix time in nanoseconds.
pub fn now_nanos() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
}

/// Go's time.Time JSON form (RFC 3339 with trimmed nanoseconds, local zone).
pub fn go_time(ns: i64) -> String {
    let t = Local.timestamp_nanos(ns);
    let mut s = t.format("%Y-%m-%dT%H:%M:%S").to_string();
    let frac = t.timestamp_subsec_nanos();
    if frac != 0 {
        let digits = format!(".{frac:09}");
        s.push_str(digits.trim_end_matches('0'));
    }
    if t.offset().local_minus_utc() == 0 {
        s.push('Z');
    } else {
        s.push_str(&t.format("%:z").to_string());
    }
    s
}

/// gnmic's utils.GetHost: strip a ":port" suffix if present.
fn host(addr: &str) -> &str {
    if let Some(rest) = addr.strip_prefix('[') {
        if let Some((h, port)) = rest.split_once("]:")
            && !port.is_empty()
            && port.bytes().all(|b| b.is_ascii_digit())
        {
            return h;
        }
        return addr;
    }
    match addr.rsplit_once(':') {
        Some((h, _)) if !h.contains(':') => h,
        _ => addr,
    }
}

#[derive(Serialize)]
struct SyncResponseMsg {
    #[serde(rename = "sync-response")]
    sync_response: bool,
}

#[derive(Serialize)]
struct UpdateMsg {
    #[serde(rename = "Path")]
    path: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    values: BTreeMap<String, Value>,
}

/// gnmic's notificationRspMsg; field order is the output key order.
#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct NotificationMsg<'a> {
    #[serde(skip_serializing_if = "str::is_empty")]
    source: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    system_name: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    subscription_name: &'a str,
    #[serde(skip_serializing_if = "is_zero")]
    timestamp: i64,
    time: String,
    #[serde(skip_serializing_if = "is_zero")]
    recv_timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    recv_time: Option<String>,
    #[serde(skip_serializing_if = "is_zero")]
    latency_nano: i64,
    #[serde(skip_serializing_if = "is_zero")]
    latency_milli: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    prefix: String,
    #[serde(skip_serializing_if = "str::is_empty")]
    target: &'a str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    updates: Vec<UpdateMsg>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    deletes: Vec<String>,
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

fn descriptor_pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| {
        DescriptorPool::decode(crate::proto::DESCRIPTOR_SET).expect("embedded descriptor set is valid")
    })
}

impl Marshaler {
    fn meta_str<'a>(meta: &'a crate::output::Meta, k: &str) -> &'a str {
        meta.get(k).map_or("", String::as_str)
    }

    /// Applies add-target and override-timestamps; returns the response to format.
    fn prepare<'a>(&self, msg: &'a Msg) -> std::borrow::Cow<'a, SubscribeResponse> {
        use std::borrow::Cow;
        let mut rsp = Cow::Borrowed(&msg.rsp);
        let Some(subscribe_response::Response::Update(n)) = &msg.rsp.response else {
            return rsp;
        };
        if let Some(mode) = self.add_target {
            let present = n.prefix.as_ref().is_some_and(|p| !p.target.is_empty());
            if mode == AddTarget::Overwrite || !present {
                let target = match msg.meta.get("subscription-target") {
                    Some(t) if !t.is_empty() => t.clone(),
                    _ => host(Self::meta_str(&msg.meta, "source")).to_string(),
                };
                if let Some(subscribe_response::Response::Update(n)) = &mut rsp.to_mut().response {
                    n.prefix.get_or_insert_with(Default::default).target = target;
                }
            }
        }
        if self.override_timestamps
            && let Some(subscribe_response::Response::Update(n)) = &mut rsp.to_mut().response
        {
            n.timestamp = now_nanos();
        }
        rsp
    }

    pub fn events(&self, msg: &Msg) -> Result<Vec<Event>> {
        let rsp = self.prepare(msg);
        match &rsp.response {
            Some(subscribe_response::Response::Update(n)) => {
                let name = msg
                    .meta
                    .get("subscription-name")
                    .map_or("default", String::as_str);
                event::notification_events(name, n, &msg.meta)
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Appends the formatted records of `msg` to `out`, each followed by
    /// `sep` (several records only for split events). On error `out` is left
    /// unchanged.
    pub fn format(&self, msg: &Msg, out: &mut Vec<u8>, sep: &[u8]) -> Result<()> {
        let start = out.len();
        let res = self.format_records(msg, out, sep);
        if res.is_err() {
            out.truncate(start);
        }
        res
    }

    fn format_records(&self, msg: &Msg, out: &mut Vec<u8>, sep: &[u8]) -> Result<()> {
        let indent = self.indent.as_deref();
        match self.format {
            Format::Event => {
                let events = self.events(msg)?;
                if events.is_empty() {
                    return Ok(());
                }
                if self.split_events {
                    for e in &events {
                        // gnmic uses json.Marshal (HTML escaping) for split events.
                        gojson::write(out, e, indent, true)?;
                        out.extend_from_slice(sep);
                    }
                    return Ok(());
                }
                gojson::write(out, &events, indent, false)?;
            }
            Format::Json => {
                if !self.json(msg, out)? {
                    return Ok(());
                }
            }
            Format::ProtoJson | Format::ProtoText => {
                let rsp = self.prepare(msg);
                let desc = descriptor_pool()
                    .get_message_by_name("gnmi.SubscribeResponse")
                    .expect("gnmi.SubscribeResponse in descriptor set");
                let dm = DynamicMessage::decode(desc, rsp.encode_to_vec().as_slice())?;
                if self.format == Format::ProtoText {
                    let text = if indent.is_some() {
                        format!("{dm:#}")
                    } else {
                        format!("{dm}")
                    };
                    out.extend_from_slice(text.as_bytes());
                } else {
                    let opts = prost_reflect::SerializeOptions::new();
                    match indent {
                        Some(i) => {
                            let f = serde_json::ser::PrettyFormatter::with_indent(i.as_bytes());
                            dm.serialize_with_options(
                                &mut serde_json::Serializer::with_formatter(&mut *out, f),
                                &opts,
                            )?
                        }
                        None => {
                            dm.serialize_with_options(&mut serde_json::Serializer::new(&mut *out), &opts)?
                        }
                    }
                }
            }
        }
        out.extend_from_slice(sep);
        Ok(())
    }

    /// gnmic "json" format. Returns false when there is nothing to write.
    fn json(&self, msg: &Msg, buf: &mut Vec<u8>) -> Result<bool> {
        let indent = self.indent.as_deref();
        let rsp = self.prepare(msg);
        match &rsp.response {
            Some(subscribe_response::Response::SyncResponse(b)) => {
                gojson::write(buf, &SyncResponseMsg { sync_response: *b }, indent, false)?;
            }
            Some(subscribe_response::Response::Update(n)) => {
                let mut updates = Vec::with_capacity(n.update.len());
                for u in &n.update {
                    let key = u
                        .path
                        .iter()
                        .flat_map(|p| p.elem.iter().map(|e| e.name.as_str()))
                        .collect::<Vec<_>>()
                        .join("/");
                    let mut values = BTreeMap::new();
                    values.insert(key, event::typed_value(u.val.as_ref())?);
                    updates.push(UpdateMsg {
                        path: path::to_xpath(u.path.as_ref()),
                        values,
                    });
                }
                let (recv_timestamp, recv_time, latency_nano) = if self.calculate_latency {
                    let r = now_nanos();
                    (r, Some(go_time(r)), r - n.timestamp)
                } else {
                    (0, None, 0)
                };
                let m = NotificationMsg {
                    source: Self::meta_str(&msg.meta, "source"),
                    system_name: Self::meta_str(&msg.meta, "system-name"),
                    subscription_name: Self::meta_str(&msg.meta, "subscription-name"),
                    timestamp: n.timestamp,
                    time: go_time(n.timestamp),
                    recv_timestamp,
                    recv_time,
                    latency_nano,
                    latency_milli: latency_nano / 1_000_000,
                    prefix: path::to_xpath(n.prefix.as_ref()),
                    target: n.prefix.as_ref().map_or("", |p| p.target.as_str()),
                    updates,
                    deletes: n.delete.iter().map(|d| path::to_xpath(Some(d))).collect(),
                };
                gojson::write(buf, &m, indent, false)?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::gnmi::{Notification, TypedValue, Update, typed_value};
    use std::sync::Arc;

    fn msg() -> Msg {
        let mut prefix = path::parse("/interfaces/interface[name=Gi1]").unwrap();
        prefix.target = String::new();
        Msg {
            rsp: SubscribeResponse {
                response: Some(subscribe_response::Response::Update(Notification {
                    timestamp: 1_700_000_000_123_456_789,
                    prefix: Some(prefix),
                    update: vec![Update {
                        path: Some(path::parse("state/oper-status").unwrap()),
                        val: Some(TypedValue {
                            value: Some(typed_value::Value::JsonIetfVal(b"\"UP\"".to_vec())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                })),
                ..Default::default()
            },
            meta: Arc::new(
                [("source", "r1:57400"), ("subscription-name", "sub1")]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
        }
    }

    fn marshaler(format: Format) -> Marshaler {
        Marshaler {
            format,
            indent: None,
            split_events: false,
            override_timestamps: false,
            calculate_latency: false,
            add_target: None,
        }
    }

    fn render(m: &Marshaler, msg: &Msg) -> Vec<String> {
        let mut out = Vec::new();
        m.format(msg, &mut out, b"\0").unwrap();
        let s = String::from_utf8(out).unwrap();
        s.split_terminator('\0').map(str::to_string).collect()
    }

    #[test]
    fn json_format() {
        let out = render(&marshaler(Format::Json), &msg());
        let want = format!(
            r#"{{"source":"r1:57400","subscription-name":"sub1","timestamp":1700000000123456789,"time":"{}","prefix":"interfaces/interface[name=Gi1]","updates":[{{"Path":"state/oper-status","values":{{"state/oper-status":"UP"}}}}]}}"#,
            go_time(1_700_000_000_123_456_789)
        );
        assert_eq!(out, vec![want]);
        let sync = Msg {
            rsp: SubscribeResponse {
                response: Some(subscribe_response::Response::SyncResponse(true)),
                ..Default::default()
            },
            meta: msg().meta,
        };
        assert_eq!(
            render(&marshaler(Format::Json), &sync),
            vec![r#"{"sync-response":true}"#]
        );
        assert!(render(&marshaler(Format::Event), &sync).is_empty());
    }

    #[test]
    fn add_target_uses_host_of_source() {
        let mut m = marshaler(Format::Event);
        m.add_target = Some(AddTarget::IfNotPresent);
        m.split_events = true;
        let out = render(&m, &msg());
        assert_eq!(out.len(), 1);
        assert!(out[0].contains(r#""target":"r1""#), "{}", out[0]);
    }

    #[test]
    fn proto_formats() {
        let mut m = marshaler(Format::ProtoJson);
        let out = render(&m, &msg());
        assert!(
            out[0].starts_with(r#"{"update":{"timestamp":"1700000000123456789""#),
            "{}",
            out[0]
        );
        m.format = Format::ProtoText;
        let out = render(&m, &msg());
        assert!(
            out[0].starts_with("update{timestamp:1700000000123456789"),
            "{}",
            out[0]
        );
    }

    #[test]
    fn go_time_format() {
        let t = go_time(1_700_000_000_120_000_000);
        assert!(t.contains(".12") && !t.contains(".120"), "{t}");
        assert!(!go_time(1_700_000_000_000_000_000).contains('.'));
        assert_eq!(host("r1:57400"), "r1");
        assert_eq!(host("r1"), "r1");
        assert_eq!(host("[::1]:57400"), "::1");
        assert_eq!(host("fe80::1"), "fe80::1");
    }
}
