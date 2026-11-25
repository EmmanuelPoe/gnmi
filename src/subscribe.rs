// Portions derived from gnmic (https://github.com/openconfig/gnmic),
// Copyright Nokia and the gNMIc contributors, Apache-2.0.
// Translated from Go to Rust and modified for gnmic-rs. See NOTICE.

//! Builds gNMI SubscribeRequests from subscription configs (gnmic's
//! CreateSubscribeRequest / SubscriptionOpts semantics).

use anyhow::{Context, Result, bail};

use crate::config::SubscriptionConfig;
use crate::path;
use crate::proto::gnmi::{
    Encoding, ModelData, QosMarking, SubscribeRequest, Subscription, SubscriptionList, SubscriptionMode,
    subscribe_request, subscription_list,
};
use crate::proto::gnmi_ext::{Depth, Extension, extension};

fn normalize(s: &str) -> String {
    s.to_ascii_uppercase().replace('-', "_")
}

/// Accepts gNMI encoding names (case insensitive, '-' or '_') or a raw integer.
pub fn parse_encoding(s: &str) -> Result<i32> {
    if let Some(e) = Encoding::from_str_name(&normalize(s)) {
        return Ok(e as i32);
    }
    s.parse::<i32>()
        .with_context(|| format!("unknown encoding type {s:?}"))
}

pub fn list_mode(sc: &SubscriptionConfig) -> Result<subscription_list::Mode> {
    match normalize(&sc.mode).as_str() {
        "" | "STREAM" => Ok(subscription_list::Mode::Stream),
        "ONCE" => Ok(subscription_list::Mode::Once),
        "POLL" => Ok(subscription_list::Mode::Poll),
        _ => bail!(
            "subscription {:?}: unknown subscription mode {:?}",
            sc.name,
            sc.mode
        ),
    }
}

fn stream_mode(name: &str, m: &str) -> Result<SubscriptionMode> {
    match normalize(m).as_str() {
        "" | "TARGET_DEFINED" => Ok(SubscriptionMode::TargetDefined),
        "ON_CHANGE" => Ok(SubscriptionMode::OnChange),
        "SAMPLE" => Ok(SubscriptionMode::Sample),
        _ => bail!("subscription {name:?}: unknown stream-mode type {m:?}"),
    }
}

fn nanos(d: Option<std::time::Duration>) -> u64 {
    d.map_or(0, |d| d.as_nanos() as u64)
}

fn validate(sc: &SubscriptionConfig) -> Result<()> {
    let name = &sc.name;
    if sc.prefix.is_empty() && sc.paths.is_empty() && sc.stream_subscriptions.is_empty() {
        bail!("missing path(s) in subscription {name:?}");
    }
    if !sc.paths.is_empty() && !sc.stream_subscriptions.is_empty() {
        bail!("subscription {name:?}: cannot set 'paths' and 'stream-subscriptions' at the same time");
    }
    let mode = list_mode(sc)?;
    if mode == subscription_list::Mode::Poll {
        bail!("subscription {name:?}: mode POLL is interactive and not supported by a dial-out collector");
    }
    if mode != subscription_list::Mode::Stream && !sc.stream_subscriptions.is_empty() {
        bail!("subscription {name:?}: cannot set 'stream-subscriptions' and 'mode'");
    }
    if let Some(enc) = &sc.encoding
        && !enc.is_empty()
    {
        parse_encoding(enc).with_context(|| format!("subscription {name:?}"))?;
    }
    if mode != subscription_list::Mode::Stream {
        return Ok(());
    }
    stream_mode(name, &sc.stream_mode)?;
    for (i, s) in sc.stream_subscriptions.iter().enumerate() {
        let forbidden = [
            ("mode", !s.mode.is_empty()),
            ("prefix", !s.prefix.is_empty()),
            ("target", !s.target.is_empty()),
            ("set-target", s.set_target),
            ("encoding", s.encoding.is_some()),
            ("models", !s.models.is_empty()),
            ("updates-only", s.updates_only),
            ("stream-subscriptions", !s.stream_subscriptions.is_empty()),
            ("qos", s.qos.is_some()),
        ];
        if let Some((attr, _)) = forbidden.iter().find(|(_, set)| *set) {
            bail!("subscription {name}/{i}: '{attr}' attribute cannot be set");
        }
        if s.paths.is_empty() {
            bail!("missing path(s) in subscription {name}/{i}");
        }
        stream_mode(&format!("{name}/{i}"), &s.stream_mode)?;
    }
    Ok(())
}

/// Per-path subscription settings for a STREAM list, as gnmic sets them for
/// each stream mode. gnmic always sends the heartbeat of a top-level
/// TARGET_DEFINED subscription, but not of a nested stream-subscription.
fn stream_subscription(sc: &SubscriptionConfig, name: &str, p: &str, nested: bool) -> Result<Subscription> {
    let mode = stream_mode(name, &sc.stream_mode)?;
    let mut sub = Subscription {
        path: Some(path::parse(p).with_context(|| format!("subscription {name:?}"))?),
        mode: mode as i32,
        ..Default::default()
    };
    match mode {
        SubscriptionMode::OnChange => sub.heartbeat_interval = nanos(sc.heartbeat_interval),
        SubscriptionMode::Sample | SubscriptionMode::TargetDefined => {
            sub.sample_interval = nanos(sc.sample_interval);
            sub.suppress_redundant = sc.suppress_redundant;
            if sc.suppress_redundant || (!nested && mode == SubscriptionMode::TargetDefined) {
                sub.heartbeat_interval = nanos(sc.heartbeat_interval);
            }
        }
    }
    Ok(sub)
}

pub fn build_request(
    sc: &SubscriptionConfig,
    target_name: &str,
    default_encoding: &str,
) -> Result<SubscribeRequest> {
    validate(sc)?;
    let mode = list_mode(sc)?;
    let encoding = match sc.encoding.as_deref() {
        Some(e) if !e.is_empty() => e,
        Some(_) => "json",
        None => default_encoding,
    };
    let mut prefix = if sc.prefix.is_empty() {
        None
    } else {
        Some(path::parse(&sc.prefix).with_context(|| format!("subscription {:?}", sc.name))?)
    };
    let target = if !sc.target.is_empty() {
        sc.target.as_str()
    } else if sc.set_target {
        target_name
    } else {
        ""
    };
    if !target.is_empty() {
        prefix.get_or_insert_with(Default::default).target = target.to_string();
    }

    let mut subscription = Vec::new();
    for ssc in &sc.stream_subscriptions {
        for p in &ssc.paths {
            subscription.push(stream_subscription(ssc, &sc.name, p, true)?);
        }
    }
    for p in &sc.paths {
        if mode == subscription_list::Mode::Stream {
            subscription.push(stream_subscription(sc, &sc.name, p, false)?);
        } else {
            subscription.push(Subscription {
                path: Some(path::parse(p).with_context(|| format!("subscription {:?}", sc.name))?),
                ..Default::default()
            });
        }
    }

    let list = SubscriptionList {
        prefix,
        subscription,
        qos: sc.qos.map(|marking| QosMarking { marking }),
        mode: mode as i32,
        use_models: sc
            .models
            .iter()
            .map(|m| ModelData {
                name: m.clone(),
                ..Default::default()
            })
            .collect(),
        encoding: parse_encoding(encoding)?,
        updates_only: sc.updates_only,
        ..Default::default()
    };
    let extension = if sc.depth > 0 {
        vec![Extension {
            ext: Some(extension::Ext::Depth(Depth { level: sc.depth })),
        }]
    } else {
        vec![]
    };
    Ok(SubscribeRequest {
        request: Some(subscribe_request::Request::Subscribe(list)),
        extension,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn sub(yaml: &str) -> SubscriptionConfig {
        let mut s: SubscriptionConfig = serde_yaml::from_str(yaml).unwrap();
        s.name = "sub1".into();
        s
    }

    fn list(r: &SubscribeRequest) -> &SubscriptionList {
        match r.request.as_ref().unwrap() {
            subscribe_request::Request::Subscribe(l) => l,
            _ => panic!("not a subscribe request"),
        }
    }

    #[test]
    fn sample_subscription() {
        let r = build_request(
            &sub("{paths: [/a/b, /c], stream-mode: sample, sample-interval: 10s, encoding: json_ietf, qos: 20, set-target: true}"),
            "router1",
            "json",
        )
        .unwrap();
        let l = list(&r);
        assert_eq!(l.mode, subscription_list::Mode::Stream as i32);
        assert_eq!(l.encoding, Encoding::JsonIetf as i32);
        assert_eq!(l.qos.unwrap().marking, 20);
        assert_eq!(l.prefix.as_ref().unwrap().target, "router1");
        assert_eq!(l.subscription.len(), 2);
        let s = &l.subscription[0];
        assert_eq!(s.mode, SubscriptionMode::Sample as i32);
        assert_eq!(s.sample_interval, Duration::from_secs(10).as_nanos() as u64);
        assert_eq!(path::to_xpath(s.path.as_ref()), "a/b");
    }

    #[test]
    fn defaults_and_on_change() {
        let r = build_request(
            &sub("{paths: [/a], stream-mode: on-change, heartbeat-interval: 1m}"),
            "t",
            "proto",
        )
        .unwrap();
        let l = list(&r);
        assert_eq!(l.encoding, Encoding::Proto as i32);
        assert!(l.prefix.is_none());
        let s = &l.subscription[0];
        assert_eq!(s.mode, SubscriptionMode::OnChange as i32);
        assert_eq!(s.heartbeat_interval, 60_000_000_000);
        assert_eq!(s.sample_interval, 0);

        let r = build_request(&sub("{paths: [/a]}"), "t", "json").unwrap();
        assert_eq!(
            list(&r).subscription[0].mode,
            SubscriptionMode::TargetDefined as i32
        );
    }

    #[test]
    fn stream_subscriptions_and_once() {
        let r = build_request(
            &sub("{prefix: 'openconfig:/interfaces', depth: 2, stream-subscriptions: [{paths: [/x], stream-mode: sample, sample-interval: 5s}, {paths: [/y], stream-mode: on-change}]}"),
            "t",
            "json",
        )
        .unwrap();
        let l = list(&r);
        assert_eq!(l.prefix.as_ref().unwrap().origin, "openconfig");
        assert_eq!(l.subscription.len(), 2);
        assert_eq!(l.subscription[1].mode, SubscriptionMode::OnChange as i32);
        assert_eq!(r.extension.len(), 1);

        let r = build_request(&sub("{paths: [/a], mode: once}"), "t", "json").unwrap();
        assert_eq!(list(&r).mode, subscription_list::Mode::Once as i32);
    }

    #[test]
    fn invalid() {
        for y in [
            "{paths: [/a], mode: poll}",
            "{paths: [/a], encoding: bogus}",
            "{paths: [/a], stream-mode: fast}",
            "{paths: [/a], stream-subscriptions: [{paths: [/b]}]}",
            "{stream-subscriptions: [{paths: [/b], qos: 1}]}",
            "{mode: once, stream-subscriptions: [{paths: [/b]}]}",
        ] {
            assert!(build_request(&sub(y), "t", "json").is_err(), "{y}");
        }
        assert_eq!(parse_encoding("17").unwrap(), 17);
        assert_eq!(parse_encoding("JSON-IETF").unwrap(), 4);
    }
}
