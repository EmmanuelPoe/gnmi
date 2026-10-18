//! gnmic-compatible configuration file (the subset needed for dial-out collection).

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer};

use crate::output::file::FileConfig;
use crate::output::postgres::PostgresConfig;

const DEFAULT_TUNNEL_ADDRESS: &str = ":57400";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_RETRY: Duration = Duration::from_secs(10);
const MIN_RETRY: Duration = Duration::from_millis(100);
const DEFAULT_MAX_MSG_SIZE: usize = 512 * 1024 * 1024;
pub const DEFAULT_OUTPUT_NAME: &str = "default-stdout";

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
pub struct Config {
    // Target defaults (gnmic global flags).
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
    pub insecure: bool,
    pub skip_verify: bool,
    pub tls_ca: Option<String>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub tls_server_name: Option<String>,
    pub encoding: Option<String>,
    #[serde(deserialize_with = "duration_opt")]
    pub timeout: Option<Duration>,
    #[serde(deserialize_with = "duration_opt")]
    pub retry: Option<Duration>,
    pub gzip: bool,
    pub metadata: BTreeMap<String, String>,
    pub max_msg_size: Option<usize>,
    pub calculate_latency: bool,

    // Logging.
    pub log: bool,
    pub log_file: Option<String>,
    pub log_format: Option<String>,
    pub debug: bool,

    // Accepted for drop-in compatibility with gnmic configs; always on here.
    pub use_tunnel_server: bool,

    pub tunnel_server: TunnelServerConfig,
    pub subscriptions: BTreeMap<String, SubscriptionConfig>,
    pub outputs: BTreeMap<String, OutputConfig>,

    /// Configuration keys that were not recognized (reported once logging is up).
    #[serde(skip)]
    pub ignored_keys: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
pub struct TunnelServerConfig {
    pub address: Option<String>,
    pub tls: Option<ServerTlsConfig>,
    pub targets: Vec<TunnelTargetMatch>,
    /// Flood protection: connection, handshake, registration and setup limits.
    pub admission: crate::admission::AdmissionConfig,
    pub debug: bool,
    // Only meaningful for gnmic unary RPCs; accepted and ignored.
    #[serde(deserialize_with = "duration_opt")]
    pub target_wait_time: Option<Duration>,
    pub enable_metrics: bool,
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
pub struct ServerTlsConfig {
    pub ca_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub client_auth: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
pub struct TunnelTargetMatch {
    /// Regex matched (unanchored, like gnmic) against the registered target type.
    #[serde(rename = "type")]
    pub target_type: String,
    /// Regex matched (unanchored, like gnmic) against the registered target ID.
    pub id: String,
    pub config: TargetConfig,
    /// `type` and `id`, compiled at load time.
    #[serde(skip)]
    patterns: Option<(regex::Regex, regex::Regex)>,
}

/// Per-target settings; unset fields inherit the global defaults.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
pub struct TargetConfig {
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
    pub insecure: Option<bool>,
    pub skip_verify: Option<bool>,
    pub tls_ca: Option<String>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub tls_server_name: Option<String>,
    pub encoding: Option<String>,
    #[serde(deserialize_with = "duration_opt")]
    pub timeout: Option<Duration>,
    #[serde(deserialize_with = "duration_opt")]
    pub retry_timer: Option<Duration>,
    pub gzip: Option<bool>,
    pub subscriptions: Vec<String>,
    pub outputs: Vec<String>,
    pub metadata: BTreeMap<String, String>,
    pub event_tags: BTreeMap<String, String>,
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
pub struct SubscriptionConfig {
    #[serde(skip)]
    pub name: String,
    pub models: Vec<String>,
    pub prefix: String,
    pub target: String,
    pub set_target: bool,
    pub paths: Vec<String>,
    pub mode: String,
    pub stream_mode: String,
    pub encoding: Option<String>,
    pub qos: Option<u32>,
    #[serde(deserialize_with = "duration_opt")]
    pub sample_interval: Option<Duration>,
    #[serde(deserialize_with = "duration_opt")]
    pub heartbeat_interval: Option<Duration>,
    pub suppress_redundant: bool,
    pub updates_only: bool,
    pub stream_subscriptions: Vec<SubscriptionConfig>,
    pub outputs: Vec<String>,
    pub depth: u32,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum OutputConfig {
    File(Box<FileConfig>),
    Postgres(Box<PostgresConfig>),
}

/// A target registered through the tunnel, with every default resolved.
#[derive(Debug, Clone)]
pub struct Target {
    pub name: String,
    pub tunnel_type: String,
    pub username: String,
    pub password: String,
    pub token: String,
    pub insecure: bool,
    pub skip_verify: bool,
    pub tls_ca: Option<String>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub tls_server_name: Option<String>,
    pub encoding: String,
    pub timeout: Duration,
    pub retry: Duration,
    pub gzip: bool,
    pub max_msg_size: usize,
    /// Subscription names; empty means all configured subscriptions.
    pub subscriptions: Vec<String>,
    pub outputs: Vec<String>,
    pub metadata: BTreeMap<String, String>,
    pub event_tags: BTreeMap<String, String>,
}

impl Config {
    /// Loads and validates a config file; `overrides` (command-line flags)
    /// are applied before validation.
    pub fn load(path: &Path, overrides: impl FnOnce(&mut Config)) -> Result<Config> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        Self::parse_with(&text, overrides).with_context(|| format!("config file {}", path.display()))
    }

    pub fn parse(text: &str) -> Result<Config> {
        Self::parse_with(text, |_| {})
    }

    fn parse_with(text: &str, overrides: impl FnOnce(&mut Config)) -> Result<Config> {
        let mut value: serde_yaml::Value = serde_yaml::from_str(text)?;
        if value.is_null() {
            value = serde_yaml::Value::Mapping(Default::default());
        }
        expand_env(&mut value, None);
        let mut ignored = Vec::new();
        let mut cfg: Config = serde_ignored::deserialize(value, |p| ignored.push(p.to_string()))?;
        cfg.ignored_keys = ignored;
        for (name, sub) in cfg.subscriptions.iter_mut() {
            sub.name = name.clone();
        }
        if cfg.outputs.is_empty() {
            cfg.outputs.insert(
                DEFAULT_OUTPUT_NAME.to_string(),
                OutputConfig::File(Box::new(FileConfig::stdout())),
            );
        }
        for (i, rule) in cfg.tunnel_server.targets.iter_mut().enumerate() {
            let typ = regex::Regex::new(&rule.target_type)
                .with_context(|| format!("tunnel-server target rule {i}: invalid type regex"))?;
            let id = regex::Regex::new(&rule.id)
                .with_context(|| format!("tunnel-server target rule {i}: invalid id regex"))?;
            rule.patterns = Some((typ, id));
        }
        overrides(&mut cfg);
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn tunnel_address(&self) -> &str {
        self.tunnel_server
            .address
            .as_deref()
            .filter(|a| !a.is_empty())
            .unwrap_or(DEFAULT_TUNNEL_ADDRESS)
    }

    fn validate(&self) -> Result<()> {
        if self.subscriptions.is_empty() {
            bail!("no subscriptions configured");
        }
        self.tunnel_server.admission.validate()?;
        for sub in self.subscriptions.values() {
            crate::subscribe::build_request(sub, "validation", "json")?;
            self.check_outputs(&sub.outputs, &format!("subscription {:?}", sub.name))?;
        }
        if let Some(enc) = &self.encoding {
            crate::subscribe::parse_encoding(enc)?;
        }
        for (i, rule) in self.tunnel_server.targets.iter().enumerate() {
            for s in &rule.config.subscriptions {
                if !self.subscriptions.contains_key(s) {
                    bail!("tunnel-server target rule {i}: unknown subscription {s:?}");
                }
            }
            if let Some(enc) = &rule.config.encoding {
                crate::subscribe::parse_encoding(enc)
                    .with_context(|| format!("tunnel-server target rule {i}"))?;
            }
            self.check_outputs(&rule.config.outputs, &format!("tunnel-server target rule {i}"))?;
        }
        if let Some(tls) = &self.tunnel_server.tls {
            crate::tls::validate_client_auth(tls.client_auth.as_deref())?;
            if tls.cert_file.is_some() != tls.key_file.is_some() {
                bail!("tunnel-server tls: cert-file and key-file must be set together");
            }
            crate::tls::server_config(tls).context("tunnel-server tls")?;
        }
        // Load certificate files now rather than when a device registers.
        let defaults = TargetConfig::default();
        let rules = self.tunnel_server.targets.iter().map(|r| &r.config);
        for (i, tc) in std::iter::once(&defaults).chain(rules).enumerate() {
            let t = self.resolve_target("validation", "", tc);
            if !t.insecure {
                let owner = match i {
                    0 => "target TLS settings".to_string(),
                    i => format!("tunnel-server target rule {}", i - 1),
                };
                crate::tls::client_config(&crate::tls::ClientTls::for_target(&t)).context(owner)?;
            }
        }
        Ok(())
    }

    fn check_outputs(&self, names: &[String], owner: &str) -> Result<()> {
        for o in names {
            if !self.outputs.contains_key(o) {
                bail!("{owner}: unknown output {o:?}");
            }
        }
        Ok(())
    }

    /// Returns the resolved config for a registered tunnel target, or None when
    /// no rule matches (same semantics as gnmic's getTunnelTargetMatch).
    pub fn match_tunnel_target(&self, id: &str, target_type: &str) -> Option<Target> {
        let rules = &self.tunnel_server.targets;
        if rules.is_empty() {
            return (target_type == "GNMI_GNOI")
                .then(|| self.resolve_target(id, target_type, &TargetConfig::default()));
        }
        rules
            .iter()
            .find(|r| {
                r.patterns
                    .as_ref()
                    .is_some_and(|(t, i)| t.is_match(target_type) && i.is_match(id))
            })
            .map(|r| self.resolve_target(id, target_type, &r.config))
    }

    fn resolve_target(&self, id: &str, target_type: &str, tc: &TargetConfig) -> Target {
        let pick = |t: &Option<String>, g: &Option<String>| t.clone().or_else(|| g.clone());
        let mut metadata = self.metadata.clone();
        metadata.extend(tc.metadata.clone());
        Target {
            name: id.to_string(),
            tunnel_type: target_type.to_string(),
            username: pick(&tc.username, &self.username).unwrap_or_default(),
            password: pick(&tc.password, &self.password).unwrap_or_default(),
            token: pick(&tc.token, &self.token).unwrap_or_default(),
            insecure: tc.insecure.unwrap_or(self.insecure),
            skip_verify: tc.skip_verify.unwrap_or(self.skip_verify),
            tls_ca: pick(&tc.tls_ca, &self.tls_ca).filter(|s| !s.is_empty()),
            tls_cert: pick(&tc.tls_cert, &self.tls_cert).filter(|s| !s.is_empty()),
            tls_key: pick(&tc.tls_key, &self.tls_key).filter(|s| !s.is_empty()),
            tls_server_name: pick(&tc.tls_server_name, &self.tls_server_name).filter(|s| !s.is_empty()),
            encoding: pick(&tc.encoding, &self.encoding).unwrap_or_else(|| "json".into()),
            timeout: tc.timeout.or(self.timeout).unwrap_or(DEFAULT_TIMEOUT),
            retry: tc
                .retry_timer
                .or(self.retry)
                .unwrap_or(DEFAULT_RETRY)
                .max(MIN_RETRY),
            gzip: tc.gzip.unwrap_or(self.gzip),
            max_msg_size: self.max_msg_size.unwrap_or(DEFAULT_MAX_MSG_SIZE),
            subscriptions: tc.subscriptions.clone(),
            outputs: tc.outputs.clone(),
            metadata,
            event_tags: tc.event_tags.clone(),
        }
    }
}

/// Expands $VAR / ${VAR} in string values, like gnmic's os.ExpandEnv calls.
/// Passwords are only expanded when they start with '$' (gnmic issue #496), and
/// tunnel match patterns (`id`, `type`) are left untouched since they are regexes.
fn expand_env(v: &mut serde_yaml::Value, key: Option<&str>) {
    use serde_yaml::Value;
    match v {
        Value::String(s) => match key {
            Some("id" | "type") => {}
            Some("password") if !s.starts_with('$') => {}
            _ => *s = expand_vars(s),
        },
        Value::Sequence(seq) => seq.iter_mut().for_each(|v| expand_env(v, None)),
        Value::Mapping(map) => {
            for (k, v) in map.iter_mut() {
                expand_env(v, k.as_str());
            }
        }
        _ => {}
    }
}

fn expand_vars(s: &str) -> String {
    if !s.contains('$') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('$') {
        out.push_str(&rest[..i]);
        let after = &rest[i + 1..];
        if let Some(braced) = after.strip_prefix('{') {
            if let Some(end) = braced.find('}') {
                out.push_str(&std::env::var(&braced[..end]).unwrap_or_default());
                rest = &braced[end + 1..];
                continue;
            }
        }
        let n = after
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_')
            .count();
        if n == 0 {
            out.push('$');
        } else {
            out.push_str(&std::env::var(&after[..n]).unwrap_or_default());
        }
        rest = &after[n..];
    }
    out.push_str(rest);
    out
}

/// Accepts Go duration strings ("10s", "1m30s", "500ms") or integer nanoseconds.
pub fn duration_opt<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Int(u64),
        Str(String),
    }
    match Option::<Raw>::deserialize(d)? {
        None => Ok(None),
        Some(Raw::Int(ns)) => Ok(Some(Duration::from_nanos(ns))),
        Some(Raw::Str(s)) if s.is_empty() => Ok(None),
        Some(Raw::Str(s)) => parse_go_duration(&s).map(Some).map_err(serde::de::Error::custom),
    }
}

pub fn parse_go_duration(s: &str) -> Result<Duration> {
    let orig = s;
    let s = s.trim();
    if s == "0" {
        return Ok(Duration::ZERO);
    }
    if s.starts_with('-') {
        bail!("negative duration {orig:?}");
    }
    let mut rest = s.strip_prefix('+').unwrap_or(s);
    if rest.is_empty() {
        bail!("invalid duration {orig:?}");
    }
    let mut total = 0f64;
    while !rest.is_empty() {
        let num_len = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        let num: f64 = rest[..num_len]
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid duration {orig:?}"))?;
        rest = &rest[num_len..];
        let unit_len = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let scale = match &rest[..unit_len] {
            "ns" => 1e0,
            "us" | "µs" | "μs" => 1e3,
            "ms" => 1e6,
            "s" => 1e9,
            "m" => 60e9,
            "h" => 3600e9,
            "" => bail!("missing unit in duration {orig:?}"),
            u => bail!("unknown unit {u:?} in duration {orig:?}"),
        };
        rest = &rest[unit_len..];
        total += num * scale;
    }
    Ok(Duration::from_nanos(total.round() as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_durations() {
        let d = |s| parse_go_duration(s).unwrap();
        assert_eq!(d("10s"), Duration::from_secs(10));
        assert_eq!(d("1m30s"), Duration::from_secs(90));
        assert_eq!(d("500ms"), Duration::from_millis(500));
        assert_eq!(d("1.5h"), Duration::from_secs(5400));
        assert_eq!(d("2us"), Duration::from_micros(2));
        assert_eq!(d("0"), Duration::ZERO);
        assert!(parse_go_duration("10").is_err());
        assert!(parse_go_duration("10x").is_err());
        assert!(parse_go_duration("-1s").is_err());
    }

    #[test]
    fn env_expansion() {
        // SAFETY: tests in this module do not read these variables concurrently.
        unsafe { std::env::set_var("GNMIC_RS_TEST_USER", "admin") };
        assert_eq!(expand_vars("${GNMIC_RS_TEST_USER}-x"), "admin-x");
        assert_eq!(expand_vars("$GNMIC_RS_TEST_USER/x"), "admin/x");
        assert_eq!(expand_vars("a$"), "a$");
        assert_eq!(expand_vars("$GNMIC_RS_TEST_UNSET_VAR"), "");
        let cfg = Config::parse(
            "username: $GNMIC_RS_TEST_USER\npassword: pa$$word\n\
             subscriptions:\n  s:\n    paths: [/a]\n\
             tunnel-server:\n  targets:\n    - id: ^r1$\n      type: GNMI_GNOI\n",
        )
        .unwrap();
        assert_eq!(cfg.username.as_deref(), Some("admin"));
        assert_eq!(cfg.password.as_deref(), Some("pa$$word"));
        assert_eq!(cfg.tunnel_server.targets[0].id, "^r1$");
    }

    #[test]
    fn target_matching_and_defaults() {
        let cfg = Config::parse(
            r#"
insecure: true
username: admin
password: secret
retry: 5s
subscriptions:
  s1: {paths: [/a]}
  s2: {paths: [/b]}
outputs:
  f: {type: file, filename: /tmp/x}
tunnel-server:
  targets:
    - id: ^core-
      type: GNMI_GNOI
      config:
        subscriptions: [s2]
        timeout: 3s
        insecure: false
        skip-verify: true
"#,
        )
        .unwrap();
        assert!(cfg.match_tunnel_target("edge-1", "GNMI_GNOI").is_none());
        assert!(cfg.match_tunnel_target("core-1", "SSH").is_none());
        let t = cfg.match_tunnel_target("core-1", "GNMI_GNOI").unwrap();
        assert_eq!(t.subscriptions, vec!["s2"]);
        assert_eq!(t.timeout, Duration::from_secs(3));
        assert_eq!(t.retry, Duration::from_secs(5));
        assert!(!t.insecure && t.skip_verify);
        assert_eq!((t.username.as_str(), t.password.as_str()), ("admin", "secret"));
        assert_eq!(t.encoding, "json");
    }

    #[test]
    fn default_rule_accepts_only_gnmi_gnoi() {
        let cfg = Config::parse("subscriptions:\n  s: {paths: [/a]}\n").unwrap();
        assert!(cfg.match_tunnel_target("r1", "GNMI_GNOI").is_some());
        assert!(cfg.match_tunnel_target("r1", "SSH").is_none());
        assert!(cfg.outputs.contains_key(DEFAULT_OUTPUT_NAME));
        assert_eq!(cfg.tunnel_address(), ":57400");
    }

    #[test]
    fn invalid_configs_are_rejected() {
        let bad = [
            "",
            "subscriptions:\n  s: {}\n",
            "subscriptions:\n  s: {paths: [/a], mode: bogus}\n",
            "subscriptions:\n  s: {paths: [/a], outputs: [nope]}\n",
            "subscriptions:\n  s: {paths: [/a]}\noutputs:\n  o: {type: kafka}\n",
            "subscriptions:\n  s: {paths: ['/a[b']}\n",
            "subscriptions:\n  s: {paths: [/a], sample-interval: 10q}\n",
            "subscriptions:\n  s: {paths: [/a]}\ntunnel-server:\n  targets: [{id: '(', type: x}]\n",
            "subscriptions:\n  s: {paths: [/a]}\ntls-ca: /nonexistent/ca.pem\n",
            "subscriptions:\n  s: {paths: [/a]}\ninsecure: true\ntunnel-server:\n  targets: [{id: x, type: y, config: {insecure: false, tls-ca: /nonexistent}}]\n",
            "subscriptions:\n  s: {paths: [/a]}\ntunnel-server:\n  tls: {cert-file: /nonexistent, key-file: /nonexistent}\n",
        ];
        for b in bad {
            assert!(Config::parse(b).is_err(), "accepted: {b}");
        }
    }
}
