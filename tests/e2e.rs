//! End-to-end tests: run the real `gnmic-rs` binary against the `dialout-sim`
//! device simulator over TCP.
//!
//! Postgres tests run when GNMIC_RS_TEST_PG_URL is set, e.g.
//! `postgres://gnmic:gnmic@127.0.0.1:55432/telemetry` (see README).

use std::fs;
use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const COLLECTOR: &str = env!("CARGO_BIN_EXE_gnmic-rs");
const SIM: &str = env!("CARGO_BIN_EXE_dialout-sim");

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn read(p: &Path) -> String {
    fs::read_to_string(p).unwrap_or_default()
}

/// A child process whose combined output goes to a log file; killed on drop.
struct Proc {
    child: Child,
    log: PathBuf,
}

impl Proc {
    fn spawn(cmd: &str, args: &[&str], log: PathBuf) -> Proc {
        let out = fs::File::create(&log).unwrap();
        let child = Command::new(cmd)
            .args(args)
            .env("TZ", "UTC")
            .env_remove("RUST_LOG")
            .stdout(out.try_clone().unwrap())
            .stderr(out)
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        Proc { child, log }
    }

    fn log(&self) -> String {
        read(&self.log)
    }

    fn wait_log(&self, needle: &str, timeout: Duration) {
        let start = Instant::now();
        while !self.log().contains(needle) {
            if start.elapsed() > timeout {
                panic!(
                    "timed out waiting for {needle:?} in {}:\n{}",
                    self.log.display(),
                    self.log()
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// SIGTERM, then wait for a clean exit.
    fn terminate(mut self) -> i32 {
        Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .unwrap();
        let start = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status.code().unwrap_or(-1);
            }
            assert!(
                start.elapsed() < Duration::from_secs(15),
                "process did not exit:\n{}",
                self.log()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn kill(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Env {
    dir: tempfile::TempDir,
    port: u16,
}

impl Env {
    fn new() -> Env {
        Env {
            dir: tempfile::tempdir().unwrap(),
            port: free_port(),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn p(&self, name: &str) -> String {
        self.path(name).display().to_string()
    }

    /// Like `wait_for`, but prints every process log on timeout.
    fn wait_for(&self, what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
        let start = Instant::now();
        while !cond() {
            if start.elapsed() > timeout {
                for e in fs::read_dir(self.dir.path()).unwrap() {
                    let p = e.unwrap().path();
                    if p.extension().is_some_and(|x| x == "log") && !p.ends_with("out.log") {
                        eprintln!("==== {}\n{}", p.display(), read(&p));
                    }
                }
                panic!("timed out waiting for {what}");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Starts the collector with `body` appended to a tunnel-server address.
    fn collector(&self, tunnel_extra: &str, body: &str) -> Proc {
        let cfg = format!(
            "tunnel-server:\n  address: 127.0.0.1:{}\n{tunnel_extra}{body}",
            self.port
        );
        fs::write(self.path("cfg.yaml"), cfg).unwrap();
        let p = Proc::spawn(
            COLLECTOR,
            &["--config", &self.p("cfg.yaml")],
            self.path("collector.log"),
        );
        p.wait_log("tunnel server listening", Duration::from_secs(10));
        p
    }

    fn sim(&self, name: &str, args: &[&str]) -> Proc {
        let server = format!("127.0.0.1:{}", self.port);
        let mut all = vec!["--server", server.as_str()];
        all.extend_from_slice(args);
        Proc::spawn(SIM, &all, self.path(&format!("{name}.log")))
    }
}

const SUB: &str = "subscriptions:\n  counters:\n    paths: [/interfaces]\n    stream-mode: sample\n    sample-interval: 1s\n    encoding: json_ietf\n";

fn file_output(name: &str, path: &str, extra: &str) -> String {
    format!("  {name}:\n    type: file\n    filename: {path}\n{extra}")
}

#[test]
fn streams_to_json_and_event_files() {
    let env = Env::new();
    let outputs = format!(
        "outputs:\n{}{}",
        file_output("json", &env.p("out.json"), ""),
        file_output("event", &env.p("event.json"), "    format: event\n")
    );
    let c = env.collector(
        "",
        &format!("insecure: true\nusername: admin\npassword: secret\n{SUB}{outputs}"),
    );
    let _sim = env.sim(
        "sim",
        &[
            "--targets",
            "2",
            "--interfaces",
            "2",
            "--interval-ms",
            "200",
            "--username",
            "admin",
            "--password",
            "secret",
        ],
    );
    env.wait_for("data from both targets", Duration::from_secs(15), || {
        let s = read(&env.path("out.json"));
        s.lines().filter(|l| l.contains("\"updates\"")).count() >= 6
            && s.contains("sim-router-1")
            && s.contains("sim-router-2")
    });
    assert_eq!(c.terminate(), 0);

    let json = read(&env.path("out.json"));
    let mut syncs = 0;
    for line in json.lines() {
        let v: serde_json::Value = serde_json::from_str(line).expect("one JSON document per line");
        if v.get("sync-response").is_some() {
            syncs += 1;
            continue;
        }
        assert_eq!(v["subscription-name"], "counters");
        assert_eq!(v["prefix"], "interfaces");
        assert_eq!(v["updates"].as_array().unwrap().len(), 2);
        assert!(v["time"].as_str().unwrap().ends_with('Z'));
    }
    assert_eq!(syncs, 2, "one sync-response per target");

    let events = read(&env.path("event.json"));
    let first: serde_json::Value = serde_json::from_str(events.lines().next().unwrap()).unwrap();
    let e = &first[0];
    assert_eq!(e["name"], "counters");
    assert_eq!(e["tags"]["interface_name"], "GigabitEthernet1/0/1");
    assert!(e["values"]["/interfaces/interface/state/counters/in-octets"].is_string());
}

#[test]
fn wrong_credentials_are_retried() {
    let env = Env::new();
    let c = env.collector(
        "",
        &format!(
            "insecure: true\nusername: admin\npassword: wrong\nretry: 500ms\n{SUB}outputs:\n{}",
            file_output("f", &env.p("out.json"), "")
        ),
    );
    let _sim = env.sim("sim", &["--username", "admin", "--password", "secret"]);
    c.wait_log("Unauthenticated: invalid credentials", Duration::from_secs(10));
    // The subscription keeps being retried.
    env.wait_for("several attempts", Duration::from_secs(10), || {
        c.log().matches("failed to create a subscribe client").count() >= 3
    });
    assert!(read(&env.path("out.json")).is_empty());
    assert_eq!(c.terminate(), 0);
}

#[test]
fn target_rules_select_targets_and_subscriptions() {
    let env = Env::new();
    let body = format!(
        "insecure: true\nsubscriptions:\n  a:\n    paths: [/a]\n    sample-interval: 1s\n  b:\n    paths: [/b]\n    sample-interval: 1s\noutputs:\n{}",
        file_output("f", &env.p("out.json"), "")
    );
    let rules = "  targets:\n    - id: ^core-\n      type: GNMI_GNOI\n      config:\n        subscriptions: [b]\n        event-tags:\n          site: lab\n";
    let c = env.collector(rules, &body);
    let _edge = env.sim("edge", &["--id-prefix", "edge-1"]);
    let _core = env.sim("core", &["--id-prefix", "core-1", "--interval-ms", "200"]);
    c.wait_log("tunnel target ignored", Duration::from_secs(10));
    env.wait_for("core data", Duration::from_secs(10), || {
        read(&env.path("out.json")).contains("\"updates\"")
    });
    assert_eq!(c.terminate(), 0);
    let out = read(&env.path("out.json"));
    assert!(!out.contains("edge-1"));
    assert!(out.contains(r#""subscription-name":"b""#));
    assert!(!out.contains(r#""subscription-name":"a""#));
}

#[test]
fn deregistration_and_device_loss() {
    let env = Env::new();
    let c = env.collector(
        "",
        &format!(
            "insecure: true\n{SUB}outputs:\n{}",
            file_output("f", &env.p("out.json"), "")
        ),
    );
    // Graceful deregistration.
    let sim = env.sim("sim1", &["--duration", "2", "--interval-ms", "200"]);
    c.wait_log("tunnel server target deregister request", Duration::from_secs(15));
    c.wait_log("target stopped", Duration::from_secs(5));
    drop(sim);

    // The same device comes back, then disappears without deregistering.
    let sim = env.sim("sim2", &["--interval-ms", "200"]);
    env.wait_for("second registration", Duration::from_secs(10), || {
        c.log().matches("target gNMI client created").count() >= 2
    });
    sim.kill();
    c.wait_log("deregistered (client disconnected)", Duration::from_secs(10));
    assert_eq!(c.terminate(), 0);
}

#[test]
fn once_subscription_is_not_repeated() {
    let env = Env::new();
    let body = format!(
        "insecure: true\nretry: 300ms\nsubscriptions:\n  snap:\n    paths: [/interfaces]\n    mode: once\noutputs:\n{}",
        file_output("f", &env.p("out.json"), "")
    );
    let c = env.collector("", &body);
    let _sim = env.sim("sim", &[]);
    c.wait_log("subscription closed stream (EOF)", Duration::from_secs(10));
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(c.log().matches("sending gNMI SubscribeRequest").count(), 1);
    let out = read(&env.path("out.json"));
    assert_eq!(out.lines().filter(|l| l.contains("\"updates\"")).count(), 1);
    assert!(out.contains("sync-response"));
    assert_eq!(c.terminate(), 0);
}

#[test]
fn rotation_with_gzip_compression() {
    let env = Env::new();
    let extra = "    rotation:\n      max-size: 1\n      max-backups: 2\n      compress: true\n";
    let c = env.collector(
        "",
        &format!(
            "insecure: true\n{SUB}outputs:\n{}",
            file_output("f", &env.p("out.json"), extra)
        ),
    );
    let _sim = env.sim(
        "sim",
        &["--interfaces", "20", "--burst", "50", "--interval-ms", "20"],
    );
    let gz_count = || {
        fs::read_dir(env.dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".json.gz")
            })
            .count()
    };
    env.wait_for("two compressed backups", Duration::from_secs(30), || {
        gz_count() >= 2
    });
    assert_eq!(c.terminate(), 0);
    std::thread::sleep(Duration::from_millis(300));
    assert!(gz_count() <= 2, "max-backups is enforced");
    for entry in fs::read_dir(env.dir.path()).unwrap() {
        let p = entry.unwrap().path();
        if p.to_string_lossy().ends_with(".json.gz") {
            let mut s = String::new();
            flate2::read::GzDecoder::new(fs::File::open(&p).unwrap())
                .read_to_string(&mut s)
                .unwrap();
            assert!(s.len() as u64 <= 1024 * 1024);
            for line in s.lines() {
                serde_json::from_str::<serde_json::Value>(line).expect("records are never split");
            }
        }
    }
}

struct Certs {
    ca: String,
    server_cert: String,
    server_key: String,
    client_cert: String,
    client_key: String,
}

fn make_certs(env: &Env) -> Certs {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, ca_key);
    let leaf = |names: &[&str]| {
        let key = KeyPair::generate().unwrap();
        let names = names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        let cert = CertificateParams::new(names)
            .unwrap()
            .signed_by(&key, &issuer)
            .unwrap();
        (cert.pem(), key.serialize_pem())
    };
    let (sc, sk) = leaf(&["localhost", "127.0.0.1"]);
    let (cc, ck) = leaf(&["sim-router"]);
    let write = |n: &str, s: &str| {
        fs::write(env.path(n), s).unwrap();
        env.p(n)
    };
    Certs {
        ca: write("ca.pem", &ca.pem()),
        server_cert: write("server.pem", &sc),
        server_key: write("server.key", &sk),
        client_cert: write("client.pem", &cc),
        client_key: write("client.key", &ck),
    }
}

#[test]
fn tunnel_mutual_tls() {
    let env = Env::new();
    let certs = make_certs(&env);
    let tls = format!(
        "  tls:\n    ca-file: {}\n    cert-file: {}\n    key-file: {}\n",
        certs.ca, certs.server_cert, certs.server_key
    );
    let c = env.collector(
        &tls,
        &format!(
            "insecure: true\n{SUB}outputs:\n{}",
            file_output("f", &env.p("out.json"), "")
        ),
    );
    // No client certificate: rejected (client-auth defaults to require-verify with a CA).
    let anon = env.sim("anon", &["--tls", "--tls-ca", &certs.ca, "--id-prefix", "anon"]);
    c.wait_log("peer sent no certificates", Duration::from_secs(10));
    anon.kill();
    // Valid client certificate, server verified against the CA.
    let _sim = env.sim(
        "sim",
        &[
            "--tls",
            "--tls-ca",
            &certs.ca,
            "--tls-cert",
            &certs.client_cert,
            "--tls-key",
            &certs.client_key,
            "--interval-ms",
            "200",
        ],
    );
    env.wait_for("data over mTLS", Duration::from_secs(10), || {
        read(&env.path("out.json")).contains("\"updates\"")
    });
    assert!(!read(&env.path("out.json")).contains("anon"));
    assert_eq!(c.terminate(), 0);
}

#[test]
fn tunnel_tls_self_signed() {
    let env = Env::new();
    let c = env.collector(
        "  tls: {}\n",
        &format!(
            "insecure: true\n{SUB}outputs:\n{}",
            file_output("f", &env.p("out.json"), "")
        ),
    );
    let _sim = env.sim("sim", &["--tls", "--interval-ms", "200"]);
    env.wait_for("data over TLS", Duration::from_secs(10), || {
        read(&env.path("out.json")).contains("\"updates\"")
    });
    assert_eq!(c.terminate(), 0);
}

#[test]
fn gnmi_tls_inside_tunnel() {
    let env = Env::new();
    // Device certificate is self-signed: verification fails unless skip-verify.
    let rules = "  targets:\n    - id: ^strict\n      type: GNMI_GNOI\n      config: {skip-verify: false}\n    - id: .*\n      type: GNMI_GNOI\n      config: {skip-verify: true}\n";
    let c = env.collector(
        rules,
        &format!(
            "insecure: false\nretry: 500ms\n{SUB}outputs:\n{}",
            file_output("f", &env.p("out.json"), "")
        ),
    );
    let _strict = env.sim("strict", &["--inner-tls", "--id-prefix", "strict-1"]);
    let _lax = env.sim(
        "lax",
        &["--inner-tls", "--id-prefix", "lax-1", "--interval-ms", "200"],
    );
    env.wait_for("data from skip-verify target", Duration::from_secs(10), || {
        read(&env.path("out.json")).contains("lax-1")
    });
    c.wait_log("TLS handshake with target", Duration::from_secs(10));
    assert!(!read(&env.path("out.json")).contains("strict-1"));
    assert_eq!(c.terminate(), 0);
}

#[test]
fn invalid_config_fails_fast() {
    let env = Env::new();
    fs::write(
        env.path("bad.yaml"),
        "subscriptions:\n  s:\n    paths: [/a]\n    mode: poll\n",
    )
    .unwrap();
    let out = Command::new(COLLECTOR)
        .args(["--config", &env.p("bad.yaml")])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("POLL"));
    fs::write(env.path("ok.yaml"), SUB).unwrap();
    let out = Command::new(COLLECTOR)
        .args(["--config", &env.p("ok.yaml"), "--check"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
}

fn pg_url() -> Option<String> {
    std::env::var("GNMIC_RS_TEST_PG_URL")
        .ok()
        .filter(|s| !s.is_empty())
}

fn psql(url: &str, sql: &str) -> String {
    // Runs via the postgres client library used by the collector.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (client, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(conn);
        let rows = client.simple_query(sql).await.unwrap();
        rows.iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(",")
    })
}

#[test]
fn postgres_rows() {
    let Some(url) = pg_url() else {
        eprintln!("skipped: GNMIC_RS_TEST_PG_URL not set");
        return;
    };
    let env = Env::new();
    let table = format!("e2e_{}", env.port);
    let wide = format!("e2e_wide_{}", env.port);
    let body = format!(
        "insecure: true\n{SUB}outputs:\n  pg:\n    type: postgres\n    url: {url}\n    table: {table}\n    create-table: true\n    flush-interval: 200ms\n  wide:\n    type: postgres\n    url: {url}\n    table: {wide}\n    create-table: true\n    row-mode: event\n    flush-interval: 200ms\n    columns:\n      - {{name: ts, type: timestamptz, source: timestamp}}\n      - {{name: device, type: text, source: \"tag:source\"}}\n      - {{name: ifname, type: text, source: \"tag:interface_name\"}}\n      - {{name: in_octets, type: bigint, source: \"value:in-octets\"}}\n"
    );
    let c = env.collector("", &body);
    let _sim = env.sim(
        "sim",
        &[
            "--interfaces",
            "2",
            "--interval-ms",
            "200",
            "--count",
            "3",
            "--deterministic",
        ],
    );
    env.wait_for("rows", Duration::from_secs(15), || {
        psql(&url, &format!("select count(*) from {wide}")) == "6"
    });
    assert_eq!(c.terminate(), 0);
    // 3 samples x 2 interfaces x 6 leaves.
    assert_eq!(psql(&url, &format!("select count(*) from {table}")), "36");
    assert_eq!(
        psql(
            &url,
            &format!("select string_agg(in_octets::text, ' ' order by in_octets) from {wide}")
        ),
        "0 64 64000 64064 128000 128064"
    );
    assert_eq!(
        psql(
            &url,
            &format!("select value #>> '{{}}' from {table} where path like '%oper-status' limit 1")
        ),
        "UP"
    );
    psql(&url, &format!("drop table {table}; drop table {wide}"));
}

#[test]
fn postgres_unreachable_does_not_block_collection() {
    let env = Env::new();
    let dead = free_port();
    let body = format!(
        "insecure: true\n{SUB}outputs:\n  pg:\n    type: postgres\n    url: postgres://u:p@127.0.0.1:{dead}/db\n    table: t\n    flush-interval: 200ms\n{}",
        file_output("f", &env.p("out.json"), "")
    );
    let c = env.collector("", &body);
    c.wait_log("postgres not reachable yet", Duration::from_secs(10));
    let _sim = env.sim("sim", &["--interval-ms", "200", "--count", "3"]);
    // The file output keeps receiving while postgres retries in the background.
    env.wait_for("file data", Duration::from_secs(10), || {
        read(&env.path("out.json"))
            .lines()
            .filter(|l| l.contains("\"updates\""))
            .count()
            >= 3
    });
    c.wait_log("postgres unavailable", Duration::from_secs(10));
    // Shutdown gives up on the unreachable database instead of hanging.
    assert_eq!(c.terminate(), 0);
}

#[test]
fn shipped_configs_are_valid() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for cfg in ["examples/gnmic.yaml", "deploy/gnmic.yaml"] {
        let out = Command::new(COLLECTOR)
            .args(["--check", "--config"])
            .arg(root.join(cfg))
            .env("GNMI_USERNAME", "u")
            .env("GNMI_PASSWORD", "p")
            .env("PG_PASSWORD", "p")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{cfg}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

#[test]
fn broken_tunnel_session_is_redialed() {
    let env = Env::new();
    let c = env.collector(
        "",
        &format!(
            "insecure: true\nretry: 300ms\n{SUB}outputs:\n{}",
            file_output("f", &env.p("out.json"), "")
        ),
    );
    // The device stays registered but each gNMI session dies after 1 s.
    let _sim = env.sim("sim", &["--interval-ms", "100", "--session-lifetime-ms", "1000"]);
    env.wait_for("three tunnel sessions", Duration::from_secs(20), || {
        c.log().matches("dialing tunnel connection").count() >= 3
    });
    let before = read(&env.path("out.json")).lines().count();
    env.wait_for("data after re-dial", Duration::from_secs(10), || {
        read(&env.path("out.json")).lines().count() > before + 3
    });
    assert!(c.log().contains("subscription receive error"));
    assert!(
        !c.log().contains("stopping target"),
        "the target was never deregistered"
    );
    assert_eq!(c.terminate(), 0);
}

#[test]
fn connection_limit_sheds_with_goaway() {
    let env = Env::new();
    let c = env.collector(
        "  admission:\n    max-connections: 1\n",
        &format!(
            "insecure: true\n{SUB}outputs:\n{}",
            file_output("f", &env.p("out.json"), "")
        ),
    );
    let first = env.sim("first", &["--id-prefix", "first"]);
    c.wait_log("target=first", Duration::from_secs(10));
    let second = env.sim("second", &["--id-prefix", "second"]);
    // The second device is turned away and keeps retrying...
    env.wait_for("second device shed", Duration::from_secs(10), || {
        second.log().contains("reconnecting")
    });
    assert!(!c.log().contains("target=second"));
    // ...and gets in once a slot frees up.
    first.kill();
    c.wait_log(
        "tunnel server discovered target target=second",
        Duration::from_secs(15),
    );
    drop(second);
    assert_eq!(c.terminate(), 0);
}

#[test]
fn registration_rate_limit_queues_then_sheds() {
    let env = Env::new();
    let c = env.collector(
        "  admission:\n    registration-rate: 1\n    registration-burst: 1\n    registration-queue-timeout: 0s\n",
        &format!("insecure: true\n{SUB}outputs:\n{}", file_output("f", &env.p("out.json"), "")),
    );
    let sims: Vec<_> = (0..3)
        .map(|i| env.sim(&format!("sim{i}"), &["--id-prefix", &format!("dev{i}")]))
        .collect();
    // All three connect at once; one is admitted, the others are told to retry.
    env.wait_for("a refused registration", Duration::from_secs(10), || {
        sims.iter().any(|s| s.log().contains("tunnel server busy"))
    });
    env.wait_for("all registered eventually", Duration::from_secs(20), || {
        (0..3).all(|i| c.log().contains(&format!("discovered target target=dev{i}")))
    });
    drop(sims);
    assert_eq!(c.terminate(), 0);
}

#[test]
fn target_setups_are_queued() {
    let env = Env::new();
    let c = env.collector(
        "  admission:\n    max-concurrent-setups: 1\n",
        &format!(
            "insecure: true\n{SUB}outputs:\n{}",
            file_output("f", &env.p("out.json"), "")
        ),
    );
    // Each device takes 1.5 s to deliver its initial sync.
    let _sim = env.sim("sim", &["--targets", "3", "--initial-delay-ms", "1500"]);
    c.wait_log("sending gNMI SubscribeRequest", Duration::from_secs(10));
    std::thread::sleep(Duration::from_millis(700));
    assert_eq!(
        c.log().matches("sending gNMI SubscribeRequest").count(),
        1,
        "one setup at a time"
    );
    env.wait_for("all three subscribed in turn", Duration::from_secs(15), || {
        c.log().matches("sending gNMI SubscribeRequest").count() == 3
    });
    env.wait_for("data from all", Duration::from_secs(10), || {
        let out = read(&env.path("out.json"));
        (1..=3).all(|i| out.contains(&format!("sim-router-{i}")))
    });
    assert_eq!(c.terminate(), 0);
}

#[test]
fn skip_verify_flag_accepts_self_signed_device() {
    let env = Env::new();
    fs::write(
        env.path("cfg.yaml"),
        format!(
            "tunnel-server:\n  address: 127.0.0.1:{}\n{SUB}outputs:\n{}",
            env.port,
            file_output("f", &env.p("out.json"), "")
        ),
    )
    .unwrap();
    let c = Proc::spawn(
        COLLECTOR,
        &["--config", &env.p("cfg.yaml"), "--skip-verify"],
        env.path("collector.log"),
    );
    c.wait_log("tunnel server listening", Duration::from_secs(10));
    let _sim = env.sim("sim", &["--inner-tls", "--interval-ms", "200"]);
    env.wait_for(
        "data over TLS with a self-signed device cert",
        Duration::from_secs(10),
        || read(&env.path("out.json")).contains("\"updates\""),
    );
    assert_eq!(c.terminate(), 0);
}

#[test]
fn postgres_block_mode_backpressures_and_still_shuts_down() {
    let env = Env::new();
    let dead = free_port();
    let body = format!(
        "insecure: true\n{SUB}outputs:\n  pg:\n    type: postgres\n    url: postgres://u:p@127.0.0.1:{dead}/db\n    table: t\n    batch-size: 10\n    max-buffered-rows: 10\n    buffer-size: 1\n    buffer-full: block\n{}",
        file_output("f", &env.p("out.json"), "")
    );
    let c = env.collector("", &body);
    let _sim = env.sim("sim", &["--interval-ms", "50"]);
    // With the database down and the buffer full, the subscription is held
    // back: the file output (same subscription) stops growing.
    env.wait_for("some data", Duration::from_secs(10), || {
        read(&env.path("out.json")).lines().count() >= 2
    });
    std::thread::sleep(Duration::from_secs(2));
    let a = read(&env.path("out.json")).lines().count();
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(
        read(&env.path("out.json")).lines().count(),
        a,
        "backpressure applied"
    );
    let start = Instant::now();
    assert_eq!(c.terminate(), 0);
    assert!(start.elapsed() < Duration::from_secs(14), "bounded shutdown");
}

#[test]
fn setup_timeout_releases_slot_of_slow_device() {
    let env = Env::new();
    let c = env.collector(
        "  admission:\n    max-concurrent-setups: 1\n    setup-timeout: 1s\n",
        &format!(
            "insecure: true\n{SUB}outputs:\n{}",
            file_output("f", &env.p("out.json"), "")
        ),
    );
    // Both devices take 20 s to sync; the second still starts after ~1 s.
    let _sim = env.sim("sim", &["--targets", "2", "--initial-delay-ms", "20000"]);
    c.wait_log("sending gNMI SubscribeRequest", Duration::from_secs(10));
    let start = Instant::now();
    env.wait_for("second setup after the timeout", Duration::from_secs(5), || {
        c.log().matches("sending gNMI SubscribeRequest").count() == 2
    });
    assert!(
        start.elapsed() >= Duration::from_millis(700),
        "{:?}",
        start.elapsed()
    );
    assert_eq!(c.terminate(), 0);
}

#[test]
fn insecure_flag_overrides_tls_settings_before_validation() {
    let env = Env::new();
    fs::write(
        env.path("cfg.yaml"),
        format!("tls-ca: /nonexistent/ca.pem\n{SUB}"),
    )
    .unwrap();
    let check = |extra: &[&str]| {
        Command::new(COLLECTOR)
            .args(["--check", "--config", &env.p("cfg.yaml")])
            .args(extra)
            .output()
            .unwrap()
            .status
            .success()
    };
    assert!(!check(&[]));
    assert!(check(&["--insecure"]));
    assert!(check(&["--skip-verify"]));
}
