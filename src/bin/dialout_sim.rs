//! Test device: emulates a Cisco IOS XE doing gNMI dial-out over grpctunnel.
//!
//! It registers targets with a tunnel server, serves gNMI Subscribe over the
//! tunnel sessions the server opens, and streams interface counters in the
//! requested encoding (JSON_IETF like IOS XE, JSON, or PROTO typed values).

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use clap::Parser;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::Connected;
use tonic::transport::{Endpoint, Uri};
use tonic::{Request, Response, Status, Streaming};

use gnmic_rs::path;
use gnmic_rs::proto::gnmi::g_nmi_server::{GNmi, GNmiServer};
use gnmic_rs::proto::gnmi::{
    CapabilityRequest, CapabilityResponse, Decimal64, Encoding, GetRequest, GetResponse, Notification, Path,
    ScalarArray, SetRequest, SetResponse, SubscribeRequest, SubscribeResponse, TypedValue, Update,
    subscribe_request, subscribe_response, subscription_list, typed_value,
};
use gnmic_rs::proto::tunnel::tunnel_client::TunnelClient;
use gnmic_rs::proto::tunnel::{Data, RegisterOp, Session, Target, register_op, target};
use gnmic_rs::tunnel::TunnelIo;

#[derive(Parser, Clone)]
struct Opts {
    /// Tunnel server address (host:port).
    #[arg(long, default_value = "127.0.0.1:57400")]
    server: String,
    /// Number of targets to register.
    #[arg(long, default_value_t = 1)]
    targets: usize,
    /// Target ID prefix; IDs are <prefix>-<n> (or just <prefix> for one target).
    #[arg(long, default_value = "sim-router")]
    id_prefix: String,
    #[arg(long, default_value = "GNMI_GNOI")]
    target_type: String,
    /// TLS towards the tunnel server.
    #[arg(long)]
    tls: bool,
    /// CA to verify the tunnel server (default: accept any certificate).
    #[arg(long)]
    tls_ca: Option<String>,
    /// Client certificate/key presented to the tunnel server.
    #[arg(long)]
    tls_cert: Option<String>,
    #[arg(long)]
    tls_key: Option<String>,
    /// Serve gNMI inside the tunnel over TLS with a self-signed certificate.
    #[arg(long)]
    inner_tls: bool,
    /// Require these gRPC metadata credentials on Subscribe.
    #[arg(long)]
    username: Option<String>,
    #[arg(long)]
    password: Option<String>,
    /// Interfaces per target (one update each per sample).
    #[arg(long, default_value_t = 4)]
    interfaces: usize,
    /// Notifications per subscription per sample interval (load testing).
    #[arg(long, default_value_t = 1)]
    burst: usize,
    /// Override the requested sample interval (milliseconds).
    #[arg(long)]
    interval_ms: Option<u64>,
    /// Stop sending after this many samples per subscription (stream stays open).
    #[arg(long)]
    count: Option<u64>,
    /// Deterministic timestamps and counters (for output comparisons).
    #[arg(long)]
    deterministic: bool,
    /// Deregister and exit after this many seconds.
    #[arg(long)]
    duration: Option<u64>,
    /// Fault injection: break each tunnel session after this many
    /// milliseconds while keeping the targets registered.
    #[arg(long)]
    session_lifetime_ms: Option<u64>,
    /// One tunnel connection per target, like separate devices (default:
    /// all targets share one connection).
    #[arg(long)]
    connection_per_target: bool,
    /// Delay before the first sample and the sync-response (a slow initial dump).
    #[arg(long, default_value_t = 0)]
    initial_delay_ms: u64,
}

static SENT: AtomicU64 = AtomicU64::new(0);

fn target_ids(o: &Opts) -> Vec<String> {
    if o.targets == 1 {
        return vec![o.id_prefix.clone()];
    }
    (1..=o.targets).map(|i| format!("{}-{i}", o.id_prefix)).collect()
}

/// IO wrapper so tonic can serve sessions from a channel.
struct Conn<T>(T);

impl<T> Connected for Conn<T> {
    type ConnectInfo = ();
    fn connect_info(&self) {}
}

impl<T: AsyncRead + Unpin> AsyncRead for Conn<T> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Conn<T> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

/// Fault injection: an IO that fails once its deadline has passed.
struct Expiring<T> {
    inner: T,
    deadline: Pin<Box<tokio::time::Sleep>>,
}

fn expired() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "session lifetime elapsed")
}

impl<T: AsyncRead + Unpin> AsyncRead for Expiring<T> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(expired()));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Expiring<T> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(expired()));
        }
        Pin::new(&mut this.inner).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}

struct Sim {
    id: String,
    opts: Arc<Opts>,
}

fn now_ns() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
}

fn leaf(encoding: i32, v: CounterValue) -> TypedValue {
    let value = match v {
        CounterValue::U64(n) if encoding == Encoding::Proto as i32 => typed_value::Value::UintVal(n),
        CounterValue::Str(s) if encoding == Encoding::Proto as i32 => typed_value::Value::StringVal(s),
        // RFC 7951: 64-bit integers are JSON strings.
        CounterValue::U64(n) => typed_value::Value::JsonIetfVal(format!("\"{n}\"").into_bytes()),
        CounterValue::Str(s) => typed_value::Value::JsonIetfVal(format!("\"{s}\"").into_bytes()),
    };
    TypedValue { value: Some(value) }
}

enum CounterValue {
    U64(u64),
    Str(String),
}

/// One sample for one subscribed path. Interface paths are answered like a
/// device does (prefix `/interfaces`, one update per interface); any other
/// path gets the same interface counters under the subscribed path.
fn sample(
    sub_path: &Path,
    prefix: Option<&Path>,
    encoding: i32,
    interfaces: usize,
    seq: u64,
    ts: i64,
) -> Notification {
    let mut p = prefix.cloned().unwrap_or_default();
    if p.origin.is_empty() {
        p.origin = sub_path.origin.clone();
    }
    match sub_path.elem.first() {
        Some(e) if e.name == "interfaces" => p.elem.push(e.clone()),
        _ => p.elem.extend(sub_path.elem.iter().cloned()),
    }
    let mut update = Vec::with_capacity(interfaces);
    for i in 0..interfaces {
        let name = format!("GigabitEthernet1/0/{}", i + 1);
        let base = seq * 1000 + i as u64;
        if encoding == Encoding::Proto as i32 {
            // PROTO: one typed leaf per update, like IOS XE.
            for (leafname, v) in [
                ("in-octets", CounterValue::U64(base * 64)),
                ("out-octets", CounterValue::U64(base * 32)),
                ("oper-status", CounterValue::Str("UP".into())),
            ] {
                update.push(Update {
                    path: Some(
                        path::parse(&format!("interface[name={name}]/state/{leafname}")).expect("valid path"),
                    ),
                    val: Some(leaf(encoding, v)),
                    ..Default::default()
                });
            }
            // The other scalar types, to exercise every value encoding.
            #[allow(deprecated)]
            let extra = [
                ("enabled", typed_value::Value::BoolVal(i % 2 == 0)),
                ("mtu", typed_value::Value::IntVal(-(1500 + i as i64))),
                ("load", typed_value::Value::DoubleVal(base as f64 / 3.0)),
                ("ratio", typed_value::Value::FloatVal(0.1 + i as f32)),
                (
                    "rate",
                    typed_value::Value::DecimalVal(Decimal64 {
                        digits: base as i64 * 7,
                        precision: 3,
                    }),
                ),
                ("raw", typed_value::Value::BytesVal(vec![0, 1, 2, i as u8])),
                (
                    "description",
                    typed_value::Value::AsciiVal(format!("<uplink & {i}>")),
                ),
                (
                    "vlans",
                    typed_value::Value::LeaflistVal(ScalarArray {
                        element: vec![
                            TypedValue {
                                value: Some(typed_value::Value::UintVal(10 + i as u64)),
                            },
                            TypedValue {
                                value: Some(typed_value::Value::StringVal("trunk".into())),
                            },
                        ],
                    }),
                ),
            ];
            for (leafname, v) in extra {
                update.push(Update {
                    path: Some(
                        path::parse(&format!("interface[name={name}]/state/{leafname}")).expect("valid path"),
                    ),
                    val: Some(TypedValue { value: Some(v) }),
                    ..Default::default()
                });
            }
            continue;
        }
        let json = format!(
            r#"{{"in-octets":"{}","out-octets":"{}","in-unicast-pkts":"{}","in-errors":"0","oper-status":"UP","last-change":"{}"}}"#,
            base * 64,
            base * 32,
            base,
            ts
        );
        let value = if encoding == Encoding::Json as i32 {
            typed_value::Value::JsonVal(json.into_bytes())
        } else {
            typed_value::Value::JsonIetfVal(json.into_bytes())
        };
        update.push(Update {
            path: Some(path::parse(&format!("interface[name={name}]/state/counters")).expect("valid path")),
            val: Some(TypedValue { value: Some(value) }),
            ..Default::default()
        });
    }
    let delete = vec![path::parse(&format!("interface[name=Loopback{seq}]")).expect("valid path")];
    Notification {
        timestamp: ts,
        prefix: Some(p),
        update,
        delete,
        ..Default::default()
    }
}

#[tonic::async_trait]
impl GNmi for Sim {
    type SubscribeStream = ReceiverStream<Result<SubscribeResponse, Status>>;

    async fn capabilities(
        &self,
        _: Request<CapabilityRequest>,
    ) -> Result<Response<CapabilityResponse>, Status> {
        Err(Status::unimplemented("capabilities"))
    }
    async fn get(&self, _: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        Err(Status::unimplemented("get"))
    }
    async fn set(&self, _: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        Err(Status::unimplemented("set"))
    }

    async fn subscribe(
        &self,
        req: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let md = req.metadata();
        let get = |k: &str| md.get(k).and_then(|v| v.to_str().ok()).map(str::to_string);
        if self.opts.username.is_some()
            && (get("username") != self.opts.username || get("password") != self.opts.password)
        {
            return Err(Status::unauthenticated("invalid credentials"));
        }
        let mut inbound = req.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("no request"))?;
        let Some(subscribe_request::Request::Subscribe(list)) = first.request else {
            return Err(Status::invalid_argument("expected a subscription list"));
        };
        eprintln!(
            "[{}] subscribe: mode={} encoding={} paths={:?}",
            self.id,
            list.mode,
            list.encoding,
            list.subscription
                .iter()
                .map(|s| path::to_xpath(s.path.as_ref()))
                .collect::<Vec<_>>()
        );
        let (tx, rx) = mpsc::channel(128);
        let opts = self.opts.clone();
        tokio::spawn(async move {
            // Keep reading so client half-close/cancel is noticed.
            let _inbound = inbound;
            let once = list.mode == subscription_list::Mode::Once as i32;
            let min_interval = list
                .subscription
                .iter()
                .map(|s| s.sample_interval)
                .filter(|i| *i > 0)
                .min()
                .unwrap_or(1_000_000_000);
            let interval = Duration::from_nanos(opts.interval_ms.map_or(min_interval, |ms| ms * 1_000_000));
            tokio::time::sleep(Duration::from_millis(opts.initial_delay_ms)).await;
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let base_ts = 1_700_000_000_000_000_000i64;
            let mut seq = 0u64;
            loop {
                tick.tick().await;
                if opts.count.is_some_and(|c| seq >= c) {
                    if tx.is_closed() {
                        return;
                    }
                    continue;
                }
                for _ in 0..opts.burst {
                    let ts = if opts.deterministic {
                        base_ts + seq as i64 * 1_000_000_000
                    } else {
                        now_ns()
                    };
                    for s in &list.subscription {
                        let Some(p) = s.path.as_ref() else { continue };
                        let n = sample(p, list.prefix.as_ref(), list.encoding, opts.interfaces, seq, ts);
                        let rsp = SubscribeResponse {
                            response: Some(subscribe_response::Response::Update(n)),
                            ..Default::default()
                        };
                        if tx.send(Ok(rsp)).await.is_err() {
                            return;
                        }
                        SENT.fetch_add(1, Ordering::Relaxed);
                    }
                    seq += 1;
                }
                if seq == opts.burst as u64 {
                    let sync = SubscribeResponse {
                        response: Some(subscribe_response::Response::SyncResponse(true)),
                        ..Default::default()
                    };
                    if tx.send(Ok(sync)).await.is_err() {
                        return;
                    }
                    if once {
                        return;
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn client_tls(o: &Opts) -> Result<Arc<rustls::ClientConfig>> {
    let opts = gnmic_rs::tls::ClientTls {
        skip_verify: o.tls_ca.is_none(),
        ca: o.tls_ca.as_deref(),
        cert: o.tls_cert.as_deref(),
        key: o.tls_key.as_deref(),
        alpn_h2: true,
    };
    gnmic_rs::tls::client_config(&opts)
}

fn inner_tls_acceptor() -> Result<tokio_rustls::TlsAcceptor> {
    let cfg = gnmic_rs::tls::server_config(&Default::default())?;
    Ok(tokio_rustls::TlsAcceptor::from(cfg))
}

/// Starts the gNMI server of one target; returns where to hand tunnel sessions.
fn start_target_server(id: String, opts: Arc<Opts>) -> mpsc::Sender<TunnelIo> {
    let (tx, mut rx) = mpsc::channel::<TunnelIo>(16);
    let (conn_tx, conn_rx) = mpsc::channel::<io::Result<Conn<Box<dyn Io>>>>(16);
    let acceptor = opts
        .inner_tls
        .then(|| inner_tls_acceptor().expect("self-signed TLS"));
    let opts2 = opts.clone();
    tokio::spawn(async move {
        let opts = opts2;
        while let Some(io) = rx.recv().await {
            let conn_tx = conn_tx.clone();
            let acceptor = acceptor.clone();
            let lifetime = opts.session_lifetime_ms;
            tokio::spawn(async move {
                let io: Box<dyn Io> = match lifetime {
                    Some(ms) => Box::new(Expiring {
                        inner: io,
                        deadline: Box::pin(tokio::time::sleep(Duration::from_millis(ms))),
                    }),
                    None => Box::new(io),
                };
                let io: Box<dyn Io> = match acceptor {
                    None => io,
                    Some(a) => match a.accept(io).await {
                        Ok(s) => Box::new(s),
                        Err(e) => {
                            eprintln!("inner TLS handshake failed: {e}");
                            return;
                        }
                    },
                };
                let _ = conn_tx.send(Ok(Conn(io))).await;
            });
        }
    });
    let svc = GNmiServer::new(Sim { id, opts });
    tokio::spawn(async move {
        let res = tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(ReceiverStream::new(conn_rx))
            .await;
        if let Err(e) = res {
            eprintln!("gNMI server error: {e}");
        }
    });
    tx
}

async fn connect(o: &Opts) -> Result<TunnelClient<tonic::transport::Channel>> {
    let tls = if o.tls { Some(client_tls(o)?) } else { None };
    let addr = o.server.clone();
    let connector = tower::service_fn(move |_: Uri| {
        let (addr, tls) = (addr.clone(), tls.clone());
        async move {
            let tcp = tokio::net::TcpStream::connect(&addr).await?;
            tcp.set_nodelay(true)?;
            let io: Box<dyn Io> = match tls {
                None => Box::new(tcp),
                Some(cfg) => {
                    let host = addr
                        .rsplit_once(':')
                        .map_or(addr.as_str(), |(h, _)| h)
                        .trim_matches(['[', ']']);
                    let name = rustls::pki_types::ServerName::try_from(host.to_string())?;
                    Box::new(tokio_rustls::TlsConnector::from(cfg).connect(name, tcp).await?)
                }
            };
            Ok::<_, anyhow::Error>(TokioIo::new(io))
        }
    });
    let channel = Endpoint::from_static("http://tunnel-server")
        .connect_timeout(Duration::from_secs(5))
        .connect_with_connector(connector)
        .await
        .context("connecting to tunnel server")?;
    Ok(TunnelClient::new(channel).max_decoding_message_size(64 * 1024 * 1024))
}

fn target_op(id: &str, typ: &str, op: target::TargetOp) -> RegisterOp {
    RegisterOp {
        registration: Some(register_op::Registration::Target(Target {
            op: op as i32,
            target: id.into(),
            target_type: typ.into(),
            ..Default::default()
        })),
    }
}

/// One tunnel client session: register all targets, serve sessions.
async fn run_once(opts: &Arc<Opts>, servers: &[(String, mpsc::Sender<TunnelIo>)]) -> Result<()> {
    let mut client = connect(opts).await?;
    let (reg_tx, reg_rx) = mpsc::channel(64);
    for (id, _) in servers {
        reg_tx
            .send(target_op(id, &opts.target_type, target::TargetOp::Add))
            .await?;
    }
    let mut inbound = client.register(ReceiverStream::new(reg_rx)).await?.into_inner();
    let deadline = opts
        .duration
        .map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
    loop {
        let msg = tokio::select! {
            m = inbound.message() => m?,
            _ = async { tokio::time::sleep_until(deadline.unwrap()).await }, if deadline.is_some() => {
                for (id, _) in servers {
                    reg_tx.send(target_op(id, &opts.target_type, target::TargetOp::Remove)).await?;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                eprintln!("duration elapsed, deregistered targets");
                return Ok(());
            }
        };
        let Some(op) = msg else {
            bail!("tunnel server closed the register stream")
        };
        match op.registration {
            Some(register_op::Registration::Target(t)) => {
                if !t.accept {
                    bail!("target {} rejected: {}", t.target, t.error);
                }
                eprintln!("target {} registered", t.target);
            }
            Some(register_op::Registration::Session(s)) => {
                let Some((_, server)) = servers.iter().find(|(id, _)| *id == s.target) else {
                    let err = RegisterOp {
                        registration: Some(register_op::Registration::Session(Session {
                            tag: s.tag,
                            error: format!("unknown target {}", s.target),
                            ..Default::default()
                        })),
                    };
                    reg_tx.send(err).await?;
                    continue;
                };
                let (mut client, server) = (client.clone(), server.clone());
                tokio::spawn(async move {
                    let (dtx, drx) = mpsc::channel(64);
                    let _ = dtx
                        .send(Data {
                            tag: s.tag,
                            ..Default::default()
                        })
                        .await;
                    match client.tunnel(ReceiverStream::new(drx)).await {
                        Ok(resp) => {
                            let _ = server.send(TunnelIo::new(resp.into_inner(), dtx)).await;
                        }
                        Err(e) => eprintln!("tunnel stream failed: {e}"),
                    }
                });
            }
            _ => {}
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Arc::new(Opts::parse());
    let servers: Vec<_> = target_ids(&opts)
        .into_iter()
        .map(|id| (id.clone(), start_target_server(id, opts.clone())))
        .collect();
    tokio::spawn(async {
        let mut last = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let now = SENT.load(Ordering::Relaxed);
            if now != last {
                eprintln!("sent {now} notifications ({}/s)", (now - last) / 5);
                last = now;
            }
        }
    });
    if !opts.connection_per_target {
        return run_device(&opts, &servers).await;
    }
    let devices: Vec<_> = servers
        .into_iter()
        .map(|server| {
            let opts = opts.clone();
            tokio::spawn(async move { run_device(&opts, &[server]).await })
        })
        .collect();
    for d in devices {
        d.await??;
    }
    Ok(())
}

/// Runs a tunnel client, reconnecting after 1-3 s (jittered, so shed devices
/// do not retry in lockstep).
async fn run_device(opts: &Arc<Opts>, servers: &[(String, mpsc::Sender<TunnelIo>)]) -> Result<()> {
    loop {
        match run_once(opts, servers).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let jitter = chrono::Utc::now().timestamp_subsec_millis() % 2000;
                let delay = Duration::from_millis(1000 + u64::from(jitter));
                eprintln!("tunnel client error: {e:#}; reconnecting in {delay:?}");
                tokio::time::sleep(delay).await;
            }
        }
    }
}
