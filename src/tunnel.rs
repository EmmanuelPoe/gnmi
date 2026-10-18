//! openconfig/grpctunnel server.
//!
//! Devices (tunnel clients) open a `Register` stream and announce their targets.
//! To reach a target, the server sends a `Session{tag}` on that client's
//! register stream; the client answers by opening a `Tunnel` stream whose first
//! message carries the tag, after which the stream carries raw bytes (here: a
//! gNMI HTTP/2 connection where the collector is the gRPC client).

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use bytes::Bytes;
use futures_util::Stream;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::{CancellationToken, PollSender};
use tonic::transport::server::{Connected, TcpConnectInfo};

use crate::admission::{Admission, ConnectionGuard};
use tonic::{Request, Response, Status, Streaming};

use crate::proto::tunnel::tunnel_server::{Tunnel, TunnelServer};
use crate::proto::tunnel::{Data, RegisterOp, Session, Subscription, register_op, target};

const MAX_TUNNEL_MSG: usize = 64 * 1024 * 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const SHED_TIMEOUT: Duration = Duration::from_secs(2);
// HTTP/2 flow-control windows bound what a backlogged device can make us
// buffer; 1 MiB per stream sustains ~80 Mbit/s per device at 100 ms RTT.
pub const STREAM_WINDOW: u32 = 1024 * 1024;
pub const CONN_WINDOW: u32 = 2 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TunnelTarget {
    pub id: String,
    pub target_type: String,
}

impl std::fmt::Display for TunnelTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.id, self.target_type)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum TargetEvent {
    Added(TunnelTarget),
    Removed(TunnelTarget),
}

type RegTx = mpsc::Sender<Result<RegisterOp, Status>>;

fn peer(addr: Option<SocketAddr>) -> String {
    addr.map_or_else(|| "unknown".into(), |a| a.to_string())
}

struct Client {
    addr: Option<SocketAddr>,
    tx: RegTx,
    targets: HashSet<TunnelTarget>,
}

struct Pending {
    client: u64,
    tx: oneshot::Sender<Result<TunnelIo, String>>,
}

#[derive(Default)]
struct State {
    clients: HashMap<u64, Client>,
    targets: HashMap<TunnelTarget, u64>,
    pending: HashMap<i32, Pending>,
}

/// Shared tunnel state: registered clients/targets and in-flight session requests.
pub struct Registry {
    state: Mutex<State>,
    next_client: AtomicU64,
    next_tag: AtomicU64,
    events: mpsc::UnboundedSender<TargetEvent>,
}

impl Registry {
    pub fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<TargetEvent>) {
        let (events, rx) = mpsc::unbounded_channel();
        let reg = Registry {
            state: Mutex::default(),
            next_client: AtomicU64::new(1),
            next_tag: AtomicU64::new(0),
            events,
        };
        (Arc::new(reg), rx)
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        // The lock is never held across code that can panic.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn emit(&self, ev: TargetEvent) {
        let _ = self.events.send(ev);
    }

    /// Server-initiated session tags are positive (grpctunnel convention).
    fn next_tag(&self) -> i32 {
        (self.next_tag.fetch_add(1, Ordering::Relaxed) % i32::MAX as u64) as i32 + 1
    }

    fn add_client(&self, addr: Option<SocketAddr>, tx: RegTx) -> u64 {
        let id = self.next_client.fetch_add(1, Ordering::Relaxed);
        self.state().clients.insert(
            id,
            Client {
                addr,
                tx,
                targets: HashSet::new(),
            },
        );
        id
    }

    fn remove_client(&self, id: u64) {
        let mut st = self.state();
        let Some(client) = st.clients.remove(&id) else {
            return;
        };
        // Dropping the senders fails the waiters of this client's sessions.
        st.pending.retain(|_, p| p.client != id);
        for t in client.targets {
            if st.targets.get(&t) == Some(&id) {
                st.targets.remove(&t);
                tracing::info!(target = %t.id, r#type = %t.target_type, "tunnel target deregistered (client disconnected)");
                // Emitted under the lock: events follow registry order.
                self.emit(TargetEvent::Removed(t));
            }
        }
    }

    /// Ends every register stream (shutdown), so graceful server shutdown is
    /// not held up by devices keeping their streams open.
    pub fn close_all(&self) {
        let mut st = self.state();
        st.clients.clear();
        st.targets.clear();
        st.pending.clear();
    }

    async fn handle_op(&self, client: u64, op: RegisterOp) {
        let reply = match op.registration {
            Some(register_op::Registration::Target(t)) => self.handle_target(client, t),
            Some(register_op::Registration::Session(s)) => self.handle_session(s),
            Some(register_op::Registration::Subscription(s)) => {
                // Peer-target subscriptions only matter for tunnel bridging,
                // which a collector does not do; acknowledge so clients proceed.
                Some(register_op::Registration::Subscription(Subscription {
                    op: s.op,
                    accept: true,
                    target_type: s.target_type,
                    error: String::new(),
                }))
            }
            None => None,
        };
        if let Some(reply) = reply {
            let tx = self.state().clients.get(&client).map(|c| c.tx.clone());
            if let Some(tx) = tx {
                let _ = tx
                    .send(Ok(RegisterOp {
                        registration: Some(reply),
                    }))
                    .await;
            }
        }
    }

    /// Applies a target registration and emits its event while holding the
    /// lock, so the manager sees events in registry order. Like grpctunnel,
    /// the ack is queued before the event; returns a reply still to be sent.
    fn handle_target(
        &self,
        client: u64,
        t: crate::proto::tunnel::Target,
    ) -> Option<register_op::Registration> {
        let mut st = self.state();
        let (reply, event) = Self::apply_target(&mut st, client, t);
        let Some(event) = event else { return reply };
        let mut pending_reply = None;
        if let (Some(reply), Some(c)) = (reply, st.clients.get(&client)) {
            let op = RegisterOp {
                registration: Some(reply),
            };
            if let Err(mpsc::error::TrySendError::Full(op)) = c.tx.try_send(Ok(op)) {
                pending_reply = op.ok().and_then(|o| o.registration);
            }
        }
        self.emit(event);
        pending_reply
    }

    fn apply_target(
        st: &mut State,
        client: u64,
        t: crate::proto::tunnel::Target,
    ) -> (Option<register_op::Registration>, Option<TargetEvent>) {
        let tt = TunnelTarget {
            id: t.target.clone(),
            target_type: t.target_type.clone(),
        };
        let reply = |accept: bool, error: String| {
            Some(register_op::Registration::Target(crate::proto::tunnel::Target {
                op: 0,
                accept,
                target: t.target.clone(),
                target_type: t.target_type.clone(),
                error,
            }))
        };
        let refuse = |error: String| (reply(false, error), None);
        let op = target::TargetOp::try_from(t.op).unwrap_or(target::TargetOp::Unknown);
        match op {
            target::TargetOp::Add => {
                if tt.id.is_empty() {
                    return refuse("empty target id".into());
                }
                let Some(c) = st.clients.get(&client) else {
                    return (None, None);
                };
                if c.targets.contains(&tt) {
                    return refuse(format!("target {tt} already registered by this client"));
                }
                // Newest registration wins: a device that reconnects after a
                // reboot must not be locked out by its own stale session.
                if let Some(old) = st.targets.insert(tt.clone(), client) {
                    if let Some(oc) = st.clients.get_mut(&old) {
                        oc.targets.remove(&tt);
                        tracing::warn!(target = %tt.id, old_peer = %peer(oc.addr), "target re-registered by another connection, replacing the previous registration");
                    }
                }
                let Some(c) = st.clients.get_mut(&client) else {
                    return (None, None);
                };
                c.targets.insert(tt.clone());
                tracing::info!(target = %tt.id, r#type = %tt.target_type, peer = %peer(c.addr), "tunnel server discovered target");
                (reply(true, String::new()), Some(TargetEvent::Added(tt)))
            }
            target::TargetOp::Remove => {
                let owned = st.clients.get_mut(&client).is_some_and(|c| c.targets.remove(&tt));
                if !owned {
                    return refuse(format!("target {tt} is not registered"));
                }
                if st.targets.get(&tt) != Some(&client) {
                    // Already taken over by a newer registration.
                    return (reply(true, String::new()), None);
                }
                st.targets.remove(&tt);
                tracing::info!(target = %tt.id, r#type = %tt.target_type, "tunnel server target deregister request");
                (reply(true, String::new()), Some(TargetEvent::Removed(tt)))
            }
            target::TargetOp::Unknown => refuse(format!("invalid target op: {}", t.op)),
        }
    }

    fn handle_session(&self, s: Session) -> Option<register_op::Registration> {
        if !s.error.is_empty() {
            // The client refused a session we requested.
            if let Some(p) = self.state().pending.remove(&s.tag) {
                let _ = p.tx.send(Err(s.error));
            }
            return None;
        }
        // Client-initiated sessions target the server's local targets; a
        // collector has none.
        Some(register_op::Registration::Session(Session {
            tag: s.tag,
            error: format!("no target {:?} of type {:?} registered", s.target, s.target_type),
            ..Default::default()
        }))
    }

    /// Asks the device that registered `target` to open a tunnel stream.
    pub async fn new_session(&self, target: &TunnelTarget, timeout: Duration) -> Result<TunnelIo> {
        let tag = self.next_tag();
        let (tx, rx) = oneshot::channel();
        let client_tx = {
            let mut st = self.state();
            let client = *st
                .targets
                .get(target)
                .ok_or_else(|| anyhow!("unknown tunnel target {target}"))?;
            let ctx = st
                .clients
                .get(&client)
                .map(|c| c.tx.clone())
                .context("client gone")?;
            st.pending.insert(tag, Pending { client, tx });
            ctx
        };
        // Removes the pending entry however this future ends, including when
        // the caller drops it (connect timeout, target cancellation).
        struct Cleanup<'a>(&'a Registry, i32);
        impl Drop for Cleanup<'_> {
            fn drop(&mut self) {
                self.0.state().pending.remove(&self.1);
            }
        }
        let _cleanup = Cleanup(self, tag);
        async {
            let op = RegisterOp {
                registration: Some(register_op::Registration::Session(Session {
                    tag,
                    accept: true,
                    target: target.id.clone(),
                    target_type: target.target_type.clone(),
                    error: String::new(),
                })),
            };
            client_tx
                .send(Ok(op))
                .await
                .map_err(|_| anyhow!("register stream closed"))?;
            match tokio::time::timeout(timeout, rx).await {
                Err(_) => bail!("timeout waiting for tunnel session (tag {tag})"),
                Ok(Err(_)) => bail!("tunnel client disconnected"),
                Ok(Ok(Err(e))) => bail!("tunnel session refused by client: {e}"),
                Ok(Ok(Ok(io))) => Ok(io),
            }
        }
        .await
    }

    fn take_pending(&self, tag: i32, addr: Option<SocketAddr>) -> Result<Pending, Status> {
        let mut st = self.state();
        let client = st
            .pending
            .get(&tag)
            .map(|p| p.client)
            .ok_or_else(|| Status::not_found(format!("no connection associated with tag {tag}")))?;
        let client_addr = st.clients.get(&client).and_then(|c| c.addr);
        // The data stream must come from the connection that registered the target.
        if let (Some(a), Some(b)) = (client_addr, addr) {
            if a != b {
                return Err(Status::permission_denied("tag belongs to another client"));
            }
        }
        Ok(st.pending.remove(&tag).expect("checked above"))
    }

    #[cfg(test)]
    pub fn target_count(&self) -> usize {
        self.state().targets.len()
    }
}

pub struct TunnelService {
    registry: Arc<Registry>,
    admission: Arc<Admission>,
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl Tunnel for TunnelService {
    type RegisterStream = BoxStream<RegisterOp>;
    type TunnelStream = BoxStream<Data>;

    async fn register(
        &self,
        req: Request<Streaming<RegisterOp>>,
    ) -> Result<Response<Self::RegisterStream>, Status> {
        let addr = req.remote_addr();
        // Registration rate limit: wait for our turn, or be told to retry.
        if let Err(retry_after) = self.admission.admit_registration().await {
            tracing::debug!(peer = %peer(addr), ?retry_after, "registration rate limit reached, shedding registration");
            let mut status = Status::resource_exhausted("tunnel server busy, retry later");
            if let Ok(v) = retry_after.as_millis().to_string().parse() {
                status.metadata_mut().insert("grpc-retry-pushback-ms", v);
            }
            return Err(status);
        }
        let mut inbound = req.into_inner();
        let (tx, rx) = mpsc::channel(64);
        let registry = self.registry.clone();
        let id = registry.add_client(addr, tx);
        tracing::debug!(peer = %peer(addr), "tunnel client connected");
        tokio::spawn(async move {
            loop {
                match inbound.message().await {
                    Ok(Some(op)) => registry.handle_op(id, op).await,
                    Ok(None) => break,
                    Err(e) => {
                        tracing::debug!(peer = %peer(addr), error = %e, "register stream error");
                        break;
                    }
                }
            }
            tracing::debug!(peer = %peer(addr), "tunnel client disconnected");
            registry.remove_client(id);
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn tunnel(&self, req: Request<Streaming<Data>>) -> Result<Response<Self::TunnelStream>, Status> {
        let addr = req.remote_addr();
        let mut inbound = req.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("tunnel stream closed before tag"))?;
        if !first.data.is_empty() {
            return Err(Status::invalid_argument("received data but only wanted tag"));
        }
        let pending = self.registry.take_pending(first.tag, addr)?;
        let (tx, rx) = mpsc::channel::<Data>(64);
        if pending.tx.send(Ok(TunnelIo::new(inbound, tx))).is_err() {
            return Err(Status::cancelled("session abandoned"));
        }
        let out = tokio_stream::StreamExt::map(ReceiverStream::new(rx), Ok);
        Ok(Response::new(Box::pin(out)))
    }
}

enum ConnIo {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

/// An admitted device connection: reports its peer address to tonic and
/// releases its admission slot when closed.
struct Conn {
    io: ConnIo,
    info: TcpConnectInfo,
    _slot: ConnectionGuard,
}

impl Connected for Conn {
    type ConnectInfo = TcpConnectInfo;
    fn connect_info(&self) -> TcpConnectInfo {
        self.info.clone()
    }
}

macro_rules! delegate {
    ($self:ident, $io:ident => $e:expr) => {
        match &mut $self.get_mut().io {
            ConnIo::Plain($io) => {
                let $io = Pin::new($io);
                $e
            }
            ConnIo::Tls($io) => {
                let $io = Pin::new($io.as_mut());
                $e
            }
        }
    };
}

impl AsyncRead for Conn {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        delegate!(self, io => io.poll_read(cx, buf))
    }
}

impl AsyncWrite for Conn {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        delegate!(self, io => io.poll_write(cx, buf))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        delegate!(self, io => io.poll_flush(cx))
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        delegate!(self, io => io.poll_shutdown(cx))
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        delegate!(self, io => io.poll_write_vectored(cx, bufs))
    }
    fn is_write_vectored(&self) -> bool {
        match &self.io {
            ConnIo::Plain(io) => io.is_write_vectored(),
            ConnIo::Tls(io) => io.is_write_vectored(),
        }
    }
}

/// Sheds a cleartext connection with an HTTP/2 GOAWAY: gRPC clients treat
/// it as "reconnect later" and apply their reconnect backoff.
async fn goaway(tcp: TcpStream) {
    // A stream opened before the client saw the GOAWAY gets a trailers-only
    // gRPC UNAVAILABLE, which clients also treat as "retry later".
    let svc = hyper::service::service_fn(|_| async {
        let rsp = hyper::Response::builder()
            .header("content-type", "application/grpc")
            .header("grpc-status", "14")
            .header("grpc-message", "tunnel server busy, retry later")
            .body(String::new())
            .expect("static response");
        Ok::<_, std::convert::Infallible>(rsp)
    });
    let conn = hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
        .serve_connection(hyper_util::rt::TokioIo::new(tcp), svc);
    tokio::pin!(conn);
    conn.as_mut().graceful_shutdown();
    let _ = tokio::time::timeout(SHED_TIMEOUT, conn).await;
}

fn tune(tcp: &TcpStream) {
    let _ = tcp.set_nodelay(true);
    let ka = socket2::TcpKeepalive::new().with_time(Duration::from_secs(60));
    let _ = socket2::SockRef::from(tcp).set_tcp_keepalive(&ka);
}

/// Accepts device connections, applying the connection cap and the TLS
/// handshake limit, and hands admitted connections to the gRPC server.
async fn accept_loop(
    listener: TcpListener,
    tls: Option<tokio_rustls::TlsAcceptor>,
    admission: Arc<Admission>,
    tx: mpsc::Sender<io::Result<Conn>>,
) {
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "tunnel server accept error");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let Some(slot) = admission.try_connection() else {
            tracing::debug!(%peer, "connection limit reached, shedding connection");
            // Shedding is bounded too: beyond that, close right away.
            if let (None, Ok(permit)) = (&tls, admission.shedders.clone().try_acquire_owned()) {
                tokio::spawn(async move {
                    goaway(tcp).await;
                    drop(permit);
                });
            }
            continue;
        };
        tune(&tcp);
        let info = TcpConnectInfo {
            local_addr: tcp.local_addr().ok(),
            remote_addr: Some(peer),
        };
        let (tls, admission, tx) = (tls.clone(), admission.clone(), tx.clone());
        // Handshakes run off the accept loop so a slow peer cannot stall others.
        tokio::spawn(async move {
            let io = match tls {
                None => ConnIo::Plain(tcp),
                Some(acceptor) => {
                    // Waiting for a handshake slot is bounded so sockets of
                    // devices that gave up do not keep their connection slot.
                    let Ok(_permit) =
                        tokio::time::timeout(HANDSHAKE_TIMEOUT, admission.handshake_permit()).await
                    else {
                        tracing::debug!(%peer, "no TLS handshake slot in time, closing connection");
                        return;
                    };
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                        Ok(Ok(s)) => ConnIo::Tls(Box::new(s)),
                        Ok(Err(e)) => {
                            tracing::warn!(%peer, error = %e, "tunnel server TLS handshake failed");
                            return;
                        }
                        Err(_) => {
                            tracing::warn!(%peer, "tunnel server TLS handshake timed out");
                            return;
                        }
                    }
                }
            };
            let _ = tx
                .send(Ok(Conn {
                    io,
                    info,
                    _slot: slot,
                }))
                .await;
        });
    }
}

/// Resolves gnmic-style listen addresses (":57400" means all interfaces).
pub async fn bind(address: &str) -> Result<TcpListener> {
    if let Some(port) = address.strip_prefix(':') {
        if let Ok(l) = TcpListener::bind(format!("[::]:{port}")).await {
            return Ok(l);
        }
        return TcpListener::bind(format!("0.0.0.0:{port}"))
            .await
            .with_context(|| format!("binding tunnel server to {address}"));
    }
    TcpListener::bind(address)
        .await
        .with_context(|| format!("binding tunnel server to {address}"))
}

/// Serves the tunnel service until `shutdown` is cancelled.
pub async fn serve(
    listener: TcpListener,
    tls: Option<Arc<rustls::ServerConfig>>,
    registry: Arc<Registry>,
    admission: Arc<Admission>,
    shutdown: CancellationToken,
) -> Result<()> {
    let svc = TunnelServer::new(TunnelService {
        registry,
        admission: admission.clone(),
    })
    .max_decoding_message_size(MAX_TUNNEL_MSG)
    .max_encoding_message_size(MAX_TUNNEL_MSG);
    let server = tonic::transport::Server::builder()
        // Detect dead devices (power loss, NAT expiry) so their targets are
        // released instead of lingering until TCP gives up.
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(20)))
        .initial_stream_window_size(STREAM_WINDOW)
        .initial_connection_window_size(CONN_WINDOW)
        .add_service(svc);
    let (tx, rx) = mpsc::channel(64);
    let acceptor = tls.map(tokio_rustls::TlsAcceptor::from);
    let accept = tokio::spawn(accept_loop(listener, acceptor, admission, tx));
    let res = server
        .serve_with_incoming_shutdown(ReceiverStream::new(rx), shutdown.cancelled_owned())
        .await;
    accept.abort();
    Ok(res?)
}

/// One end of a tunnel session as a byte stream.
pub struct TunnelIo {
    rx: Streaming<Data>,
    buf: Bytes,
    eof: bool,
    tx: PollSender<Data>,
    closed: bool,
}

impl TunnelIo {
    pub fn new(rx: Streaming<Data>, tx: mpsc::Sender<Data>) -> Self {
        TunnelIo {
            rx,
            buf: Bytes::new(),
            eof: false,
            tx: PollSender::new(tx),
            closed: false,
        }
    }
}

fn broken(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, msg.into())
}

impl AsyncRead for TunnelIo {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, out: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        while this.buf.is_empty() && !this.eof {
            match ready!(Pin::new(&mut this.rx).poll_next(cx)) {
                Some(Ok(d)) if d.close => this.eof = true,
                Some(Ok(d)) => this.buf = d.data,
                Some(Err(s)) => return Poll::Ready(Err(broken(format!("tunnel stream: {s}")))),
                None => this.eof = true,
            }
        }
        let n = this.buf.len().min(out.remaining());
        out.put_slice(&this.buf.split_to(n));
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for TunnelIo {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, b: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Err(broken("tunnel stream closed")));
        }
        ready!(this.tx.poll_reserve(cx)).map_err(|_| broken("tunnel stream closed"))?;
        let d = Data {
            data: Bytes::copy_from_slice(b),
            ..Default::default()
        };
        this.tx.send_item(d).map_err(|_| broken("tunnel stream closed"))?;
        Poll::Ready(Ok(b.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.closed {
            if ready!(this.tx.poll_reserve(cx)).is_ok() {
                let _ = this.tx.send_item(Data {
                    close: true,
                    ..Default::default()
                });
            }
            this.tx.close();
            this.closed = true;
        }
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::tunnel::Target as PTarget;

    fn add(id: &str) -> RegisterOp {
        RegisterOp {
            registration: Some(register_op::Registration::Target(PTarget {
                op: target::TargetOp::Add as i32,
                target: id.into(),
                target_type: "GNMI_GNOI".into(),
                ..Default::default()
            })),
        }
    }

    fn tt(id: &str) -> TunnelTarget {
        TunnelTarget {
            id: id.into(),
            target_type: "GNMI_GNOI".into(),
        }
    }

    fn accepted(op: Option<Result<RegisterOp, Status>>) -> bool {
        match op.unwrap().unwrap().registration.unwrap() {
            register_op::Registration::Target(t) => t.accept,
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn registration_lifecycle() {
        let (reg, mut events) = Registry::new();
        let (tx1, mut rx1) = mpsc::channel(8);
        let c1 = reg.add_client(None, tx1);
        reg.handle_op(c1, add("r1")).await;
        assert!(accepted(rx1.recv().await));
        assert_eq!(events.recv().await, Some(TargetEvent::Added(tt("r1"))));
        // Duplicate from the same client is refused.
        reg.handle_op(c1, add("r1")).await;
        assert!(!accepted(rx1.recv().await));

        // The same target from a new connection replaces the old registration;
        // the old connection going away must then not remove it.
        let (tx2, mut rx2) = mpsc::channel(8);
        let c2 = reg.add_client(None, tx2);
        reg.handle_op(c2, add("r1")).await;
        assert!(accepted(rx2.recv().await));
        assert_eq!(events.recv().await, Some(TargetEvent::Added(tt("r1"))));
        reg.remove_client(c1);
        assert_eq!(reg.target_count(), 1);
        reg.remove_client(c2);
        assert_eq!(events.recv().await, Some(TargetEvent::Removed(tt("r1"))));
        assert_eq!(reg.target_count(), 0);
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn session_refused_and_unknown_target() {
        let (reg, _events) = Registry::new();
        assert!(
            reg.new_session(&tt("nope"), Duration::from_secs(1))
                .await
                .is_err()
        );
        let (tx, mut rx) = mpsc::channel(8);
        let c = reg.add_client(None, tx);
        reg.handle_op(c, add("r1")).await;
        rx.recv().await;
        let reg2 = reg.clone();
        let refuse = tokio::spawn(async move {
            let op = rx.recv().await.unwrap().unwrap();
            let Some(register_op::Registration::Session(s)) = op.registration else {
                panic!()
            };
            assert!(s.accept && s.tag > 0);
            let err = RegisterOp {
                registration: Some(register_op::Registration::Session(Session {
                    tag: s.tag,
                    error: "busy".into(),
                    ..Default::default()
                })),
            };
            reg2.handle_op(c, err).await;
        });
        let err = reg
            .new_session(&tt("r1"), Duration::from_secs(5))
            .await
            .err()
            .unwrap();
        assert!(err.to_string().contains("busy"), "{err}");
        refuse.await.unwrap();
        assert!(reg.state().pending.is_empty());
    }

    #[tokio::test]
    async fn abandoned_session_request_is_cleaned_up() {
        let (reg, _events) = Registry::new();
        let (tx, mut rx) = mpsc::channel(8);
        let c = reg.add_client(None, tx);
        reg.handle_op(c, add("r1")).await;
        rx.recv().await;
        // The device never answers; the caller gives up first (connect timeout).
        let attempt = tokio::time::timeout(
            Duration::from_millis(50),
            reg.new_session(&tt("r1"), Duration::from_secs(60)),
        )
        .await;
        assert!(attempt.is_err());
        assert!(reg.state().pending.is_empty());
    }
}
