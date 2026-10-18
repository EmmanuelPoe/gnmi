//! Per-target lifecycle: when a device registers through the tunnel, dial its
//! gNMI server over a tunnel session and run the configured subscriptions,
//! retrying until the device deregisters.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::codec::CompressionEncoding;
use tonic::metadata::{MetadataKey, MetadataValue};
use tonic::transport::{Channel, Endpoint, Uri};

use crate::admission::Admission;
use crate::config::{Config, SubscriptionConfig, Target};
use crate::output::{Meta, Msg, Output, Outputs};
use crate::proto::gnmi::g_nmi_client::GNmiClient;
use crate::proto::gnmi::{SubscribeRequest, subscribe_request, subscribe_response, subscription_list};
use crate::subscribe;
use crate::tunnel::{Registry, TargetEvent, TunnelTarget};

trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}

/// Runs until `shutdown` is cancelled, then waits for target tasks to stop.
pub async fn manage(
    cfg: Arc<Config>,
    registry: Arc<Registry>,
    admission: Arc<Admission>,
    mut events: mpsc::UnboundedReceiver<TargetEvent>,
    outputs: Arc<Outputs>,
    shutdown: CancellationToken,
) {
    let tracker = TaskTracker::new();
    let mut running: HashMap<TunnelTarget, CancellationToken> = HashMap::new();
    loop {
        let ev = tokio::select! {
            _ = shutdown.cancelled() => break,
            ev = events.recv() => match ev {
                Some(ev) => ev,
                None => break,
            },
        };
        match ev {
            TargetEvent::Added(tt) => {
                if let Some(old) = running.remove(&tt) {
                    old.cancel();
                }
                let Some(target) = cfg.match_tunnel_target(&tt.id, &tt.target_type) else {
                    tracing::info!(target = %tt.id, r#type = %tt.target_type, "tunnel target ignored (no matching rule)");
                    continue;
                };
                let token = shutdown.child_token();
                running.insert(tt.clone(), token.clone());
                let ctx = Ctx {
                    cfg: cfg.clone(),
                    registry: registry.clone(),
                    admission: admission.clone(),
                    outputs: outputs.clone(),
                };
                tracker.spawn(run_target(ctx, tt, target, token));
            }
            TargetEvent::Removed(tt) => {
                if let Some(token) = running.remove(&tt) {
                    tracing::info!(target = %tt.id, "stopping target");
                    token.cancel();
                }
            }
        }
    }
    tracker.close();
    tracker.wait().await;
}

struct Sub {
    name: String,
    config: SubscriptionConfig,
    request: SubscribeRequest,
}

fn subscriptions(cfg: &Config, target: &Target) -> Result<Vec<Sub>> {
    let names: Vec<&String> = if target.subscriptions.is_empty() {
        cfg.subscriptions.keys().collect()
    } else {
        target.subscriptions.iter().collect()
    };
    names
        .into_iter()
        .map(|n| {
            let sc = cfg
                .subscriptions
                .get(n)
                .with_context(|| format!("unknown subscription {n:?}"))?;
            Ok(Sub {
                name: n.clone(),
                config: sc.clone(),
                request: subscribe::build_request(sc, &target.name, &target.encoding)?,
            })
        })
        .collect()
}

/// Shared services a target task needs.
struct Ctx {
    cfg: Arc<Config>,
    registry: Arc<Registry>,
    admission: Arc<Admission>,
    outputs: Arc<Outputs>,
}

async fn run_target(ctx: Ctx, tt: TunnelTarget, target: Target, token: CancellationToken) {
    let Ctx {
        cfg,
        registry,
        admission,
        outputs,
    } = ctx;
    let subs = match subscriptions(&cfg, &target) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(target = %target.name, error = %format!("{e:#}"), "invalid subscription for target");
            return;
        }
    };
    let tls = match tls_dial(&target) {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(target = %target.name, error = %format!("{e:#}"), "invalid TLS settings for target");
            return;
        }
    };
    let target = Arc::new(target);
    let (channel, setup) = loop {
        // Flood protection: only a bounded number of targets dial and receive
        // their initial sync at once; the others wait here in FIFO order.
        let setup = tokio::select! {
            _ = token.cancelled() => return,
            p = admission.setup_permit() => p,
        };
        let attempt = tokio::select! {
            _ = token.cancelled() => return,
            r = connect(&registry, &tt, &target, &tls) => r,
        };
        match attempt {
            Ok(ch) => break (ch, setup),
            Err(e) => {
                drop(setup);
                tracing::warn!(target = %target.name, error = %format!("{e:#}"), retry_in = ?target.retry, "failed to initialize target");
                tokio::select! {
                    _ = token.cancelled() => return,
                    _ = tokio::time::sleep(target.retry) => {}
                }
            }
        }
    };
    tracing::info!(target = %target.name, "target gNMI client created");

    let tasks = TaskTracker::new();
    let mut setup_done = Vec::new();
    for sub in subs {
        let mut meta = Meta::new();
        meta.insert("source".into(), target.name.clone());
        meta.insert("subscription-name".into(), sub.name.clone());
        if !sub.config.target.is_empty() {
            meta.insert("subscription-target".into(), sub.config.target.clone());
        }
        meta.extend(target.event_tags.clone());
        // Subscription outputs take precedence over target outputs.
        let outs = if sub.config.outputs.is_empty() {
            &target.outputs
        } else {
            &sub.config.outputs
        };
        let outs = outputs.select(outs);
        let (done_tx, done_rx) = oneshot::channel();
        setup_done.push(done_rx);
        tasks.spawn(run_subscription(
            channel.clone(),
            target.clone(),
            sub,
            Arc::new(meta),
            outs,
            Some(done_tx),
            token.clone(),
        ));
    }
    tasks.close();
    // The setup slot is held until every subscription reports its initial
    // sync (or a failure), and never longer than setup-timeout.
    if setup.is_some() {
        tokio::select! {
            _ = futures_util::future::join_all(setup_done) => {}
            _ = tokio::time::sleep(admission.setup_timeout) => {
                tracing::debug!(target = %target.name, "setup timeout, releasing setup slot");
            }
            _ = token.cancelled() => {}
        }
        drop(setup);
    }
    tasks.wait().await;
    tracing::info!(target = %target.name, "target stopped");
}

type TlsDial = Option<(tokio_rustls::TlsConnector, rustls::pki_types::ServerName<'static>)>;

/// TLS for the gNMI session inside the tunnel, built once per target.
fn tls_dial(target: &Target) -> Result<TlsDial> {
    if target.insecure {
        return Ok(None);
    }
    let cfg = crate::tls::client_config(&crate::tls::ClientTls::for_target(target))?;
    Ok(Some((
        tokio_rustls::TlsConnector::from(cfg),
        crate::tls::server_name(target)?,
    )))
}

/// Opens the gNMI channel. Every (re)connect requests a new tunnel session.
async fn connect(
    registry: &Arc<Registry>,
    tt: &TunnelTarget,
    target: &Arc<Target>,
    tls: &TlsDial,
) -> Result<Channel> {
    let (registry, tt, t, tls) = (registry.clone(), tt.clone(), target.clone(), tls.clone());
    let connector = tower::service_fn(move |_: Uri| {
        let (registry, tt, t, tls) = (registry.clone(), tt.clone(), t.clone(), tls.clone());
        async move {
            tracing::info!(target = %t.name, "dialing tunnel connection for tunnel target");
            // One deadline for session setup and the TLS handshake.
            let dial = async {
                let io = registry.new_session(&tt, t.timeout).await?;
                let io: Box<dyn Io> = match tls {
                    None => Box::new(io),
                    Some((connector, name)) => Box::new(
                        connector
                            .connect(name, io)
                            .await
                            .context("TLS handshake with target")?,
                    ),
                };
                Ok::<_, anyhow::Error>(TokioIo::new(io))
            };
            tokio::time::timeout(t.timeout, dial)
                .await
                .map_err(|_| anyhow::anyhow!("timeout ({:?}) establishing tunnel session", t.timeout))?
        }
    });
    // The authority is the target name, as gnmic dials address=name.
    let uri = format!("http://{}", target.name);
    let endpoint = Endpoint::from_shared(uri)
        .or_else(|_| Endpoint::from_shared("http://tunnel-target".to_string()))?
        .initial_stream_window_size(crate::tunnel::STREAM_WINDOW)
        .initial_connection_window_size(crate::tunnel::CONN_WINDOW);
    Ok(endpoint.connect_with_connector(connector).await?)
}

fn request_metadata<T>(req: &mut tonic::Request<T>, t: &Target) {
    let md = req.metadata_mut();
    let mut put = |k: &str, v: &str| match (MetadataKey::from_bytes(k.as_bytes()), MetadataValue::try_from(v))
    {
        (Ok(k), Ok(v)) => {
            md.append(k, v);
        }
        _ => tracing::warn!(target = %t.name, key = k, "invalid gRPC metadata, skipped"),
    };
    if !t.username.is_empty() {
        put("username", &t.username);
    }
    if !t.password.is_empty() {
        put("password", &t.password);
    }
    if !t.token.is_empty() {
        put("authorization", &format!("Bearer {}", t.token));
    }
    for (k, v) in &t.metadata {
        put(&k.to_ascii_lowercase(), v);
    }
}

/// "Unauthenticated: invalid credentials" rather than tonic's long Display form.
fn status_str(s: &tonic::Status) -> String {
    format!("{:?}: {}", s.code(), s.message())
}

fn mode_names(req: &SubscribeRequest) -> (String, String) {
    match &req.request {
        Some(subscribe_request::Request::Subscribe(l)) => (
            subscription_list::Mode::try_from(l.mode).map_or("?".into(), |m| m.as_str_name().into()),
            crate::proto::gnmi::Encoding::try_from(l.encoding)
                .map_or_else(|_| l.encoding.to_string(), |e| e.as_str_name().into()),
        ),
        _ => Default::default(),
    }
}

/// Runs one subscription until cancelled, re-subscribing after `retry` on
/// errors. ONCE subscriptions stop after the target closes the stream.
/// `setup_done` is consumed (sent or dropped) at the first sync-response or
/// the end of the first attempt, releasing the target's setup slot.
async fn run_subscription(
    channel: Channel,
    target: Arc<Target>,
    sub: Sub,
    meta: Arc<Meta>,
    outputs: Vec<Output>,
    mut setup_done: Option<oneshot::Sender<()>>,
    token: CancellationToken,
) {
    let once = matches!(&sub.request.request, Some(subscribe_request::Request::Subscribe(l)) if l.mode == subscription_list::Mode::Once as i32);
    let (mode, encoding) = mode_names(&sub.request);
    let mut client = GNmiClient::new(channel)
        .max_decoding_message_size(target.max_msg_size)
        .accept_compressed(CompressionEncoding::Gzip);
    if target.gzip {
        client = client.send_compressed(CompressionEncoding::Gzip);
    }
    loop {
        // Keep the request stream open for the life of the subscription:
        // closing it half-closes the RPC, which some targets treat as cancel.
        let (req_tx, req_rx) = mpsc::channel(1);
        let _ = req_tx.send(sub.request.clone()).await;
        let mut req = tonic::Request::new(ReceiverStream::new(req_rx));
        request_metadata(&mut req, &target);
        tracing::info!(target = %target.name, subscription = %sub.name, %mode, %encoding, "sending gNMI SubscribeRequest");
        let result = tokio::select! {
            _ = token.cancelled() => return,
            r = client.subscribe(req) => r,
        };
        let mut finished = false;
        match result {
            Err(status) => {
                tracing::error!(target = %target.name, subscription = %sub.name, error = %status_str(&status), retry_in = ?target.retry, "failed to create a subscribe client");
            }
            Ok(resp) => {
                let mut stream = resp.into_inner();
                let mut synced = false;
                loop {
                    let msg = tokio::select! {
                        _ = token.cancelled() => return,
                        m = stream.message() => m,
                    };
                    match msg {
                        Ok(Some(rsp)) => {
                            if matches!(rsp.response, Some(subscribe_response::Response::SyncResponse(_))) {
                                if !synced {
                                    tracing::info!(target = %target.name, subscription = %sub.name, "initial sync complete");
                                    synced = true;
                                }
                                if let Some(done) = setup_done.take() {
                                    let _ = done.send(());
                                }
                            }
                            #[allow(deprecated)]
                            if let Some(subscribe_response::Response::Error(e)) = &rsp.response {
                                tracing::warn!(target = %target.name, subscription = %sub.name, code = e.code, message = %e.message, "target returned an error response");
                            }
                            let msg = Msg {
                                rsp,
                                meta: meta.clone(),
                            };
                            for out in &outputs {
                                // A backlogged output must not block cancellation.
                                tokio::select! {
                                    _ = token.cancelled() => return,
                                    r = out.write(&msg) => if r.is_err() { return },
                                }
                            }
                        }
                        Ok(None) => {
                            tracing::info!(target = %target.name, subscription = %sub.name, "subscription closed stream (EOF)");
                            finished = once;
                            break;
                        }
                        Err(status) => {
                            tracing::error!(target = %target.name, subscription = %sub.name, error = %status_str(&status), retry_in = ?target.retry, "subscription receive error");
                            break;
                        }
                    }
                }
            }
        }
        drop(req_tx);
        // Retries never hold a setup slot.
        setup_done = None;
        if finished {
            return;
        }
        tokio::select! {
            _ = token.cancelled() => return,
            _ = tokio::time::sleep(target.retry) => {}
        }
    }
}
