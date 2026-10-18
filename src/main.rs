use std::io::IsTerminal;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use gnmic_rs::config::Config;

/// gNMI dial-out collector (openconfig/grpctunnel) for Cisco devices.
///
/// Reads a gnmic-compatible configuration file, accepts device tunnel
/// registrations and writes subscription data to the configured outputs.
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// Configuration file (gnmic YAML format).
    #[arg(long, env = "GNMIC_RS_CONFIG", default_value = "gnmic.yaml")]
    config: PathBuf,
    /// Debug logging (also `debug: true` in the config file).
    #[arg(short, long)]
    debug: bool,
    /// Write logs to this file instead of stderr.
    #[arg(long)]
    log_file: Option<PathBuf>,
    /// Log format: text or json.
    #[arg(long)]
    log_format: Option<String>,
    /// Validate the configuration and exit.
    #[arg(long)]
    check: bool,
    /// Accept self-signed/unverified device certificates for gNMI inside the
    /// tunnel (as gnmic's --skip-verify; per-target rules can override).
    #[arg(long)]
    skip_verify: bool,
    /// Plaintext gNMI inside the tunnel (as gnmic's --insecure).
    #[arg(long)]
    insecure: bool,
    /// Accepted for gnmic command-line compatibility (always enabled).
    #[arg(long, hide = true)]
    use_tunnel_server: bool,
    /// Accepted for gnmic command-line compatibility: `gnmic subscribe`.
    #[arg(hide = true)]
    command: Option<String>,
}

fn init_logging(cli: &Cli, cfg: &Config) -> Result<()> {
    let level = if cli.debug || cfg.debug { "debug" } else { "info" };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("warn,gnmic_rs={level}")));
    let json = cli.log_format.as_deref().or(cfg.log_format.as_deref()) == Some("json");
    let log_file = cli
        .log_file
        .clone()
        .or_else(|| cfg.log_file.as_ref().map(PathBuf::from));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false);
    match (log_file, json) {
        (Some(path), json) => {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("opening log file {}", path.display()))?;
            let b = builder.with_ansi(false).with_writer(std::sync::Mutex::new(file));
            if json { b.json().init() } else { b.init() }
        }
        (None, true) => builder.json().with_writer(std::io::stderr).init(),
        (None, false) => builder
            .with_ansi(std::io::stderr().is_terminal())
            .with_writer(std::io::stderr)
            .init(),
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installing SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Some(cmd) = cli.command.as_deref().filter(|c| *c != "subscribe") {
        anyhow::bail!("unsupported command {cmd:?}: only `subscribe` (dial-out collection) is supported");
    }
    let cfg = Config::load(&cli.config, |cfg| {
        cfg.skip_verify |= cli.skip_verify;
        cfg.insecure |= cli.insecure;
    })?;
    init_logging(&cli, &cfg)?;
    for key in &cfg.ignored_keys {
        tracing::warn!(key, "ignoring unsupported configuration key");
    }
    if cli.check {
        println!(
            "configuration OK: {} subscription(s), {} output(s)",
            cfg.subscriptions.len(),
            cfg.outputs.len()
        );
        return Ok(());
    }
    tracing::info!(version = env!("CARGO_PKG_VERSION"), config = %cli.config.display(), "starting gnmic-rs");

    let listener = gnmic_rs::tunnel::bind(cfg.tunnel_address()).await?;
    let shutdown = CancellationToken::new();
    let signal = {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            tracing::info!("shutting down");
            shutdown.cancel();
        })
    };
    let res = gnmic_rs::run(cfg, listener, shutdown).await;
    signal.abort();
    tracing::info!("stopped");
    res
}
