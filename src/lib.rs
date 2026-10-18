//! gNMI dial-out collector: an openconfig/grpctunnel server that subscribes to
//! registered devices and writes telemetry to files or PostgreSQL.

pub mod admission;
pub mod config;
pub mod event;
pub mod format;
pub mod gojson;
pub mod output;
pub mod path;
pub mod subscribe;
pub mod target;
pub mod tls;
pub mod tunnel;

pub mod proto {
    #![allow(clippy::all)]
    pub mod gnmi_ext {
        tonic::include_proto!("gnmi_ext");
    }
    pub mod gnmi {
        tonic::include_proto!("gnmi");
    }
    pub mod tunnel {
        tonic::include_proto!("grpctunnel");
    }
    pub const DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/descriptor.bin"));
}

use std::sync::Arc;

use anyhow::Result;
use tokio_util::sync::CancellationToken;

/// Runs the collector until `shutdown` is cancelled, then drains outputs.
pub async fn run(
    cfg: config::Config,
    listener: tokio::net::TcpListener,
    shutdown: CancellationToken,
) -> Result<()> {
    let cfg = Arc::new(cfg);
    let tls = cfg
        .tunnel_server
        .tls
        .as_ref()
        .map(tls::server_config)
        .transpose()?;
    let outputs = Arc::new(output::Outputs::start(&cfg.outputs).await?);
    let (registry, events) = tunnel::Registry::new();
    let admission = admission::Admission::new(&cfg.tunnel_server.admission);
    tokio::spawn(admission.clone().report(shutdown.clone()));
    tracing::info!(address = %listener.local_addr()?, tls = tls.is_some(), "tunnel server listening");

    let manager = tokio::spawn(target::manage(
        cfg.clone(),
        registry.clone(),
        admission.clone(),
        events,
        outputs.clone(),
        shutdown.clone(),
    ));
    let closer = {
        let (registry, shutdown) = (registry.clone(), shutdown.clone());
        async move {
            shutdown.cancelled().await;
            registry.close_all();
            // Bound the wait for devices to go away.
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    };
    let served = tokio::select! {
        r = tunnel::serve(listener, tls, registry, admission, shutdown.clone()) => r,
        _ = closer => Ok(()),
    };
    // The server only returns on shutdown or failure; stop targets either way.
    shutdown.cancel();
    let _ = manager.await;
    match Arc::try_unwrap(outputs) {
        Ok(outputs) => outputs.shutdown().await,
        Err(_) => tracing::warn!("outputs still referenced at shutdown"),
    }
    served
}
