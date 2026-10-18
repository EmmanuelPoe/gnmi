//! Outputs. Messages are formatted by the subscription task that received
//! them, so formatting runs in parallel across targets; each output then has
//! a single writer fed through a bounded channel, so a slow output applies
//! backpressure instead of buffering without limit.

pub mod file;
pub mod postgres;
mod rotate;

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::OutputConfig;
use crate::proto::gnmi::SubscribeResponse;

/// Message metadata (gnmic outputs.Meta): source, subscription-name, ...
pub type Meta = BTreeMap<String, String>;

pub struct Msg {
    pub rsp: SubscribeResponse,
    pub meta: Arc<Meta>,
}

/// Queue depth (in messages) of an output before backpressure applies.
const DEFAULT_BUFFER: usize = 1024;

#[derive(Clone)]
enum Kind {
    File(Arc<file::Formatter>, mpsc::Sender<Vec<u8>>),
    Postgres(Arc<postgres::RowBuilder>, mpsc::Sender<Vec<postgres::Row>>),
}

/// Handle to an output, cheap to clone.
#[derive(Clone)]
pub struct Output {
    name: Arc<str>,
    kind: Kind,
}

/// The output's writer has stopped (shutdown).
#[derive(Debug)]
pub struct Closed;

impl Output {
    /// Formats `msg` for this output on the calling task and queues it.
    pub async fn write(&self, msg: &Msg) -> Result<(), Closed> {
        match &self.kind {
            Kind::File(fmt, tx) => {
                let mut buf = Vec::with_capacity(1024);
                if let Err(e) = fmt.format(msg, &mut buf) {
                    tracing::warn!(output = %self.name, error = %e, "failed to format message");
                }
                if buf.is_empty() {
                    return Ok(());
                }
                tx.send(buf).await.map_err(|_| Closed)
            }
            Kind::Postgres(rows, tx) => match rows.rows(msg) {
                Ok(r) if r.is_empty() => Ok(()),
                Ok(r) => tx.send(r).await.map_err(|_| Closed),
                Err(e) => {
                    tracing::warn!(output = %self.name, error = %e, "failed to convert message");
                    Ok(())
                }
            },
        }
    }
}

pub struct Outputs {
    outputs: BTreeMap<String, Output>,
    workers: Vec<(String, JoinHandle<()>)>,
}

impl Outputs {
    pub async fn start(cfgs: &BTreeMap<String, OutputConfig>) -> Result<Outputs> {
        let mut outputs = BTreeMap::new();
        let mut workers = Vec::new();
        for (name, cfg) in cfgs {
            let ctx = || format!("output {name:?}");
            let (kind, handle) = match cfg {
                OutputConfig::File(c) => {
                    let (tx, rx) = mpsc::channel(c.buffer_size.unwrap_or(DEFAULT_BUFFER).max(1));
                    let (fmt, handle) = file::start(name, c, rx).with_context(ctx)?;
                    (Kind::File(Arc::new(fmt), tx), handle)
                }
                OutputConfig::Postgres(c) => {
                    let (tx, rx) = mpsc::channel(c.buffer_size.unwrap_or(DEFAULT_BUFFER).max(1));
                    let (rows, handle) = postgres::start(name, c, rx).await.with_context(ctx)?;
                    (Kind::Postgres(Arc::new(rows), tx), handle)
                }
            };
            let name_arc: Arc<str> = Arc::from(name.as_str());
            outputs.insert(name.clone(), Output { name: name_arc, kind });
            workers.push((name.clone(), handle));
        }
        Ok(Outputs { outputs, workers })
    }

    /// The named outputs; all outputs when `names` is empty.
    pub fn select(&self, names: &[String]) -> Vec<Output> {
        if names.is_empty() {
            return self.outputs.values().cloned().collect();
        }
        names
            .iter()
            .filter_map(|n| self.outputs.get(n).cloned())
            .collect()
    }

    /// Waits for every output to drain and flush. Handles returned by
    /// `select` must have been dropped for this to complete.
    pub async fn shutdown(self) {
        drop(self.outputs);
        for (name, h) in self.workers {
            if let Err(e) = h.await {
                tracing::error!(output = %name, error = %e, "output worker failed");
            }
        }
    }
}
