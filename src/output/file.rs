//! File output: stdout, stderr or a file (optionally rotated, with gzip
//! compression of rotated files), configured like gnmic's `type: file`.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::Msg;
use super::rotate::{RotatingFile, RotationConfig};
use crate::format::{AddTarget, Format, Marshaler};

const WRITE_BUFFER: usize = 256 * 1024;

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct FileConfig {
    pub filename: String,
    pub file_type: String,
    pub format: String,
    pub multiline: bool,
    pub indent: String,
    pub separator: String,
    pub split_events: bool,
    pub override_timestamps: bool,
    pub add_target: String,
    pub calculate_latency: bool,
    pub rotation: Option<RotationConfig>,
    /// Messages queued for this output before backpressure applies.
    pub buffer_size: Option<usize>,

    // gnmic keys accepted for compatibility.
    pub name: String,
    pub target_template: String,
    pub msg_template: String,
    pub event_processors: Vec<String>,
    pub concurrency_limit: i64,
    pub enable_metrics: bool,
    pub debug: bool,
}

impl FileConfig {
    pub fn stdout() -> Self {
        FileConfig {
            file_type: "stdout".into(),
            ..Default::default()
        }
    }

    fn marshaler(&self, file_type: &str) -> Result<Marshaler> {
        let stdio = matches!(file_type, "stdout" | "stderr");
        let multiline = self.multiline || stdio;
        let indent = match self.indent.as_str() {
            _ if stdio => "  ",
            "" => "  ",
            i => i,
        };
        Ok(Marshaler {
            format: Format::parse(&self.format)?,
            indent: multiline.then(|| indent.to_string()),
            split_events: self.split_events,
            override_timestamps: self.override_timestamps,
            calculate_latency: self.calculate_latency,
            add_target: AddTarget::parse(&self.add_target)?,
        })
    }
}

/// Turns messages into the bytes this output writes (runs on the caller's task).
pub struct Formatter {
    marshaler: Marshaler,
    separator: Vec<u8>,
}

impl Formatter {
    pub fn format(&self, msg: &Msg, out: &mut Vec<u8>) -> Result<()> {
        self.marshaler.format(msg, out, &self.separator)
    }
}

enum Sink {
    Plain(BufWriter<Box<dyn Write + Send>>),
    Rotating(RotatingFile),
}

impl Sink {
    fn write(&mut self, chunk: &[u8]) -> io::Result<()> {
        match self {
            Sink::Plain(w) => w.write_all(chunk),
            Sink::Rotating(r) => r.write_chunk(chunk),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Sink::Plain(w) => w.flush(),
            Sink::Rotating(r) => r.flush(),
        }
    }
}

/// Opens the output and starts its writer. Each received chunk holds the
/// complete records of one message.
pub fn start(
    name: &str,
    cfg: &FileConfig,
    mut rx: mpsc::Receiver<Vec<u8>>,
) -> Result<(Formatter, JoinHandle<()>)> {
    let file_type = match cfg.file_type.as_str() {
        "" if cfg.filename.is_empty() => "stdout",
        t => t,
    };
    let marshaler = cfg.marshaler(file_type)?;
    for (key, set) in [
        ("event-processors", !cfg.event_processors.is_empty()),
        ("msg-template", !cfg.msg_template.is_empty()),
        ("target-template", !cfg.target_template.is_empty()),
    ] {
        if set {
            tracing::warn!(output = name, key, "unsupported file output option, ignoring");
        }
    }
    let (mut sink, label) = match file_type {
        "stdout" => (
            Sink::Plain(BufWriter::with_capacity(WRITE_BUFFER, Box::new(io::stdout()))),
            "stdout".to_string(),
        ),
        "stderr" => (
            Sink::Plain(BufWriter::with_capacity(WRITE_BUFFER, Box::new(io::stderr()))),
            "stderr".to_string(),
        ),
        "" => {
            let sink = match &cfg.rotation {
                Some(rot) => Sink::Rotating(RotatingFile::open(&cfg.filename, rot)?),
                None => {
                    let f: File = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&cfg.filename)
                        .with_context(|| format!("opening {}", cfg.filename))?;
                    Sink::Plain(BufWriter::with_capacity(WRITE_BUFFER, Box::new(f)))
                }
            };
            (sink, cfg.filename.clone())
        }
        other => anyhow::bail!("unknown file-type {other:?}, expected stdout or stderr"),
    };
    let separator = if cfg.separator.is_empty() {
        "\n"
    } else {
        cfg.separator.as_str()
    };
    tracing::info!(output = name, file = %label, format = ?marshaler.format, "initialized file output");
    let formatter = Formatter {
        marshaler,
        separator: separator.as_bytes().to_vec(),
    };

    let name = name.to_string();
    // File I/O is blocking: run the writer on its own thread.
    let handle = tokio::task::spawn_blocking(move || {
        let mut write_err = false;
        while let Some(chunk) = rx.blocking_recv() {
            match sink.write(&chunk) {
                Err(e) if !write_err => {
                    tracing::error!(output = %name, file = %label, error = %e, "failed to write to file");
                    write_err = true;
                }
                Err(_) => {}
                Ok(()) => write_err = false,
            }
            // Flush whenever the queue is drained: low latency when idle,
            // large writes under load.
            if rx.is_empty() {
                if let Err(e) = sink.flush() {
                    tracing::error!(output = %name, file = %label, error = %e, "failed to flush file");
                }
            }
        }
        if let Err(e) = sink.flush() {
            tracing::error!(output = %name, file = %label, error = %e, "failed to flush file");
        }
        tracing::info!(output = %name, file = %label, "closed file output");
    });
    Ok((formatter, handle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_keys() {
        let c: crate::config::OutputConfig = serde_yaml::from_str(
            "{type: file, filename: /tmp/x, format: event, concurrency-limit: 10, rotation: {max-size: 5, compress: true}}",
        )
        .unwrap();
        let crate::config::OutputConfig::File(c) = c else {
            panic!()
        };
        assert_eq!(c.rotation.unwrap().max_size, 5);
        let bad: Result<crate::config::OutputConfig, _> =
            serde_yaml::from_str("{type: file, filname: /tmp/x}");
        assert!(bad.is_err(), "typos must be rejected");
    }

    #[test]
    fn stdout_is_multiline() {
        let m = FileConfig::stdout().marshaler("stdout").unwrap();
        assert_eq!(m.indent.as_deref(), Some("  "));
        let c = FileConfig {
            filename: "x".into(),
            ..Default::default()
        };
        assert_eq!(c.marshaler("").unwrap().indent, None);
        let c = FileConfig {
            multiline: true,
            indent: "\t".into(),
            ..Default::default()
        };
        assert_eq!(c.marshaler("").unwrap().indent.as_deref(), Some("\t"));
    }
}
