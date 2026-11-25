//! PostgreSQL output: events are turned into rows according to the configured
//! columns and bulk-loaded with binary COPY.

use std::collections::VecDeque;
use std::error::Error as StdError;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::BytesMut;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{IsNull, ToSql, Type, to_sql_checked};
use tokio_postgres::{Client, NoTls};

use super::Msg;
use crate::config::duration_opt;
use crate::event::Event;
use crate::format::{AddTarget, Format, Marshaler, now_nanos};

const DEFAULT_BATCH: usize = 1000;
const DEFAULT_FLUSH: Duration = Duration::from_secs(1);
const DEFAULT_MAX_BUFFERED: usize = 100_000;
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Flush attempts, and their total time, on shutdown before unsent rows are dropped.
const FINAL_ATTEMPTS: u32 = 3;
const FINAL_FLUSH_TIMEOUT: Duration = Duration::from_secs(10);
/// A COPY that takes longer is abandoned (e.g. a black-holed connection).
const COPY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct PostgresConfig {
    /// libpq URL or key=value string; the discrete fields below override it.
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub tls: Option<PgTlsConfig>,
    /// Table name, optionally schema-qualified ("telemetry.gnmi").
    pub table: String,
    /// CREATE TABLE IF NOT EXISTS from `columns` on every (re)connect.
    #[serde(default)]
    pub create_table: bool,
    #[serde(default)]
    pub row_mode: RowMode,
    #[serde(default)]
    pub columns: Vec<ColumnConfig>,
    #[serde(default)]
    pub batch_size: Option<usize>,
    #[serde(default, deserialize_with = "duration_opt")]
    pub flush_interval: Option<Duration>,
    #[serde(default)]
    pub buffer_size: Option<usize>,
    /// Rows kept in memory while the database is slow or unreachable.
    #[serde(default)]
    pub max_buffered_rows: Option<usize>,
    /// What to do when that buffer is full.
    #[serde(default)]
    pub buffer_full: BufferFull,
    #[serde(default)]
    pub add_target: String,
    #[serde(default)]
    pub override_timestamps: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct PgTlsConfig {
    /// disable | require (encrypt, no verification) | verify-full
    pub mode: String,
    pub ca_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BufferFull {
    /// Drop the oldest rows; collection and other outputs are never slowed.
    #[default]
    DropOldest,
    /// Stop taking messages: backpressure slows the subscriptions feeding
    /// this output (gnmic's behavior). No rows are lost while running.
    Block,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RowMode {
    /// One row per leaf value.
    #[default]
    Value,
    /// One row per event (an update and its flattened values).
    Event,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub pg_type: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq)]
enum Source {
    Timestamp,
    Now,
    Name,
    Path,
    Value,
    Values,
    Tags,
    Tag(String),
    ValueOf(String),
}

impl Source {
    fn parse(s: &str, mode: RowMode) -> Result<Source> {
        let src = match s {
            "timestamp" => Source::Timestamp,
            "now" => Source::Now,
            "name" => Source::Name,
            "path" => Source::Path,
            "value" => Source::Value,
            "values" => Source::Values,
            "tags" => Source::Tags,
            _ => match s.split_once(':') {
                Some(("tag", k)) if !k.is_empty() => Source::Tag(k.to_string()),
                Some(("value", k)) if !k.is_empty() => Source::ValueOf(k.to_string()),
                _ => bail!(
                    "unknown column source {s:?} (expected timestamp, now, name, path, value, values, tags, tag:<name> or value:<path>)"
                ),
            },
        };
        match (&src, mode) {
            (Source::Path | Source::Value, RowMode::Event) => {
                bail!("column source {s:?} requires row-mode: value")
            }
            (Source::ValueOf(_), RowMode::Value) => bail!("column source {s:?} requires row-mode: event"),
            _ => Ok(src),
        }
    }
}

fn parse_type(t: &str) -> Result<Type> {
    let norm = t
        .trim()
        .to_ascii_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    Ok(match norm.as_str() {
        "text" => Type::TEXT,
        "varchar" | "character varying" => Type::VARCHAR,
        "bigint" | "int8" => Type::INT8,
        "integer" | "int" | "int4" => Type::INT4,
        "smallint" | "int2" => Type::INT2,
        "double precision" | "float8" => Type::FLOAT8,
        "real" | "float4" => Type::FLOAT4,
        "boolean" | "bool" => Type::BOOL,
        "jsonb" => Type::JSONB,
        "json" => Type::JSON,
        "timestamptz" | "timestamp with time zone" => Type::TIMESTAMPTZ,
        "timestamp" | "timestamp without time zone" => Type::TIMESTAMP,
        _ => bail!(
            "unsupported column type {t:?} (supported: text, varchar, bigint, integer, smallint, double precision, real, boolean, jsonb, json, timestamptz, timestamp)"
        ),
    })
}

struct Column {
    name: String,
    sql_type: String,
    ty: Type,
    source: Source,
}

fn default_columns() -> Vec<ColumnConfig> {
    [
        ("time", "timestamptz", "timestamp"),
        ("target", "text", "tag:source"),
        ("subscription", "text", "name"),
        ("path", "text", "path"),
        ("value", "jsonb", "value"),
        ("tags", "jsonb", "tags"),
    ]
    .into_iter()
    .map(|(n, t, s)| ColumnConfig {
        name: n.into(),
        pg_type: t.into(),
        source: s.into(),
    })
    .collect()
}

/// Quotes an identifier, keeping schema qualification ("a.b" -> "a"."b").
fn quote_ident(name: &str) -> String {
    name.split('.')
        .map(|p| format!("\"{}\"", p.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}

/// A typed cell; values are coerced to the column type, NULL when impossible.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Null,
    Text(String),
    I64(i64),
    I32(i32),
    I16(i16),
    F64(f64),
    F32(f32),
    Bool(bool),
    Json(Value),
    TsTz(DateTime<Utc>),
    Ts(NaiveDateTime),
}

impl ToSql for Cell {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn StdError + Sync + Send>> {
        match self {
            Cell::Null => Ok(IsNull::Yes),
            Cell::Text(v) => v.to_sql(ty, out),
            Cell::I64(v) => v.to_sql(ty, out),
            Cell::I32(v) => v.to_sql(ty, out),
            Cell::I16(v) => v.to_sql(ty, out),
            Cell::F64(v) => v.to_sql(ty, out),
            Cell::F32(v) => v.to_sql(ty, out),
            Cell::Bool(v) => v.to_sql(ty, out),
            Cell::Json(v) => v.to_sql(ty, out),
            Cell::TsTz(v) => v.to_sql(ty, out),
            Cell::Ts(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| {
            n.as_f64()
                .filter(|f| f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64)
                .map(|f| f as i64)
        }),
        Value::String(s) => {
            let s = s.trim();
            s.parse()
                .ok()
                .or_else(|| as_i64(&Value::from(s.parse::<f64>().ok()?)))
        }
        Value::Bool(b) => Some(*b as i64),
        _ => None,
    }
}

fn as_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) if n.is_f64() => {
            let mut b = Vec::new();
            let _ = crate::gojson::write_go_float(&mut b, n.as_f64().unwrap_or_default());
            String::from_utf8(b).unwrap_or_default()
        }
        other => other.to_string(),
    }
}

fn nanos_to_utc(ns: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_nanos(ns)
}

fn coerce(v: &Value, ty: &Type) -> Cell {
    if v.is_null() {
        return Cell::Null;
    }
    let cell = match *ty {
        Type::TEXT | Type::VARCHAR => Some(Cell::Text(as_text(v))),
        Type::INT8 => as_i64(v).map(Cell::I64),
        Type::INT4 => as_i64(v).and_then(|i| i32::try_from(i).ok()).map(Cell::I32),
        Type::INT2 => as_i64(v).and_then(|i| i16::try_from(i).ok()).map(Cell::I16),
        Type::FLOAT8 => as_f64(v).map(Cell::F64),
        Type::FLOAT4 => as_f64(v).map(|f| Cell::F32(f as f32)),
        Type::BOOL => match v {
            Value::Bool(b) => Some(Cell::Bool(*b)),
            Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                "true" | "1" => Some(Cell::Bool(true)),
                "false" | "0" => Some(Cell::Bool(false)),
                _ => None,
            },
            other => as_f64(other).map(|f| Cell::Bool(f != 0.0)),
        },
        Type::JSON | Type::JSONB => Some(Cell::Json(v.clone())),
        Type::TIMESTAMPTZ | Type::TIMESTAMP => {
            let t = match v {
                Value::String(s) => DateTime::parse_from_rfc3339(s)
                    .ok()
                    .map(|t| t.with_timezone(&Utc)),
                other => as_i64(other).map(nanos_to_utc),
            };
            t.map(|t| {
                if *ty == Type::TIMESTAMP {
                    Cell::Ts(t.naive_utc())
                } else {
                    Cell::TsTz(t)
                }
            })
        }
        _ => None,
    };
    cell.unwrap_or(Cell::Null)
}

fn time_cell(ns: i64, ty: &Type) -> Cell {
    match *ty {
        Type::TIMESTAMPTZ => Cell::TsTz(nanos_to_utc(ns)),
        Type::TIMESTAMP => Cell::Ts(nanos_to_utc(ns).naive_utc()),
        _ => coerce(&Value::from(ns), ty),
    }
}

/// Looks up `key` in the event values: exact key, else the first key ending in "/key".
fn value_of<'a>(e: &'a Event, key: &str) -> Option<&'a Value> {
    e.values.get(key).or_else(|| {
        let suffix = format!("/{}", key.trim_start_matches('/'));
        e.values
            .iter()
            .find(|(k, _)| k.ends_with(&suffix))
            .map(|(_, v)| v)
    })
}

pub type Row = Vec<Cell>;

/// Turns messages into rows (runs on the caller's task).
pub struct RowBuilder {
    columns: Vec<Column>,
    mode: RowMode,
    marshaler: Marshaler,
}

impl RowBuilder {
    pub fn rows(&self, msg: &Msg) -> Result<Vec<Row>> {
        let events = self.marshaler.events(msg)?;
        let mut out = Vec::new();
        self.event_rows(&events, now_nanos(), &mut out);
        Ok(out)
    }

    fn event_rows(&self, events: &[Event], now: i64, out: &mut Vec<Row>) {
        for e in events.iter().filter(|e| !e.values.is_empty()) {
            match self.mode {
                RowMode::Event => out.push(self.row(e, None, now)),
                RowMode::Value => {
                    for leaf in &e.values {
                        out.push(self.row(e, Some(leaf), now));
                    }
                }
            }
        }
    }

    fn row(&self, e: &Event, leaf: Option<(&String, &Value)>, now: i64) -> Row {
        self.columns
            .iter()
            .map(|c| match &c.source {
                Source::Timestamp => time_cell(e.timestamp, &c.ty),
                Source::Now => time_cell(now, &c.ty),
                Source::Name => coerce(&Value::from(e.name.as_str()), &c.ty),
                Source::Path => leaf.map_or(Cell::Null, |(p, _)| coerce(&Value::from(p.as_str()), &c.ty)),
                Source::Value => leaf.map_or(Cell::Null, |(_, v)| coerce(v, &c.ty)),
                Source::Values => {
                    let m = match leaf {
                        Some((p, v)) => serde_json::json!({ p: v }),
                        None => serde_json::to_value(&e.values).unwrap_or_default(),
                    };
                    coerce(&m, &c.ty)
                }
                Source::Tags => coerce(&serde_json::to_value(&e.tags).unwrap_or_default(), &c.ty),
                Source::Tag(k) => e
                    .tags
                    .get(k)
                    .map_or(Cell::Null, |v| coerce(&Value::from(v.as_str()), &c.ty)),
                Source::ValueOf(k) => value_of(e, k).map_or(Cell::Null, |v| coerce(v, &c.ty)),
            })
            .collect()
    }
}

enum Tls {
    None,
    Rustls(Arc<rustls::ClientConfig>),
}

struct Writer {
    name: String,
    pg: tokio_postgres::Config,
    tls: Tls,
    table: String,
    copy_sql: String,
    create_sql: Option<String>,
    types: Vec<Type>,
    client: Option<Client>,
    /// Set while the database is unreachable: no attempt before this instant.
    retry_at: Option<tokio::time::Instant>,
    backoff: Duration,
}

fn pg_config(cfg: &PostgresConfig) -> Result<tokio_postgres::Config> {
    let mut pg = if cfg.url.is_empty() {
        tokio_postgres::Config::new()
    } else {
        tokio_postgres::Config::from_str(&cfg.url).context("invalid url")?
    };
    if !cfg.host.is_empty() {
        pg.host(&cfg.host);
    }
    if let Some(p) = cfg.port {
        pg.port(p);
    }
    if !cfg.database.is_empty() {
        pg.dbname(&cfg.database);
    }
    if !cfg.username.is_empty() {
        pg.user(&cfg.username);
    }
    if !cfg.password.is_empty() {
        pg.password(&cfg.password);
    }
    if pg.get_hosts().is_empty() {
        pg.host("localhost");
    }
    if pg.get_application_name().is_none() {
        pg.application_name("gnmic-rs");
    }
    pg.connect_timeout(Duration::from_secs(10));
    pg.keepalives_idle(Duration::from_secs(60));
    Ok(pg)
}

fn tls_config(t: &PgTlsConfig) -> Result<Tls> {
    let mode = if t.mode.is_empty() {
        "require"
    } else {
        t.mode.as_str()
    };
    if mode == "disable" {
        return Ok(Tls::None);
    }
    if !matches!(mode, "require" | "verify-full") {
        bail!("unknown tls mode {mode:?}, expected disable, require or verify-full");
    }
    let cfg = crate::tls::client_config(&crate::tls::ClientTls {
        skip_verify: mode == "require",
        ca: t.ca_file.as_deref(),
        cert: t.cert_file.as_deref(),
        key: t.key_file.as_deref(),
        alpn_h2: false,
    })?;
    Ok(Tls::Rustls(cfg))
}

impl Writer {
    async fn connect(&mut self) -> Result<()> {
        let client = match &self.tls {
            Tls::None => {
                let (client, conn) = self.pg.connect(NoTls).await?;
                spawn_connection(self.name.clone(), conn);
                client
            }
            Tls::Rustls(cfg) => {
                let mut pg = self.pg.clone();
                pg.ssl_mode(tokio_postgres::config::SslMode::Require);
                let tls = tokio_postgres_rustls::MakeRustlsConnect::new((**cfg).clone());
                let (client, conn) = pg.connect(tls).await?;
                spawn_connection(self.name.clone(), conn);
                client
            }
        };
        if let Some(sql) = &self.create_sql {
            client.batch_execute(sql).await.context("creating table")?;
        }
        tracing::info!(output = %self.name, table = %self.table, "connected to postgres");
        self.client = Some(client);
        Ok(())
    }

    async fn copy(&self, rows: &[Row]) -> Result<u64, tokio_postgres::Error> {
        let client = self.client.as_ref().expect("connected");
        let sink = client.copy_in(&self.copy_sql).await?;
        let writer = BinaryCopyInWriter::new(sink, &self.types);
        tokio::pin!(writer);
        for row in rows {
            writer.as_mut().write_raw(row.iter()).await?;
        }
        writer.finish().await
    }

    /// Writes one batch. Rows rejected by the server (constraint or type
    /// errors) are dropped; on a connection failure the next attempt is
    /// scheduled with exponential backoff and Err asks to keep the rows.
    async fn write_batch(&mut self, rows: &[Row]) -> Result<(), ()> {
        if self.client.is_none()
            && let Err(e) = self.connect().await
        {
            self.fail(&format!("{e:#}"));
            return Err(());
        }
        match tokio::time::timeout(COPY_TIMEOUT, self.copy(rows)).await {
            Ok(Ok(written)) => {
                tracing::debug!(output = %self.name, rows = written, "postgres batch written");
                self.retry_at = None;
                self.backoff = Duration::from_secs(1);
                Ok(())
            }
            Ok(Err(e)) if !retryable(&e) => {
                tracing::error!(output = %self.name, rows = rows.len(), error = %e, "postgres rejected batch, dropping it");
                Ok(())
            }
            Ok(Err(e)) => {
                self.client = None;
                self.fail(&e.to_string());
                Err(())
            }
            Err(_) => {
                self.client = None;
                self.fail(&format!("COPY did not complete within {COPY_TIMEOUT:?}"));
                Err(())
            }
        }
    }

    fn fail(&mut self, error: &str) {
        tracing::warn!(output = %self.name, error, retry_in = ?self.backoff, "postgres unavailable, buffering rows");
        self.retry_at = Some(tokio::time::Instant::now() + self.backoff);
        self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
    }

    fn can_write(&self) -> bool {
        self.retry_at.is_none_or(|t| tokio::time::Instant::now() >= t)
    }
}

fn spawn_connection<S, T>(name: String, conn: tokio_postgres::Connection<S, T>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::warn!(output = %name, error = %e, "postgres connection closed");
        }
    });
}

fn retryable(e: &tokio_postgres::Error) -> bool {
    match e.code() {
        // Connection exception, operator intervention, insufficient resources,
        // transaction rollback (deadlock/serialization).
        Some(code) => ["08", "57", "53", "40"]
            .iter()
            .any(|c| code.code().starts_with(c)),
        None => true,
    }
}

/// Validates the configuration and starts the writer; rows built with the
/// returned RowBuilder are sent to it through `rx`.
pub async fn start(
    name: &str,
    cfg: &PostgresConfig,
    mut rx: mpsc::Receiver<Vec<Row>>,
) -> Result<(RowBuilder, JoinHandle<()>)> {
    if cfg.table.is_empty() {
        bail!("table is required");
    }
    let column_cfgs = if cfg.columns.is_empty() {
        default_columns()
    } else {
        cfg.columns.clone()
    };
    let mut columns = Vec::with_capacity(column_cfgs.len());
    for c in &column_cfgs {
        if c.name.is_empty() {
            bail!("column name is required");
        }
        columns.push(Column {
            name: c.name.clone(),
            sql_type: c.pg_type.clone(),
            ty: parse_type(&c.pg_type).with_context(|| format!("column {:?}", c.name))?,
            source: Source::parse(&c.source, cfg.row_mode).with_context(|| format!("column {:?}", c.name))?,
        });
    }
    let table = quote_ident(&cfg.table);
    let col_list = columns
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let create_sql = cfg.create_table.then(|| {
        let defs = columns
            .iter()
            .map(|c| format!("{} {}", quote_ident(&c.name), c.sql_type))
            .collect::<Vec<_>>()
            .join(", ");
        format!("CREATE TABLE IF NOT EXISTS {table} ({defs})")
    });
    let mut writer = Writer {
        name: name.to_string(),
        pg: pg_config(cfg)?,
        tls: match &cfg.tls {
            Some(t) => tls_config(t).context("tls")?,
            None => Tls::None,
        },
        copy_sql: format!("COPY {table} ({col_list}) FROM STDIN BINARY"),
        table,
        create_sql,
        types: columns.iter().map(|c| c.ty.clone()).collect(),
        client: None,
        retry_at: None,
        backoff: Duration::from_secs(1),
    };
    let rows_builder = RowBuilder {
        columns,
        mode: cfg.row_mode,
        marshaler: Marshaler {
            format: Format::Event,
            indent: None,
            split_events: false,
            override_timestamps: cfg.override_timestamps,
            calculate_latency: false,
            add_target: AddTarget::parse(&cfg.add_target)?,
        },
    };
    let batch_size = cfg.batch_size.unwrap_or(DEFAULT_BATCH).max(1);
    let max_buffered = cfg
        .max_buffered_rows
        .unwrap_or(DEFAULT_MAX_BUFFERED)
        .max(batch_size);
    let flush_every = cfg
        .flush_interval
        .unwrap_or(DEFAULT_FLUSH)
        .max(Duration::from_millis(10));
    tracing::info!(output = name, table = %cfg.table, mode = ?cfg.row_mode, batch_size, "initialized postgres output");

    // Connect eagerly so configuration errors surface at startup; failures
    // are retried by the worker, so a database outage does not block startup.
    if let Err(e) = writer.connect().await {
        tracing::warn!(output = name, error = %format!("{e:#}"), "postgres not reachable yet, will retry");
    }

    let block = cfg.buffer_full == BufferFull::Block;
    let buffer = Arc::new(Buffer::new(max_buffered, block));
    // Receiver: appends to the bounded buffer. With drop-oldest it never stops
    // draining the queue, so a slow database never stalls other outputs; with
    // block it waits for space, which backpressures the subscriptions.
    let receiver = {
        let buffer = buffer.clone();
        let name = writer.name.clone();
        tokio::spawn(async move {
            let mut lost = 0usize;
            while let Some(rows) = rx.recv().await {
                if block && !buffer.wait_for_space(&rx).await {
                    lost += rows.len();
                    continue;
                }
                buffer.push(rows, batch_size);
            }
            if lost > 0 {
                tracing::error!(output = %name, rows = lost, "postgres too slow at shutdown, dropped queued rows");
            }
            buffer.close();
        })
    };
    // Flusher: writes batches, retrying with backoff while the database is down.
    let name = writer.name.clone();
    let handle = tokio::spawn(async move {
        loop {
            let deadline = writer
                .retry_at
                .unwrap_or_else(|| tokio::time::Instant::now() + flush_every);
            if !(buffer.ready(batch_size) && writer.can_write()) {
                tokio::select! {
                    _ = buffer.notify.notified() => {}
                    _ = tokio::time::sleep_until(deadline) => {}
                }
            }
            if buffer.is_closed() {
                break;
            }
            if writer.can_write() {
                flush_all(&mut writer, &buffer, batch_size).await;
            }
            let dropped = buffer.take_dropped();
            if dropped > 0 {
                tracing::error!(output = %writer.name, dropped, max_buffered, "postgres buffer full, dropped oldest rows");
            }
        }
        // Shutdown: a few more attempts within a bounded time, then give up
        // rather than hang (a black-holed host would block on connect).
        let final_flush = async {
            for _ in 0..FINAL_ATTEMPTS {
                writer.retry_at = None;
                if flush_all(&mut writer, &buffer, batch_size).await {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        };
        let _ = tokio::time::timeout(FINAL_FLUSH_TIMEOUT, final_flush).await;
        let left = buffer.len();
        if left > 0 {
            tracing::error!(output = %writer.name, rows = left, "postgres unavailable at shutdown, dropping buffered rows");
        }
        let _ = receiver.await;
        tracing::info!(output = %name, "closed postgres output");
    });
    Ok((rows_builder, handle))
}

/// Writes batches until the buffer is empty (true) or the database fails
/// (false, the batch is put back).
async fn flush_all(writer: &mut Writer, buffer: &Buffer, batch_size: usize) -> bool {
    loop {
        let batch = buffer.take(batch_size);
        if batch.is_empty() {
            return true;
        }
        let failed = writer.write_batch(&batch).await.is_err();
        buffer.failing.store(failed, Ordering::Relaxed);
        if failed {
            buffer.put_back(batch);
            return false;
        }
    }
}

/// Rows waiting to be written, bounded: the oldest are dropped first.
struct Buffer {
    state: std::sync::Mutex<BufferState>,
    /// Wakes the flusher: a batch is ready or the input closed.
    notify: tokio::sync::Notify,
    /// Wakes a blocked receiver: rows were taken.
    space: tokio::sync::Notify,
    /// The last write attempt failed (database unreachable).
    failing: std::sync::atomic::AtomicBool,
    max: usize,
    block: bool,
}

#[derive(Default)]
struct BufferState {
    rows: VecDeque<Row>,
    dropped: u64,
    closed: bool,
}

impl Buffer {
    fn new(max: usize, block: bool) -> Self {
        Buffer {
            state: Default::default(),
            notify: tokio::sync::Notify::new(),
            space: tokio::sync::Notify::new(),
            failing: Default::default(),
            max,
            block,
        }
    }

    /// Waits until the buffer has room. Once the input has closed
    /// (shutdown), waits at most FINAL_FLUSH_TIMEOUT for the writer to make
    /// room, then returns false so shutdown is never held up.
    async fn wait_for_space(&self, rx: &mpsc::Receiver<Vec<Row>>) -> bool {
        let mut closed_at = None;
        loop {
            let space = self.space.notified();
            if self.len() < self.max {
                return true;
            }
            if rx.is_closed() {
                let since = *closed_at.get_or_insert_with(tokio::time::Instant::now);
                let failing = self.failing.load(Ordering::Relaxed);
                if failing || since.elapsed() >= FINAL_FLUSH_TIMEOUT {
                    return false;
                }
            }
            tokio::select! {
                _ = space => {}
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BufferState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn trim(&self, st: &mut BufferState) {
        if !self.block && st.rows.len() > self.max {
            let excess = st.rows.len() - self.max;
            st.rows.drain(..excess);
            st.dropped += excess as u64;
        }
    }

    fn push(&self, rows: Vec<Row>, batch_size: usize) {
        let mut st = self.lock();
        st.rows.extend(rows);
        self.trim(&mut st);
        if st.rows.len() >= batch_size {
            self.notify.notify_one();
        }
    }

    fn put_back(&self, batch: Vec<Row>) {
        let mut st = self.lock();
        for row in batch.into_iter().rev() {
            st.rows.push_front(row);
        }
        self.trim(&mut st);
    }

    fn take(&self, n: usize) -> Vec<Row> {
        let mut st = self.lock();
        let n = n.min(st.rows.len());
        let batch = st.rows.drain(..n).collect();
        self.space.notify_one();
        batch
    }

    fn close(&self) {
        self.lock().closed = true;
        self.notify.notify_one();
    }

    fn ready(&self, batch_size: usize) -> bool {
        let st = self.lock();
        st.closed || st.rows.len() >= batch_size
    }

    fn is_closed(&self) -> bool {
        self.lock().closed
    }

    fn len(&self) -> usize {
        self.lock().rows.len()
    }

    fn take_dropped(&self) -> u64 {
        std::mem::take(&mut self.lock().dropped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn event() -> Event {
        Event {
            name: "sub1".into(),
            timestamp: 1_700_000_000_000_000_000,
            tags: BTreeMap::from([("source".to_string(), "r1".to_string())]),
            values: BTreeMap::from([
                ("/a/in-octets".to_string(), Value::from("12345678901")),
                ("/a/oper-status".to_string(), Value::from("UP")),
            ]),
            deletes: vec![],
        }
    }

    fn marshaler() -> Marshaler {
        Marshaler {
            format: Format::Event,
            indent: None,
            split_events: false,
            override_timestamps: false,
            calculate_latency: false,
            add_target: None,
        }
    }

    fn builder(mode: RowMode, cols: &[(&str, &str, &str)]) -> RowBuilder {
        RowBuilder {
            mode,
            marshaler: marshaler(),
            columns: cols
                .iter()
                .map(|(n, t, s)| Column {
                    name: n.to_string(),
                    sql_type: t.to_string(),
                    ty: parse_type(t).unwrap(),
                    source: Source::parse(s, mode).unwrap(),
                })
                .collect(),
        }
    }

    #[test]
    fn value_rows() {
        let b = RowBuilder {
            mode: RowMode::Value,
            marshaler: marshaler(),
            columns: default_columns()
                .iter()
                .map(|c| Column {
                    name: c.name.clone(),
                    sql_type: c.pg_type.clone(),
                    ty: parse_type(&c.pg_type).unwrap(),
                    source: Source::parse(&c.source, RowMode::Value).unwrap(),
                })
                .collect(),
        };
        let mut rows = Vec::new();
        b.event_rows(&[event(), Event::default()], 0, &mut rows);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Cell::TsTz(nanos_to_utc(1_700_000_000_000_000_000)));
        assert_eq!(rows[0][1], Cell::Text("r1".into()));
        assert_eq!(rows[0][2], Cell::Text("sub1".into()));
        assert_eq!(rows[0][3], Cell::Text("/a/in-octets".into()));
        assert_eq!(rows[0][4], Cell::Json(Value::from("12345678901")));
    }

    #[test]
    fn event_rows_and_coercion() {
        let b = builder(
            RowMode::Event,
            &[
                ("ts", "bigint", "timestamp"),
                ("octets", "bigint", "value:in-octets"),
                ("octets_f", "double precision", "value:/a/in-octets"),
                ("status", "text", "value:oper-status"),
                ("missing", "integer", "value:nope"),
                ("tag", "varchar", "tag:nope"),
                ("all", "jsonb", "values"),
            ],
        );
        let mut rows = Vec::new();
        b.event_rows(&[event()], 0, &mut rows);
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r[0], Cell::I64(1_700_000_000_000_000_000));
        assert_eq!(r[1], Cell::I64(12_345_678_901));
        assert_eq!(r[2], Cell::F64(12_345_678_901.0));
        assert_eq!(r[3], Cell::Text("UP".into()));
        assert_eq!((&r[4], &r[5]), (&Cell::Null, &Cell::Null));
        assert!(matches!(&r[6], Cell::Json(Value::Object(m)) if m.len() == 2));
    }

    #[test]
    fn coercions() {
        assert_eq!(coerce(&Value::from("x"), &Type::INT8), Cell::Null);
        assert_eq!(coerce(&Value::from(3.0), &Type::INT4), Cell::I32(3));
        assert_eq!(coerce(&Value::from(3.5), &Type::INT4), Cell::Null);
        assert_eq!(coerce(&Value::from(70000), &Type::INT2), Cell::Null);
        assert_eq!(coerce(&Value::from("true"), &Type::BOOL), Cell::Bool(true));
        assert_eq!(coerce(&Value::from(1.5), &Type::TEXT), Cell::Text("1.5".into()));
        assert_eq!(
            coerce(&serde_json::json!({"a":1}), &Type::TEXT),
            Cell::Text(r#"{"a":1}"#.into())
        );
        assert!(matches!(
            coerce(&Value::from("2024-01-02T03:04:05Z"), &Type::TIMESTAMPTZ),
            Cell::TsTz(_)
        ));
        assert_eq!(coerce(&Value::Null, &Type::JSONB), Cell::Null);
    }

    #[test]
    fn validation() {
        assert!(Source::parse("path", RowMode::Event).is_err());
        assert!(Source::parse("value:x", RowMode::Value).is_err());
        assert!(Source::parse("bogus", RowMode::Value).is_err());
        assert!(parse_type("numeric").is_err());
        assert_eq!(parse_type("Double  Precision").unwrap(), Type::FLOAT8);
        assert_eq!(quote_ident("tele.gnmi\"x"), "\"tele\".\"gnmi\"\"x\"");
        let c: PostgresConfig =
            serde_yaml::from_str("{table: t, columns: [{name: a, type: text, source: name}]}").unwrap();
        assert_eq!(c.row_mode, RowMode::Value);
        assert!(serde_yaml::from_str::<PostgresConfig>("{table: t, tabel: x}").is_err());
    }

    #[test]
    fn buffer_is_bounded_and_ordered() {
        let row = |i: i64| vec![Cell::I64(i)];
        let b = Buffer::new(5, false);
        b.push((0..4).map(row).collect(), 10);
        b.push((4..8).map(row).collect(), 10);
        assert_eq!(b.len(), 5);
        assert_eq!(b.take_dropped(), 3);
        let batch = b.take(2);
        assert_eq!(batch, vec![row(3), row(4)]);
        // A failed batch goes back in front, keeping order.
        b.put_back(batch);
        assert_eq!(b.take(10), (3..8).map(row).collect::<Vec<_>>());
        assert!(!b.ready(1));
        b.close();
        assert!(b.ready(1) && b.is_closed());
    }

    #[tokio::test]
    async fn block_mode_waits_for_space_instead_of_dropping() {
        let row = |i: i64| vec![Cell::I64(i)];
        let b = Arc::new(Buffer::new(2, true));
        b.push((0..3).map(row).collect(), 10);
        assert_eq!((b.len(), b.take_dropped()), (3, 0), "never drops");
        let (tx, rx) = mpsc::channel::<Vec<Row>>(1);
        let waiter = tokio::spawn({
            let b = b.clone();
            async move { b.wait_for_space(&rx).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());
        b.take(2);
        assert!(waiter.await.unwrap());
        drop(tx);
    }
}
