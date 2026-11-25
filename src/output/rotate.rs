//! Size-based file rotation compatible with gnmic's lumberjack setup: backups
//! are named `<name>-<UTC 2006-01-02T15-04-05.000><ext>`, optionally gzipped,
//! and pruned by count and age.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use chrono::{NaiveDateTime, Utc};
use serde::Deserialize;

const MB: u64 = 1024 * 1024;
const TIME_FORMAT: &str = "%Y-%m-%dT%H-%M-%S%.3f";

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct RotationConfig {
    /// Megabytes before rotating (default 100).
    pub max_size: u64,
    /// Rotated files to keep (default 3).
    pub max_backups: usize,
    /// Days to keep rotated files (default 30).
    pub max_age: u64,
    /// Gzip rotated files.
    pub compress: bool,
}

pub struct RotatingFile {
    path: PathBuf,
    max_size: u64,
    max_backups: usize,
    max_age: Duration,
    compress: bool,
    file: Option<BufWriter<File>>,
    size: u64,
    mills: Vec<std::thread::JoinHandle<()>>,
}

// Serializes background cleanup so two rotations never prune concurrently.
static MILL: Mutex<()> = Mutex::new(());

impl RotatingFile {
    pub fn open(path: &str, cfg: &RotationConfig) -> Result<Self> {
        let or = |v: u64, d: u64| if v == 0 { d } else { v };
        let mut rf = RotatingFile {
            path: PathBuf::from(path),
            max_size: or(cfg.max_size, 100) * MB,
            max_backups: if cfg.max_backups == 0 { 3 } else { cfg.max_backups },
            max_age: Duration::from_secs(or(cfg.max_age, 30) * 86400),
            compress: cfg.compress,
            file: None,
            size: 0,
            mills: Vec::new(),
        };
        rf.open_existing_or_new(0)
            .with_context(|| format!("opening {path}"))?;
        Ok(rf)
    }

    fn open_existing_or_new(&mut self, write_len: u64) -> io::Result<()> {
        match fs::metadata(&self.path) {
            Ok(m) if m.len() + write_len <= self.max_size => {
                let f = OpenOptions::new().append(true).open(&self.path)?;
                self.size = m.len();
                self.file = Some(BufWriter::with_capacity(256 * 1024, f));
                Ok(())
            }
            Ok(_) => self.rotate(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => self.open_new(),
            Err(e) => Err(e),
        }
    }

    fn open_new(&mut self) -> io::Result<()> {
        if let Some(dir) = self.path.parent().filter(|d| !d.as_os_str().is_empty()) {
            fs::create_dir_all(dir)?;
        }
        if self.path.exists() {
            // Names have millisecond resolution: never overwrite a backup.
            let backup = loop {
                let b = backup_name(&self.path, Utc::now().naive_utc());
                if !b.exists() {
                    break b;
                }
                std::thread::sleep(Duration::from_millis(1));
            };
            fs::rename(&self.path, backup)?;
        }
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.file = Some(BufWriter::with_capacity(256 * 1024, f));
        self.size = 0;
        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        if let Some(mut f) = self.file.take() {
            f.flush()?;
        }
        self.open_new()?;
        let (path, max_backups, max_age, compress) =
            (self.path.clone(), self.max_backups, self.max_age, self.compress);
        self.mills.retain(|m| !m.is_finished());
        self.mills.push(std::thread::spawn(move || {
            let _guard = MILL.lock().unwrap_or_else(|e| e.into_inner());
            if let Err(e) = mill(&path, max_backups, max_age, compress) {
                tracing::warn!(file = %path.display(), error = %e, "rotated file cleanup failed");
            }
        }));
        Ok(())
    }

    /// Writes whole records, rotating first if they would exceed max-size,
    /// so a record never spans two files.
    pub fn write_chunk(&mut self, chunk: &[u8]) -> io::Result<()> {
        let len = chunk.len() as u64;
        if self.file.is_none() {
            self.open_existing_or_new(len)?;
        } else if self.size > 0 && self.size + len > self.max_size {
            self.rotate()?;
        }
        let f = self.file.as_mut().expect("opened above");
        f.write_all(chunk)?;
        self.size += len;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        match &mut self.file {
            Some(f) => f.flush(),
            None => Ok(()),
        }
    }
}

impl Drop for RotatingFile {
    /// Lets an in-progress compression finish before the process exits.
    fn drop(&mut self) {
        if let Some(f) = &mut self.file {
            let _ = f.flush();
        }
        for m in self.mills.drain(..) {
            let _ = m.join();
        }
    }
}

fn split_name(path: &Path) -> (String, String) {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    match name.rfind('.') {
        Some(i) => (name[..i].to_string(), name[i..].to_string()),
        None => (name, String::new()),
    }
}

fn backup_name(path: &Path, t: NaiveDateTime) -> PathBuf {
    let (prefix, ext) = split_name(path);
    path.with_file_name(format!("{prefix}-{}{ext}", t.format(TIME_FORMAT)))
}

/// Prunes backups beyond max_backups or older than max_age and gzips the rest.
fn mill(path: &Path, max_backups: usize, max_age: Duration, compress: bool) -> io::Result<()> {
    let dir = match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let (prefix, ext) = split_name(path);
    let head = format!("{prefix}-");
    let gz_ext = format!("{ext}.gz");
    let tmp_ext = format!("{ext}.gz.tmp");
    let mut backups: Vec<(NaiveDateTime, PathBuf, bool)> = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(rest) = name.strip_prefix(&head) else {
            continue;
        };
        if rest.ends_with(&tmp_ext) {
            // Left over by an interrupted compression.
            fs::remove_file(entry.path())?;
            continue;
        }
        let (stamp, gz) = if let Some(s) = rest.strip_suffix(&gz_ext) {
            (s, true)
        } else if let Some(s) = rest.strip_suffix(ext.as_str()) {
            (s, false)
        } else {
            continue;
        };
        if let Ok(t) = NaiveDateTime::parse_from_str(stamp, TIME_FORMAT) {
            backups.push((t, entry.path(), gz));
        }
    }
    backups.sort_by_key(|b| std::cmp::Reverse(b.0));

    let cutoff = SystemTime::now()
        .checked_sub(max_age)
        .map(chrono::DateTime::<Utc>::from)
        .map(|t| t.naive_utc());
    let mut kept_names: Vec<NaiveDateTime> = Vec::new();
    for (t, p, gz) in backups {
        // A backup and its .gz twin count once (compression in progress).
        let counted = kept_names.contains(&t);
        let too_many = !counted && kept_names.len() >= max_backups;
        let too_old = cutoff.is_some_and(|c| t < c);
        if too_many || too_old {
            fs::remove_file(&p)?;
            continue;
        }
        if !counted {
            kept_names.push(t);
        }
        if compress && !gz {
            compress_file(&p)?;
        }
    }
    Ok(())
}

/// Gzips `src` into `src.gz` via a temporary file, so a `.gz` is never partial.
fn compress_file(src: &Path) -> io::Result<()> {
    let with_ext = |ext: &str| {
        let mut n = src.as_os_str().to_owned();
        n.push(ext);
        PathBuf::from(n)
    };
    let (tmp, dst) = (with_ext(".gz.tmp"), with_ext(".gz"));
    let mut input = File::open(src)?;
    let out = File::create(&tmp)?;
    let mut gz = flate2::write::GzEncoder::new(BufWriter::new(out), flate2::Compression::default());
    io::copy(&mut input, &mut gz)?;
    gz.finish()?
        .into_inner()
        .map_err(|e| e.into_error())?
        .sync_all()?;
    fs::rename(&tmp, &dst)?;
    fs::remove_file(src)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn rotates_compresses_and_prunes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.log");
        let mut rf = RotatingFile::open(
            path.to_str().unwrap(),
            &RotationConfig {
                max_size: 1,
                max_backups: 2,
                max_age: 0,
                compress: true,
            },
        )
        .unwrap();
        let mut rec = vec![b'x'; 400 * 1024];
        rec.push(b'\n');
        for _ in 0..12 {
            rf.write_chunk(&rec).unwrap();
        }
        // Dropping joins the background cleanup threads.
        drop(rf);
        let mut names: Vec<String> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names.len(), 3, "{names:?}");
        assert!(
            names.iter().filter(|n| n.ends_with(".log.gz")).count() == 2,
            "{names:?}"
        );
        let current = fs::metadata(&path).unwrap().len();
        assert!(current <= MB, "{current}");
        // Every backup holds whole records.
        let gz = dir
            .path()
            .join(names.iter().find(|n| n.ends_with(".gz")).unwrap());
        let mut s = Vec::new();
        flate2::read::GzDecoder::new(File::open(gz).unwrap())
            .read_to_end(&mut s)
            .unwrap();
        assert_eq!(s.len() % rec.len(), 0);
        assert!(s.len() as u64 <= MB);
    }

    #[test]
    fn appends_to_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a");
        fs::write(&path, b"old\n").unwrap();
        let mut rf = RotatingFile::open(path.to_str().unwrap(), &RotationConfig::default()).unwrap();
        rf.write_chunk(b"new\n").unwrap();
        rf.flush().unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"old\nnew\n");
        assert_eq!(split_name(Path::new("/x/a.b.log")), ("a.b".into(), ".log".into()));
    }
}
