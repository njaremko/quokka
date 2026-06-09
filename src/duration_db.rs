use rustc_hash::{FxHashMap, FxHasher};
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::result::{FailureClass, TestIdentity};

/// Max recent samples retained per test for percentile estimation.
const MAX_SAMPLES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Environment {
    Local,
    Remote,
}

impl Environment {
    const ALL: [Self; 2] = [Environment::Local, Environment::Remote];

    fn to_index(self) -> usize {
        match self {
            Environment::Local => 0,
            Environment::Remote => 1,
        }
    }
}

/// A test's duration estimate. `Unseen` (no history) is a named state, not a
/// missing value, and is sorted to a defined middle position by callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurationEstimate {
    Measured { p50_ms: u64, p95_ms: u64 },
    Unseen,
}

impl DurationEstimate {
    pub fn weight_ms(self, unseen_default_ms: u64) -> u64 {
        match self {
            DurationEstimate::Measured { p95_ms, .. } => p95_ms,
            DurationEstimate::Unseen => unseen_default_ms,
        }
    }

    pub fn p50_ms(self, unseen_default_ms: u64) -> u64 {
        match self {
            DurationEstimate::Measured { p50_ms, .. } => p50_ms,
            DurationEstimate::Unseen => unseen_default_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DurationSample {
    pub timestamp_ms: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Default, Clone)]
pub struct PerfRecord {
    pub samples: Vec<DurationSample>,
}

impl PerfRecord {
    fn record(&mut self, timestamp_ms: u64, duration_ms: u64) {
        self.samples.push(DurationSample {
            timestamp_ms,
            duration_ms,
        });
        self.sort_and_truncate();
    }

    fn combine(&mut self, other: &Self) {
        self.samples.extend_from_slice(&other.samples);
        self.sort_and_truncate();
    }

    fn sort_and_truncate(&mut self) {
        // Sort descending by timestamp, then descending by duration (as a tiebreaker)
        self.samples.sort_unstable_by(|a, b| b.cmp(a));
        self.samples.truncate(MAX_SAMPLES);
    }
}

#[derive(Debug, Default, Clone)]
pub struct FlakeRecord {
    pub runs: u64,
    pub failures: u64,
    pub last_failure_timestamp_ms: Option<u64>,
    pub last_failure_class: Option<FailureClass>,
}

impl FlakeRecord {
    fn record(&mut self, failed: bool, class: Option<FailureClass>, timestamp_ms: u64) {
        self.runs += 1;
        if failed {
            self.failures += 1;
            if self.last_failure_timestamp_ms.unwrap_or(0) <= timestamp_ms {
                self.last_failure_timestamp_ms = Some(timestamp_ms);
                self.last_failure_class = class;
            }
        }
    }

    fn combine(&mut self, other: &Self) {
        self.runs += other.runs;
        self.failures += other.failures;
        if let Some(other_ts) = other.last_failure_timestamp_ms {
            if self.last_failure_timestamp_ms.unwrap_or(0) <= other_ts {
                self.last_failure_timestamp_ms = Some(other_ts);
                self.last_failure_class = other.last_failure_class;
            }
        }
    }
}

fn failure_class_to_u8(class: FailureClass) -> u8 {
    match class {
        FailureClass::Fail => 1,
        FailureClass::Fatal => 2,
        FailureClass::Timeout => 3,
        FailureClass::Infra => 4,
    }
}

fn failure_class_from_u8(val: u8) -> Option<FailureClass> {
    match val {
        1 => Some(FailureClass::Fail),
        2 => Some(FailureClass::Fatal),
        3 => Some(FailureClass::Timeout),
        4 => Some(FailureClass::Infra),
        _ => None,
    }
}

fn read_string<R: Read>(reader: &mut R) -> Option<String> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).ok()?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > 10_000 {
        return None;
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).ok()?;
    String::from_utf8(buf).ok()
}

fn write_string<W: Write>(writer: &mut W, s: &str) -> std::io::Result<()> {
    writer.write_all(&(s.len() as u32).to_le_bytes())?;
    writer.write_all(s.as_bytes())?;
    Ok(())
}

fn read_test_identity<R: Read>(reader: &mut R) -> Option<TestIdentity> {
    let target = read_string(reader)?;
    let name = read_string(reader)?;
    let variant_str = read_string(reader)?;
    let variant = crate::variant::Variant::parse(&variant_str);
    Some(TestIdentity { target, name, variant })
}

fn write_test_identity<W: Write>(writer: &mut W, tid: &TestIdentity) -> std::io::Result<()> {
    write_string(writer, &tid.target)?;
    write_string(writer, &tid.name)?;
    let variant_str = tid.variant.identity().unwrap_or_else(|| "default".to_owned());
    write_string(writer, &variant_str)?;
    Ok(())
}

fn hash_key(test_id: &TestIdentity) -> u64 {
    let mut hasher = FxHasher::default();
    crate::result::project_dir_key().hash(&mut hasher);
    test_id.hash(&mut hasher);
    hasher.finish()
}

/// The loaded metadata store plus in-memory accumulators for this run.
pub struct DurationDb {
    dir: PathBuf,
    perf: [FxHashMap<u64, PerfRecord>; 2],
    flake: [FxHashMap<u64, FlakeRecord>; 2],
    names: FxHashMap<u64, TestIdentity>,
    new_perf_samples: Vec<(Environment, u64, u64, u64)>, // env, key, timestamp, duration
    new_flake_records: Vec<(Environment, u64, bool, Option<FailureClass>, u64)>, // env, key, failed, class, timestamp
    new_names: Vec<(u64, TestIdentity)>,
}

impl DurationDb {
    /// Load the store from `dir` (creating empty state if absent or corrupt —
    /// advisory data, so a parse failure starts fresh rather than aborting).
    pub fn load(dir: PathBuf) -> Self {
        let mut perf = [FxHashMap::default(), FxHashMap::default()];
        let mut flake = [FxHashMap::default(), FxHashMap::default()];
        let mut names = FxHashMap::default();

        if let Ok(file) = std::fs::File::open(dir.join("perf.bin")) {
            let mut reader = BufReader::new(file);
            let mut magic = [0u8; 4];
            if reader.read_exact(&mut magic).is_ok() && &magic == b"PRF1" {
                for i in 0..2 {
                    let mut len_buf = [0u8; 4];
                    if reader.read_exact(&mut len_buf).is_ok() {
                        let len = u32::from_le_bytes(len_buf);
                        perf[i].reserve((len as usize).min(1_000_000));
                        for _ in 0..len {
                            let mut k_buf = [0u8; 8];
                            let mut samples_len = [0u8; 1];
                            if reader.read_exact(&mut k_buf).is_ok() && reader.read_exact(&mut samples_len).is_ok() {
                                let k = u64::from_le_bytes(k_buf);
                                let s_len = samples_len[0];
                                let mut samples = Vec::with_capacity(s_len as usize);
                                for _ in 0..s_len {
                                    let mut ts_buf = [0u8; 8];
                                    let mut dur_buf = [0u8; 8];
                                    if reader.read_exact(&mut ts_buf).is_ok() && reader.read_exact(&mut dur_buf).is_ok() {
                                        samples.push(DurationSample {
                                            timestamp_ms: u64::from_le_bytes(ts_buf),
                                            duration_ms: u64::from_le_bytes(dur_buf),
                                        });
                                    }
                                }
                                perf[i].insert(k, PerfRecord { samples });
                            }
                        }
                    }
                }
            }
        }

        if let Ok(mut file) = std::fs::File::open(dir.join("perf.log")) {
            let mut buf = Vec::new();
            if file.read_to_end(&mut buf).is_ok() {
                let mut i = 0;
                while i + 25 <= buf.len() {
                    let env_idx = buf[i] as usize;
                    let k = u64::from_le_bytes(buf[i+1..i+9].try_into().unwrap());
                    let ts = u64::from_le_bytes(buf[i+9..i+17].try_into().unwrap());
                    let dur = u64::from_le_bytes(buf[i+17..i+25].try_into().unwrap());
                    if env_idx < 2 {
                        perf[env_idx].entry(k).or_default().record(ts, dur);
                    }
                    i += 25;
                }
            }
        }

        if let Ok(file) = std::fs::File::open(dir.join("flake.bin")) {
            let mut reader = BufReader::new(file);
            let mut magic = [0u8; 4];
            if reader.read_exact(&mut magic).is_ok() && &magic == b"FLK1" {
                for i in 0..2 {
                    let mut len_buf = [0u8; 4];
                    if reader.read_exact(&mut len_buf).is_ok() {
                        let len = u32::from_le_bytes(len_buf);
                        flake[i].reserve((len as usize).min(1_000_000));
                        for _ in 0..len {
                            let mut k_buf = [0u8; 8];
                            let mut runs_buf = [0u8; 8];
                            let mut fail_buf = [0u8; 8];
                            let mut ts_buf = [0u8; 8];
                            let mut class_buf = [0u8; 1];
                            if reader.read_exact(&mut k_buf).is_ok() &&
                               reader.read_exact(&mut runs_buf).is_ok() &&
                               reader.read_exact(&mut fail_buf).is_ok() &&
                               reader.read_exact(&mut ts_buf).is_ok() &&
                               reader.read_exact(&mut class_buf).is_ok() {
                                let k = u64::from_le_bytes(k_buf);
                                let ts = u64::from_le_bytes(ts_buf);
                                flake[i].insert(k, FlakeRecord {
                                    runs: u64::from_le_bytes(runs_buf),
                                    failures: u64::from_le_bytes(fail_buf),
                                    last_failure_timestamp_ms: if ts == u64::MAX { None } else { Some(ts) },
                                    last_failure_class: failure_class_from_u8(class_buf[0]),
                                });
                            }
                        }
                    }
                }
            }
        }

        if let Ok(mut file) = std::fs::File::open(dir.join("flake.log")) {
            let mut buf = Vec::new();
            if file.read_to_end(&mut buf).is_ok() {
                let mut i = 0;
                while i + 19 <= buf.len() {
                    let env_idx = buf[i] as usize;
                    let k = u64::from_le_bytes(buf[i+1..i+9].try_into().unwrap());
                    let failed = buf[i+9] != 0;
                    let class = failure_class_from_u8(buf[i+10]);
                    let ts = u64::from_le_bytes(buf[i+11..i+19].try_into().unwrap());
                    if env_idx < 2 {
                        flake[env_idx].entry(k).or_default().record(failed, class, ts);
                    }
                    i += 19;
                }
            }
        }

        if let Ok(file) = std::fs::File::open(dir.join("names.bin")) {
            let mut reader = BufReader::new(file);
            let mut magic = [0u8; 4];
            if reader.read_exact(&mut magic).is_ok() && &magic == b"NMS1" {
                let mut len_buf = [0u8; 4];
                if reader.read_exact(&mut len_buf).is_ok() {
                    let len = u32::from_le_bytes(len_buf);
                    names.reserve((len as usize).min(1_000_000));
                    for _ in 0..len {
                        let mut k_buf = [0u8; 8];
                        if reader.read_exact(&mut k_buf).is_ok() {
                            let k = u64::from_le_bytes(k_buf);
                            if let Some(tid) = read_test_identity(&mut reader) {
                                names.insert(k, tid);
                            }
                        }
                    }
                }
            }
        }

        if let Ok(mut file) = std::fs::File::open(dir.join("names.log")) {
            let mut buf = Vec::new();
            if file.read_to_end(&mut buf).is_ok() {
                let mut reader = std::io::Cursor::new(buf);
                while reader.position() < reader.get_ref().len() as u64 {
                    let mut k_buf = [0u8; 8];
                    if reader.read_exact(&mut k_buf).is_ok() {
                        let k = u64::from_le_bytes(k_buf);
                        if let Some(tid) = read_test_identity(&mut reader) {
                            names.insert(k, tid);
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        Self {
            dir,
            perf,
            flake,
            names,
            new_perf_samples: Vec::new(),
            new_flake_records: Vec::new(),
            new_names: Vec::new(),
        }
    }

    /// An empty in-memory store that never persists.
    pub fn ephemeral() -> Self {
        Self {
            dir: PathBuf::new(),
            perf: [FxHashMap::default(), FxHashMap::default()],
            flake: [FxHashMap::default(), FxHashMap::default()],
            names: FxHashMap::default(),
            new_perf_samples: Vec::new(),
            new_flake_records: Vec::new(),
            new_names: Vec::new(),
        }
    }

    pub fn all_keys(&self) -> Vec<u64> {
        let mut keys: Vec<u64> = self.perf.iter().flat_map(|m| m.keys().copied()).collect();
        for map in &self.flake {
            keys.extend(map.keys().copied());
        }
        keys.sort_unstable();
        keys.dedup();
        keys
    }

    pub fn get_name(&self, key: u64) -> Option<&TestIdentity> {
        self.names.get(&key)
    }

    pub fn get_perf_samples(&self, env: Option<Environment>, key: u64) -> Vec<DurationSample> {
        let mut combined = PerfRecord::default();
        if let Some(e) = env {
            if let Some(record) = self.perf[e.to_index()].get(&key) {
                combined.combine(record);
            }
        } else {
            for e in Environment::ALL {
                if let Some(record) = self.perf[e.to_index()].get(&key) {
                    combined.combine(record);
                }
            }
        }
        combined.samples
    }

    pub fn get_flake_record(&self, env: Option<Environment>, key: u64) -> Option<FlakeRecord> {
        let mut combined = FlakeRecord::default();
        let mut found = false;
        if let Some(e) = env {
            if let Some(record) = self.flake[e.to_index()].get(&key) {
                combined.combine(record);
                found = true;
            }
        } else {
            for e in Environment::ALL {
                if let Some(record) = self.flake[e.to_index()].get(&key) {
                    combined.combine(record);
                    found = true;
                }
            }
        }
        if found {
            Some(combined)
        } else {
            None
        }
    }

    /// The duration estimate for a test. If environment is known, queries it,
    /// otherwise aggregates across environments.
    pub fn estimate(&self, env: Option<Environment>, test_id: &TestIdentity) -> DurationEstimate {
        let key = hash_key(test_id);
        self.estimate_by_key(env, key)
    }

    pub fn estimate_by_key(&self, env: Option<Environment>, key: u64) -> DurationEstimate {
        let mut combined = PerfRecord::default();
        if let Some(e) = env {
            if let Some(record) = self.perf[e.to_index()].get(&key) {
                combined.combine(record);
            }
        } else {
            for e in Environment::ALL {
                if let Some(record) = self.perf[e.to_index()].get(&key) {
                    combined.combine(record);
                }
            }
        }

        if combined.samples.is_empty() {
            DurationEstimate::Unseen
        } else {
            let ms_samples: Vec<u64> = combined.samples.iter().map(|s| s.duration_ms).collect();
            let p50 = percentile(&ms_samples, 50);
            let p95 = percentile(&ms_samples, 95);
            DurationEstimate::Measured { p50_ms: p50, p95_ms: p95 }
        }
    }

    /// The flake record for a test. Aggregates across environments if env is None.
    pub fn flake(&self, env: Option<Environment>, test_id: &TestIdentity) -> Option<FlakeRecord> {
        let key = hash_key(test_id);
        let mut combined = FlakeRecord::default();
        let mut found = false;

        if let Some(e) = env {
            if let Some(record) = self.flake[e.to_index()].get(&key) {
                combined.combine(record);
                found = true;
            }
        } else {
            for e in Environment::ALL {
                if let Some(record) = self.flake[e.to_index()].get(&key) {
                    combined.combine(record);
                    found = true;
                }
            }
        }

        if found {
            Some(combined)
        } else {
            None
        }
    }

    pub fn record(
        &mut self,
        env: Environment,
        test_id: &TestIdentity,
        duration: Duration,
        failed: bool,
        failure_class: Option<FailureClass>,
    ) {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        self.record_at(env, test_id, duration, failed, failure_class, timestamp_ms);
    }

    pub fn record_at(
        &mut self,
        env: Environment,
        test_id: &TestIdentity,
        duration: Duration,
        failed: bool,
        failure_class: Option<FailureClass>,
        timestamp_ms: u64,
    ) {
        let key = hash_key(test_id);
        if !self.names.contains_key(&key) {
            self.names.insert(key, test_id.clone());
            self.new_names.push((key, test_id.clone()));
        }
        if !failed {
            let perf = self.perf[env.to_index()].entry(key).or_default();
            perf.record(timestamp_ms, duration.as_millis() as u64);
            self.new_perf_samples.push((env, key, timestamp_ms, duration.as_millis() as u64));
        }
        let flake = self.flake[env.to_index()].entry(key).or_default();
        flake.record(failed, failure_class, timestamp_ms);
        self.new_flake_records.push((env, key, failed, failure_class, timestamp_ms));
    }

    pub fn combine(&mut self, mut other: Self) {
        for (i, tests) in other.perf.into_iter().enumerate() {
            for (k, v) in tests {
                self.perf[i].entry(k).or_default().combine(&v);
            }
        }
        for (i, tests) in other.flake.into_iter().enumerate() {
            for (k, v) in tests {
                self.flake[i].entry(k).or_default().combine(&v);
            }
        }
        for (k, v) in other.names {
            if !self.names.contains_key(&k) {
                self.names.insert(k, v.clone());
                self.new_names.push((k, v));
            }
        }
        self.new_perf_samples.append(&mut other.new_perf_samples);
        self.new_flake_records.append(&mut other.new_flake_records);
    }

    /// Persist accumulated state (no-op for an ephemeral store). Best-effort.
    pub fn flush(&mut self) -> std::io::Result<()> {
        if self.dir.as_os_str().is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(&self.dir)?;

        let lock_path = self.dir.join("db.lock");
        let mut lock_acquired = false;
        for _ in 0..200 {
            match std::fs::OpenOptions::new().write(true).create_new(true).open(&lock_path) {
                Ok(_) => { lock_acquired = true; break; }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    if let Ok(meta) = std::fs::metadata(&lock_path) {
                        if let Ok(mod_time) = meta.modified() {
                            if mod_time.elapsed().unwrap_or(Duration::ZERO) > Duration::from_secs(10) {
                                let _ = std::fs::remove_file(&lock_path);
                            }
                        }
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }

        let perf_log_path = self.dir.join("perf.log");
        if !self.new_perf_samples.is_empty() {
            if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&perf_log_path) {
                let mut buf = Vec::with_capacity(self.new_perf_samples.len() * 25);
                for (env, key, ts, dur) in &self.new_perf_samples {
                    buf.push(env.to_index() as u8);
                    buf.extend_from_slice(&key.to_le_bytes());
                    buf.extend_from_slice(&ts.to_le_bytes());
                    buf.extend_from_slice(&dur.to_le_bytes());
                }
                let _ = file.write_all(&buf);
                let _ = file.sync_data();
            }
            self.new_perf_samples.clear();
        }

        let flake_log_path = self.dir.join("flake.log");
        if !self.new_flake_records.is_empty() {
            if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&flake_log_path) {
                let mut buf = Vec::with_capacity(self.new_flake_records.len() * 19);
                for (env, key, failed, class, ts) in &self.new_flake_records {
                    buf.push(env.to_index() as u8);
                    buf.extend_from_slice(&key.to_le_bytes());
                    buf.push(*failed as u8);
                    buf.push(class.map(failure_class_to_u8).unwrap_or(0));
                    buf.extend_from_slice(&ts.to_le_bytes());
                }
                let _ = file.write_all(&buf);
                let _ = file.sync_data();
            }
            self.new_flake_records.clear();
        }

        let names_log_path = self.dir.join("names.log");
        if !self.new_names.is_empty() {
            if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&names_log_path) {
                let mut buf = Vec::new();
                for (key, tid) in &self.new_names {
                    buf.extend_from_slice(&key.to_le_bytes());
                    let _ = write_test_identity(&mut buf, tid);
                }
                let _ = file.write_all(&buf);
                let _ = file.sync_data();
            }
            self.new_names.clear();
        }

        let perf_size = std::fs::metadata(&perf_log_path).map(|m| m.len()).unwrap_or(0);
        let flake_size = std::fs::metadata(&flake_log_path).map(|m| m.len()).unwrap_or(0);
        let names_size = std::fs::metadata(&names_log_path).map(|m| m.len()).unwrap_or(0);

        if perf_size > 512 * 1024 || flake_size > 512 * 1024 || names_size > 512 * 1024 {
            let db_disk = Self::load(self.dir.clone());

            let write_perf = || -> std::io::Result<()> {
                let path = self.dir.join("perf.bin");
                let tmp = path.with_extension(format!("{}.tmp", std::process::id()));
                {
                    let file = std::fs::File::create(&tmp)?;
                    let mut writer = BufWriter::new(file);
                    writer.write_all(b"PRF1")?;
                    for env_map in &db_disk.perf {
                        writer.write_all(&(env_map.len() as u32).to_le_bytes())?;
                        for (k, v) in env_map {
                            writer.write_all(&k.to_le_bytes())?;
                            writer.write_all(&[v.samples.len() as u8])?;
                            for s in &v.samples {
                                writer.write_all(&s.timestamp_ms.to_le_bytes())?;
                                writer.write_all(&s.duration_ms.to_le_bytes())?;
                            }
                        }
                    }
                    writer.flush()?;
                }
                std::fs::rename(&tmp, &path)?;
                let _ = std::fs::File::create(&perf_log_path);
                Ok(())
            };
            let _ = write_perf();

            let write_flake = || -> std::io::Result<()> {
                let path = self.dir.join("flake.bin");
                let tmp = path.with_extension(format!("{}.tmp", std::process::id()));
                {
                    let file = std::fs::File::create(&tmp)?;
                    let mut writer = BufWriter::new(file);
                    writer.write_all(b"FLK1")?;
                    for env_map in &db_disk.flake {
                        writer.write_all(&(env_map.len() as u32).to_le_bytes())?;
                        for (k, v) in env_map {
                            writer.write_all(&k.to_le_bytes())?;
                            writer.write_all(&v.runs.to_le_bytes())?;
                            writer.write_all(&v.failures.to_le_bytes())?;
                            writer.write_all(&v.last_failure_timestamp_ms.unwrap_or(u64::MAX).to_le_bytes())?;
                            writer.write_all(&[v.last_failure_class.map(failure_class_to_u8).unwrap_or(0)])?;
                        }
                    }
                    writer.flush()?;
                }
                std::fs::rename(&tmp, &path)?;
                let _ = std::fs::File::create(&flake_log_path);
                Ok(())
            };
            let _ = write_flake();

            let write_names = || -> std::io::Result<()> {
                let path = self.dir.join("names.bin");
                let tmp = path.with_extension(format!("{}.tmp", std::process::id()));
                {
                    let file = std::fs::File::create(&tmp)?;
                    let mut writer = BufWriter::new(file);
                    writer.write_all(b"NMS1")?;
                    writer.write_all(&(db_disk.names.len() as u32).to_le_bytes())?;
                    for (k, v) in &db_disk.names {
                        writer.write_all(&k.to_le_bytes())?;
                        write_test_identity(&mut writer, v)?;
                    }
                    writer.flush()?;
                }
                std::fs::rename(&tmp, &path)?;
                let _ = std::fs::File::create(&names_log_path);
                Ok(())
            };
            let _ = write_names();
        }

        if lock_acquired {
            let _ = std::fs::remove_file(&lock_path);
        }

        Ok(())
    }
}

/// Nearest-rank percentile (`p` in 0..=100) over unsorted samples.
fn percentile(samples: &[u64], p: u64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let mut sorted: Vec<u64> = samples.to_vec();
    sorted.sort_unstable();
    let rank = ((p * (sorted.len() as u64)).div_ceil(100)).max(1) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::variant::Variant;

    fn id(t: &str, n: &str) -> TestIdentity {
        TestIdentity {
            target: t.into(),
            name: n.into(),
            variant: Variant::Default,
        }
    }

    #[test]
    fn percentiles_basic() {
        let s = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        assert_eq!(percentile(&s, 50), 50);
        assert_eq!(percentile(&s, 95), 100);
        assert_eq!(percentile(&[42], 50), 42);
        assert_eq!(percentile(&[], 50), 0);
    }

    #[test]
    fn record_and_estimate_roundtrip() {
        let mut db = DurationDb::ephemeral();
        let tid = id("m", "t");
        for (i, ms) in [100u64, 200, 300, 400].into_iter().enumerate() {
            db.record_at(Environment::Local, &tid, Duration::from_millis(ms), false, None, i as u64);
        }
        match db.estimate(Some(Environment::Local), &tid) {
            DurationEstimate::Measured { p50_ms, p95_ms } => {
                assert!((200..=300).contains(&p50_ms));
                assert_eq!(p95_ms, 400);
            }
            DurationEstimate::Unseen => panic!("should be measured"),
        }
        assert_eq!(db.estimate(None, &id("m", "absent")), DurationEstimate::Unseen);
    }

    #[test]
    fn flake_accounting() {
        let mut db = DurationDb::ephemeral();
        let tid = id("m", "t");
        db.record_at(
            Environment::Local,
            &tid,
            Duration::from_millis(1),
            true,
            Some(FailureClass::Fail),
            1,
        );
        db.record_at(Environment::Local, &tid, Duration::from_millis(1), false, None, 2);
        let f = db.flake(None, &tid).unwrap();
        assert_eq!(f.runs, 2);
        assert_eq!(f.failures, 1);
        assert_eq!(f.last_failure_class, Some(FailureClass::Fail));
    }

    #[test]
    fn cache_replays_are_not_recorded() {
        let mut db = DurationDb::ephemeral();
        let tid = id("m", "t");
        db.record_at(Environment::Local, &tid, Duration::from_millis(10), false, None, 1);
    }

    #[test]
    fn duration_db_persists_and_reloads() {
        let dir = std::env::temp_dir().join(format!("brtr-db-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let tid = id("m", "t");
        {
            let mut db = DurationDb::load(dir.clone());
            db.record_at(
                Environment::Local,
                &tid,
                Duration::from_millis(250),
                true,
                Some(FailureClass::Fatal),
                1,
            );
            db.record_at(Environment::Local, &tid, Duration::from_millis(250), false, None, 2);
            db.flush().unwrap();
        }
        let db = DurationDb::load(dir.clone());
        assert!(matches!(
            db.estimate(None, &tid),
            DurationEstimate::Measured { .. }
        ));
        assert_eq!(db.flake(None, &tid).unwrap().failures, 1);
        assert_eq!(db.get_name(hash_key(&tid)), Some(&tid));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_combine_monoid() {
        let mut db1 = DurationDb::ephemeral();
        let mut db2 = DurationDb::ephemeral();
        let tid = id("m", "t");

        db1.record_at(Environment::Local, &tid, Duration::from_millis(100), false, None, 1);
        db1.record_at(Environment::Remote, &tid, Duration::from_millis(150), true, Some(FailureClass::Fail), 2);

        db2.record_at(Environment::Local, &tid, Duration::from_millis(200), false, None, 3);
        db2.record_at(Environment::Remote, &tid, Duration::from_millis(250), false, None, 4);

        db1.combine(db2);

        // Check local
        match db1.estimate(Some(Environment::Local), &tid) {
            DurationEstimate::Measured { p50_ms, p95_ms } => {
                assert_eq!(p50_ms, 100); // 100, 200 -> 50% rank 1 is 100
                assert_eq!(p95_ms, 200);
            }
            _ => panic!("Expected measured"),
        }

        // Check remote flakes
        let f = db1.flake(Some(Environment::Remote), &tid).unwrap();
        assert_eq!(f.runs, 2);
        assert_eq!(f.failures, 1);
        assert_eq!(f.last_failure_class, Some(FailureClass::Fail));
    }
}
