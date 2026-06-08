//! The historical-duration / flake metadata store.
//!
//! Strictly **advisory**: it informs longest-first ordering, timeout selection,
//! micro-batching, CI sharding and failure-prediction display. It is NEVER used
//! to skip a test. It is loaded once at startup and flushed once at shutdown.
//!
//! Perf data (durations) and policy-affecting data (flake counts, last failure
//! class) are kept in separate files so a change to one cannot corrupt the
//! other.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::result::{FailureClass, TestIdentity};

/// Max recent samples retained per test for percentile estimation.
const MAX_SAMPLES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Environment {
    Local,
    Remote,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DurationSample {
    pub timestamp_ms: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Default, Serialize, Deserialize)]
struct PerfFile {
    env_tests: BTreeMap<Environment, BTreeMap<String, PerfRecord>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct FlakeFile {
    env_tests: BTreeMap<Environment, BTreeMap<String, FlakeRecord>>,
}

/// The loaded metadata store plus in-memory accumulators for this run.
pub struct DurationDb {
    dir: PathBuf,
    perf: PerfFile,
    flake: FlakeFile,
}

impl DurationDb {
    /// Load the store from `dir` (creating empty state if absent or corrupt —
    /// advisory data, so a parse failure starts fresh rather than aborting).
    pub fn load(dir: PathBuf) -> Self {
        let perf = read_json(&dir.join("perf.json")).unwrap_or_default();
        let flake = read_json(&dir.join("flake.json")).unwrap_or_default();
        Self { dir, perf, flake }
    }

    /// An empty in-memory store that never persists.
    pub fn ephemeral() -> Self {
        Self {
            dir: PathBuf::new(),
            perf: PerfFile::default(),
            flake: FlakeFile::default(),
        }
    }

    /// The duration estimate for a test. If environment is known, queries it,
    /// otherwise aggregates across environments.
    pub fn estimate(&self, env: Option<Environment>, test_id: &TestIdentity) -> DurationEstimate {
        let key = test_id.to_db_key();
        let mut combined = PerfRecord::default();
        if let Some(e) = env {
            if let Some(record) = self.perf.env_tests.get(&e).and_then(|t| t.get(&key)) {
                combined.combine(record);
            }
        } else {
            for tests in self.perf.env_tests.values() {
                if let Some(record) = tests.get(&key) {
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
        let key = test_id.to_db_key();
        let mut combined = FlakeRecord::default();
        let mut found = false;

        if let Some(e) = env {
            if let Some(record) = self.flake.env_tests.get(&e).and_then(|t| t.get(&key)) {
                combined.combine(record);
                found = true;
            }
        } else {
            for tests in self.flake.env_tests.values() {
                if let Some(record) = tests.get(&key) {
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
        let key = test_id.to_db_key();
        if !failed {
            let perf = self
                .perf
                .env_tests
                .entry(env)
                .or_default()
                .entry(key.clone())
                .or_default();
            perf.record(timestamp_ms, duration.as_millis() as u64);
        }
        let flake = self
            .flake
            .env_tests
            .entry(env)
            .or_default()
            .entry(key)
            .or_default();
        flake.record(failed, failure_class, timestamp_ms);
    }

    pub fn combine(&mut self, other: Self) {
        for (env, tests) in other.perf.env_tests {
            let self_tests = self.perf.env_tests.entry(env).or_default();
            for (k, v) in tests {
                self_tests.entry(k).or_default().combine(&v);
            }
        }
        for (env, tests) in other.flake.env_tests {
            let self_tests = self.flake.env_tests.entry(env).or_default();
            for (k, v) in tests {
                self_tests.entry(k).or_default().combine(&v);
            }
        }
    }

    /// Persist accumulated state (no-op for an ephemeral store). Best-effort.
    pub fn flush(&self) -> std::io::Result<()> {
        if self.dir.as_os_str().is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(&self.dir)?;
        write_json_atomic(&self.dir.join("perf.json"), &self.perf)?;
        write_json_atomic(&self.dir.join("flake.json"), &self.flake)?;
        Ok(())
    }
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Option<T> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    // Include PID in tmp file name to prevent collision across concurrent processes.
    let tmp = path.with_extension(format!("{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec(value).map_err(std::io::Error::other)?;
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
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
