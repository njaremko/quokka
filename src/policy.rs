//! Per-target policy axes derived from labels, each an independent typed value.
//!
//! These intentionally do not live in one bundled `TestPolicy` struct: each is
//! consumed by a different part of the pipeline (timeout by execution, retry by
//! the verdict fold, quarantine by the reporter, …), and bundling them would
//! couple every consumer to every axis.

use std::time::Duration;

/// Per-test timeout. `Default` means "use the runner's configured default".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestTimeout {
    Default,
    Fixed(Duration),
}

impl TestTimeout {
    /// Resolve to a concrete duration given the runner-wide default.
    pub fn resolve(self, default: Duration) -> Duration {
        match self {
            TestTimeout::Default => default,
            TestTimeout::Fixed(d) => d,
        }
    }
}

/// Whether a target's failures should block the build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuarantineStatus {
    /// Failures fail the run.
    Active,
    /// Run the tests, report their real status, but do not fail the run.
    Quarantined,
}

/// How many attempts a failing test gets (>= 1). One attempt = no retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    max_attempts: u32,
}

impl RetryPolicy {
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
        }
    }

    pub fn max_attempts(self) -> u32 {
        self.max_attempts
    }

    pub fn allows_retry(self) -> bool {
        self.max_attempts > 1
    }
}

/// The owning team, for failure routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owner {
    Unowned,
    Team(String),
}

/// Parse a timeout like `60`, `60s`, `5m`, `2h`.
fn parse_timeout(spec: &str) -> Option<Duration> {
    let spec = spec.trim();
    let (digits, mult) = if let Some(d) = spec.strip_suffix("ms") {
        (d, 1u64)
    } else if let Some(d) = spec.strip_suffix('s') {
        (d, 1000)
    } else if let Some(d) = spec.strip_suffix('m') {
        (d, 60_000)
    } else if let Some(d) = spec.strip_suffix('h') {
        (d, 3_600_000)
    } else {
        (spec, 1000)
    };
    let value: u64 = digits.trim().parse().ok()?;
    Some(Duration::from_millis(value.checked_mul(mult)?))
}

/// Derive the per-test timeout from labels.
pub fn test_timeout(labels: &[String]) -> TestTimeout {
    labels
        .iter()
        .filter_map(|l| {
            let suffix = l.split_once(':').unwrap_or(("", l)).1;
            suffix.strip_prefix("timeout=").and_then(parse_timeout)
        })
        .map(TestTimeout::Fixed)
        .reduce(|a, b| match (a, b) {
            (TestTimeout::Fixed(x), TestTimeout::Fixed(y)) => TestTimeout::Fixed(x.max(y)),
            _ => b,
        })
        .unwrap_or(TestTimeout::Default)
}

/// Derive quarantine status from labels.
pub fn quarantine_status(labels: &[String]) -> QuarantineStatus {
    if labels
        .iter()
        .any(|l| l.split_once(':').unwrap_or(("", l)).1 == "quarantined")
    {
        QuarantineStatus::Quarantined
    } else {
        QuarantineStatus::Active
    }
}

/// Derive the retry policy.
pub fn retry_policy(labels: &[String], flaky_attempts: u32) -> RetryPolicy {
    let retryable = labels.iter().any(|l| {
        let suffix = l.split_once(':').unwrap_or(("", l)).1;
        matches!(suffix, "flaky" | "retry")
    });
    if retryable {
        RetryPolicy::new(flaky_attempts)
    } else {
        RetryPolicy::new(1)
    }
}

/// Derive the owning team from labels.
pub fn owner(labels: &[String]) -> Owner {
    labels
        .iter()
        .filter_map(|l| {
            let suffix = l.split_once(':').unwrap_or(("", l)).1;
            suffix
                .strip_prefix("owner=")
                .map(|t| Owner::Team(t.to_owned()))
        })
        .last()
        .unwrap_or(Owner::Unowned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_resolution() {
        assert_eq!(
            test_timeout(&["rust:timeout=30s".to_owned()]),
            TestTimeout::Fixed(Duration::from_secs(30))
        );
        assert_eq!(
            TestTimeout::Default.resolve(Duration::from_secs(600)),
            Duration::from_secs(600)
        );
        assert_eq!(
            test_timeout(&[
                "python:timeout=10s".to_owned(),
                "rust:timeout=20s".to_owned()
            ]),
            TestTimeout::Fixed(Duration::from_secs(20))
        );
    }

    #[test]
    fn retry_policy_from_labels() {
        assert!(retry_policy(&["rust:flaky".to_owned()], 3).allows_retry());
        assert_eq!(
            retry_policy(&["python:flaky".to_owned()], 3).max_attempts(),
            3
        );
        assert!(!retry_policy(&[], 3).allows_retry());
        assert_eq!(retry_policy(&[], 3).max_attempts(), 1);
    }

    #[test]
    fn quarantine_and_exclusivity_and_owner() {
        assert_eq!(
            quarantine_status(&["quarantined".to_owned()]),
            QuarantineStatus::Quarantined
        );
        assert_eq!(
            owner(&["rust:owner=team-x".into()]),
            Owner::Team("team-x".into())
        );
        assert_eq!(owner(&[]), Owner::Unowned);
    }
}
