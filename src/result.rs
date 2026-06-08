//! Generic (translator-independent) decoding of an `Execute2` response, plus the
//! terminal-status, result-identity, log-sink and execution-kind types.
//!
//! This is **stage 1** of status decoding: it turns the wire `ExecuteResponse2`
//! into a `RawOutcome` carrying the exit code / timeout / cancellation and the
//! captured streams — with NO pass/fail judgment. "exit 0 = pass" is a libtest
//! convention, not a protocol fact, so that judgment lives in the translator
//! (stage 2, see [`crate::translator`]).

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::proto::data::command_execution_kind::Command;
use crate::proto::test::execution_status::Status as WireStatus;
use crate::proto::test::{
    CancellationReason, ExecuteResponse2, ExecutionResult2, TestStatus, execute_response2,
};
use crate::variant::{RepeatKind, Variant};

/// What buck2 returned for one `Execute2`: a completed action, or a cancellation
/// whose two reasons mean very different things (see [`crate::scheduler`]).
#[derive(Debug)]
pub enum Execute2Outcome {
    Completed(CompletedAction),
    /// `RE_QUEUE_TIMEOUT`: transient infra; eligible for a bounded retry.
    CancelledQueueTimeout,
    /// `NOT_SPECIFIED` or absent reason: global/deadline cancel; never retried.
    CancelledUnspecified,
}

/// A completed action's observable results (still no pass/fail judgment).
#[derive(Debug)]
pub struct CompletedAction {
    pub status: ProcessOutcome,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub execution_time: Duration,
    pub max_memory_used_bytes: Option<u64>,
    pub exec_kind: ExecKind,
}

/// The raw process outcome, before any harness interpretation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOutcome {
    Finished { exit_code: i32 },
    TimedOut { after: Duration },
}

/// How buck2 executed the action — drives whether the run counts as a fresh
/// observation for the duration/flake DB; never gates test status. Has a
/// catch-all so an absent/unknown kind never panics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecKind {
    Local,
    RemoteCacheHit,
    RemoteExecuted,
    Omitted,
    Worker,
    Unknown,
}

impl ExecKind {
    /// Whether this execution is a fresh, independent run of the test (vs. a
    /// cache replay or omitted action). Only fresh runs are recorded into the
    /// duration/flake DB: a cache hit replays an already-recorded result, so
    /// counting it again would double-count an old duration sample and inflate
    /// the flake denominator with a non-independent observation.
    pub fn is_fresh_run(self) -> bool {
        matches!(
            self,
            ExecKind::Local | ExecKind::RemoteExecuted | ExecKind::Worker
        )
    }
}

/// A failure-class tag for the flake DB, derived from a terminal status. A named
/// enum (not a bare string) so a failing test's category is type-checked end to
/// end and serialized stably.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureClass {
    Fail,
    Fatal,
    Timeout,
    Infra,
}

/// The terminal statuses buck2 actually counts for a *test* result. Distinct
/// from the wire `TestStatus` so we cannot accidentally send a value buck2 drops
/// (UNKNOWN/RERUN) or a listing-only status as a per-test result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestVerdict {
    Pass,
    Fail,
    Skip,
    Omitted,
    Fatal,
    Timeout,
    InfraFailure,
}

impl TestVerdict {
    /// Map to the wire `TestStatus` discriminant.
    pub fn to_wire(self) -> i32 {
        let status = match self {
            TestVerdict::Pass => TestStatus::Pass,
            TestVerdict::Fail => TestStatus::Fail,
            TestVerdict::Skip => TestStatus::Skip,
            TestVerdict::Omitted => TestStatus::Omitted,
            TestVerdict::Fatal => TestStatus::Fatal,
            TestVerdict::Timeout => TestStatus::Timeout,
            TestVerdict::InfraFailure => TestStatus::InfraFailure,
        };
        status as i32
    }

    /// Whether this status should count as a run failure (before quarantine).
    ///
    /// `InfraFailure` counts as a failure: a test whose action exhausted its
    /// `RE_QUEUE_TIMEOUT` retries never produced a verdict, and reporting an
    /// undetermined test as a pass would turn an infra outage into a green build.
    pub fn is_failure(self) -> bool {
        matches!(
            self,
            TestVerdict::Fail
                | TestVerdict::Fatal
                | TestVerdict::Timeout
                | TestVerdict::InfraFailure
        )
    }
}

/// Decode a wire `ExecuteResponse2` into a [`Execute2Outcome`].
pub fn decode_response(response: ExecuteResponse2) -> Execute2Outcome {
    match response.response {
        Some(execute_response2::Response::Result(result)) => {
            Execute2Outcome::Completed(decode_result(result))
        }
        Some(execute_response2::Response::Cancelled(cancelled)) => {
            match cancelled
                .reason
                .and_then(|r| CancellationReason::try_from(r).ok())
            {
                Some(CancellationReason::ReQueueTimeout) => Execute2Outcome::CancelledQueueTimeout,
                // NOT_SPECIFIED or an absent/unknown reason: treat as a global
                // cancel; never retried.
                _ => Execute2Outcome::CancelledUnspecified,
            }
        }
        // A response with no `response` oneof is a malformed message from buck2;
        // treat it as an unspecified cancellation rather than panicking.
        None => Execute2Outcome::CancelledUnspecified,
    }
}

fn decode_result(result: ExecutionResult2) -> CompletedAction {
    let status = match result.status.and_then(|s| s.status) {
        Some(WireStatus::Finished(exit_code)) => ProcessOutcome::Finished { exit_code },
        Some(WireStatus::TimedOut(d)) => ProcessOutcome::TimedOut {
            after: duration_from_proto(d),
        },
        // No status is anomalous; surface it as a nonzero exit so the translator
        // reports a failure rather than a silent pass.
        None => ProcessOutcome::Finished { exit_code: -1 },
    };
    let stdout = result
        .stdout
        .and_then(|s| s.item)
        .map(stream_bytes)
        .unwrap_or_default();
    let stderr = result
        .stderr
        .and_then(|s| s.item)
        .map(stream_bytes)
        .unwrap_or_default();
    let execution_time = result
        .execution_time
        .map(duration_from_proto)
        .unwrap_or_default();
    let exec_kind = result
        .execution_details
        .and_then(|d| d.execution_kind)
        .map(classify_exec_kind)
        .unwrap_or(ExecKind::Unknown);
    CompletedAction {
        status,
        stdout,
        stderr,
        execution_time,
        max_memory_used_bytes: result.max_memory_used_bytes,
        exec_kind,
    }
}

fn stream_bytes(item: crate::proto::test::execution_stream::Item) -> Vec<u8> {
    match item {
        crate::proto::test::execution_stream::Item::Inline(bytes) => bytes,
    }
}

fn classify_exec_kind(kind: crate::proto::data::CommandExecutionKind) -> ExecKind {
    match kind.command {
        Some(Command::LocalCommand(_)) => ExecKind::Local,
        Some(Command::RemoteCommand(remote)) => {
            if remote.cache_hit {
                ExecKind::RemoteCacheHit
            } else {
                ExecKind::RemoteExecuted
            }
        }
        Some(Command::OmittedLocalCommand(_)) => ExecKind::Omitted,
        Some(Command::WorkerInitCommand(_)) | Some(Command::WorkerCommand(_)) => ExecKind::Worker,
        None => ExecKind::Unknown,
    }
}

fn duration_from_proto(d: prost_types::Duration) -> Duration {
    // Durations from buck2 are non-negative; clamp defensively rather than panic.
    let secs = d.seconds.max(0) as u64;
    let nanos = d.nanos.clamp(0, 1_999_999_999) as u32;
    Duration::new(secs, nanos)
}

use std::path::PathBuf;
use std::sync::OnceLock;

fn find_project_dir() -> Option<PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        if dir.join(".buckconfig").exists() {
            return Some(dir);
        }
        if !dir.pop() {
            break;
        }
    }
    std::env::current_dir().ok()
}

pub(crate) fn project_dir_key() -> &'static str {
    static CELL: OnceLock<String> = OnceLock::new();
    CELL.get_or_init(|| {
        let path = find_project_dir().unwrap_or_else(|| PathBuf::from("."));
        let path = path.canonicalize().unwrap_or(path);
        path.to_string_lossy().into_owned()
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TestIdentity {
    pub target: String,
    pub name: String,
    pub variant: Variant,
}

impl TestIdentity {
    pub(crate) fn to_db_key(&self) -> String {
        let proj = project_dir_key();
        let mut key = format!("{}\u{2}{}\u{1}{}", proj, self.target, self.name);
        if let Some(v) = self.variant.identity() {
            key.push('#');
            key.push_str(&v);
        }
        key
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RunIdentity {
    pub test: TestIdentity,
    pub repeat: RepeatKind,
    pub repeat_index: u32,
}

impl RunIdentity {
    pub fn to_buck2_name(&self) -> String {
        let mut name = self.test.name.to_owned();
        if let Some(v) = self.test.variant.identity() {
            name.push('#');
            name.push_str(&v);
        }
        if self.repeat.is_stress() {
            name.push_str("#rep");
            name.push_str(&self.repeat_index.to_string());
        }
        name
    }
}

/// Whether a details blob is small enough to send inline, or must go to CAS.
pub enum LogRouting {
    Inline,
    UploadToCas,
}

/// Decide where a log of `len` bytes should go given the inline limit.
pub fn route_log(len: usize, inline_limit: usize) -> LogRouting {
    if len <= inline_limit {
        LogRouting::Inline
    } else {
        LogRouting::UploadToCas
    }
}

/// Build a wire `TestResult`. `name` must already be the canonical result
/// identity (see [`result_name`]); `status` is a terminal, countable status.
pub fn build_test_result(
    identity: &RunIdentity,
    target: crate::proto::test::ConfiguredTargetHandle,
    status: TestVerdict,
    duration: Option<Duration>,
    details: String,
    max_memory_used_bytes: Option<u64>,
) -> crate::proto::test::TestResult {
    crate::proto::test::TestResult {
        name: identity.to_buck2_name(),
        status: status.to_wire(),
        msg: None,
        target: Some(target),
        duration: duration.map(|d| prost_types::Duration {
            seconds: d.as_secs() as i64,
            nanos: d.subsec_nanos() as i32,
        }),
        details,
        max_memory_used_bytes,
    }
}

/// The failure class for the flake DB, derived from a terminal status (`None`
/// for non-failures).
pub fn failure_class(status: TestVerdict) -> Option<FailureClass> {
    match status {
        TestVerdict::Fail => Some(FailureClass::Fail),
        TestVerdict::Fatal => Some(FailureClass::Fatal),
        TestVerdict::Timeout => Some(FailureClass::Timeout),
        TestVerdict::InfraFailure => Some(FailureClass::Infra),
        TestVerdict::Pass | TestVerdict::Skip | TestVerdict::Omitted => None,
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;

    #[test]
    fn test_to_db_key_contains_project_dir_and_separator() {
        let base_id = TestIdentity { target: "m".into(), name: "t".into(), variant: Variant::Default };
        let key = base_id.to_db_key();
        let proj = project_dir_key();
        assert!(!proj.is_empty(), "Project directory key should not be empty");
        assert!(key.starts_with(proj), "Key should start with project directory");
        assert!(key.contains('\u{2}'), "Key should contain the separator u2");
        assert!(key.contains('\u{1}'), "Key should contain the separator u1");
        let expected = format!("{}\u{2}m\u{1}t", proj);
        assert_eq!(key, expected);
    }

    #[test]
    fn result_identity_is_unique_across_axes() {
        let base_id = TestIdentity { target: "m".into(), name: "t".into(), variant: Variant::Default };
        let asan_id = TestIdentity { target: "m".into(), name: "t".into(), variant: Variant::Asan };

        let base = RunIdentity { test: base_id.clone(), repeat: RepeatKind::Once, repeat_index: 0 }.to_buck2_name();
        let asan = RunIdentity { test: asan_id.clone(), repeat: RepeatKind::Once, repeat_index: 0 }.to_buck2_name();
        let stress = RunIdentity { test: base_id, repeat: RepeatKind::Stress(NonZeroU32::new(5).unwrap()), repeat_index: 3 }.to_buck2_name();
        let both = RunIdentity { test: asan_id, repeat: RepeatKind::Stress(NonZeroU32::new(5).unwrap()), repeat_index: 3 }.to_buck2_name();

        assert_eq!(base, "t");
        assert_eq!(asan, "t#asan");
        assert_eq!(stress, "t#rep3");
        assert_eq!(both, "t#asan#rep3");

        let all = [base, asan, stress, both];
        let unique: std::collections::HashSet<_> = all.iter().collect();
        assert_eq!(unique.len(), 4);
    }

    #[test]
    fn terminal_status_maps_to_countable_wire_values() {
        assert_eq!(TestVerdict::Pass.to_wire(), TestStatus::Pass as i32);
        assert_eq!(
            TestVerdict::Timeout.to_wire(),
            TestStatus::Timeout as i32
        );
        assert!(TestVerdict::Fail.is_failure());
        // A test that never produced a verdict must not read as a pass.
        assert!(TestVerdict::InfraFailure.is_failure());
        assert!(!TestVerdict::Skip.is_failure());
        assert!(!TestVerdict::Omitted.is_failure());
    }

    #[test]
    fn cancelled_reason_classification() {
        let queue = ExecuteResponse2 {
            response: Some(execute_response2::Response::Cancelled(
                crate::proto::test::Cancelled {
                    reason: Some(CancellationReason::ReQueueTimeout as i32),
                },
            )),
        };
        assert!(matches!(
            decode_response(queue),
            Execute2Outcome::CancelledQueueTimeout
        ));

        let none = ExecuteResponse2 {
            response: Some(execute_response2::Response::Cancelled(
                crate::proto::test::Cancelled { reason: None },
            )),
        };
        assert!(matches!(
            decode_response(none),
            Execute2Outcome::CancelledUnspecified
        ));
    }
}
