//! The scheduler: the orchestration that turns intake specs into per-test
//! `Execute2` calls and reported results, under bounded, fair concurrency.
//!
//! Concurrency model: each target is driven by its own task; each action
//! acquires its per-target permit before preparing its request, then acquires
//! the scarce global permit immediately before issuing its `Execute2`. The
//! order matters for fairness: because a target's actions cannot enter the
//! global queue until they hold one of that target's per-target permits, at
//! most `max_inflight_per_target` actions of any single target are ever queued
//! on the (FIFO) global semaphore at once. So a 20k-test target and 200 small
//! targets interleave with a bounded gap — a big target cannot monopolize the
//! global queue ahead of a later small one — and the global bound is never
//! pinned by an action parked on a per-target gate. Within a target, actions
//! are spawned longest-first (by historical p95) so the long tail starts
//! early. (The work is gRPC-bound, so this bounded-interleaving guarantee from
//! tokio's FIFO primitives is sufficient; a hand-rolled SoA selection loop
//! would buy no measurable locality.)
//!
//! Result reporting and the run verdict are owned by a single reporter task: it
//! is the sole caller of `report_test_result` and the sole computer of the exit
//! code, so results are written in one ordered stream and the verdict has one
//! owner. Discovery, CAS uploads and info-messages are intentionally fanned out
//! from the action tasks (tonic multiplexes concurrent RPCs over the one
//! connection); they carry no ordering relative to the verdict, and discovery of
//! a target's tests always happens-before that target's results. The caller
//! guarantees `end_of_test_results` is sent on every exit path (see
//! [`crate::run`]); this module only computes the exit code.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::FutureExt;
use futures::future::join_all;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;
use tracing::Level;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::batching::{self, BatchFailurePolicy};
use crate::caching::{self, CacheClass};
use crate::cli::RunnerConfig;
use crate::duration_db::{DurationDb, DurationEstimate};
use crate::execution::{
    ResourceFailure, ResourceMarkerToken, TestingRequest, build_listing_request,
    build_testing_request, cgroup_memory_max_for_action, parse_resource_failures,
};
use crate::executor_server::SpecEnvelope;

use crate::environment::{SchedulingProfile, profile_from_labels};
use crate::listing::{IgnoredPolicy, TestCase};
use crate::orchestrator::Orchestrator;
use crate::policy::{self, Owner, QuarantineStatus, RetryPolicy};
use crate::result::{
    self, Execute2Outcome, FailureClass, ProcessOutcome, RunIdentity, TestIdentity, TestVerdict,
    build_test_result, decode_response,
};
use crate::spec::TargetSpec;
use crate::translator::{
    ListingStrategy, PerTestObservation, Translator, TranslatorRegistry, libtest_help_output,
    libtest_list_output,
};
use crate::variant::RepeatKind;

const NONZERO_EXIT: i32 = 32;
/// Bounded retry budget for transient executor failures, independent of a
/// target's test-failure retry policy.
const INFRA_MAX_ATTEMPTS: u32 = 3;
/// Neutral duration (ms) assigned to tests with no history, so a cold cache
/// neither starves nor over-prioritizes them.
const UNSEEN_WEIGHT_MS: u64 = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunVerdict {
    Pass,
    Fail,
}

impl RunVerdict {
    fn exit_code(self) -> i32 {
        match self {
            RunVerdict::Pass => 0,
            RunVerdict::Fail => NONZERO_EXIT,
        }
    }
}

/// The observed outcome of one test within an action, before it is turned into
/// a wire result (groups the fields that vary per test to keep builders small).
#[derive(Clone)]
struct TestOutcome {
    status: TestVerdict,
    integrity: ResultIntegrity,
    details: String,
    duration: AttemptDuration,
    max_memory: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResultIntegrity {
    Complete,
    MissingTerminalResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReportDisposition {
    Passed,
    Skipped,
    QuarantinedFailure,
    Failure,
}

fn report_disposition(
    status: TestVerdict,
    integrity: ResultIntegrity,
    quarantined: bool,
) -> ReportDisposition {
    if integrity == ResultIntegrity::MissingTerminalResult {
        return ReportDisposition::Failure;
    }
    match status {
        TestVerdict::Pass => ReportDisposition::Passed,
        TestVerdict::Skip | TestVerdict::Omitted => ReportDisposition::Skipped,
        _ if quarantined => ReportDisposition::QuarantinedFailure,
        _ => ReportDisposition::Failure,
    }
}

/// One independent fresh execution of a test, recorded into the duration/flake
/// DB. Each *attempt* is its own observation — recording only the folded best
/// result would make a fail-then-pass flake indistinguishable from a clean pass,
/// blinding the flake history to exactly the tests it exists to surface.
#[derive(Clone)]
struct DbObservation {
    duration: Duration,
    failed: bool,
    failure_class: Option<FailureClass>,
    env: crate::duration_db::Environment,
}

#[derive(Clone)]
struct AttemptObservation {
    status: TestVerdict,
    duration: AttemptDuration,
    framework_case_duration: Option<Duration>,
    details: String,
    execution: AttemptExecution,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptDuration {
    Measured(Duration),
    Unmeasured,
}

#[derive(Clone, Copy)]
enum AttemptExecution {
    Physical(crate::duration_db::Environment),
    CacheServed,
    Unknown,
}

impl AttemptExecution {
    fn from_exec_kind(exec_kind: crate::result::ExecKind) -> Self {
        match exec_kind {
            crate::result::ExecKind::Local | crate::result::ExecKind::Worker => {
                Self::Physical(crate::duration_db::Environment::Local)
            }
            crate::result::ExecKind::RemoteExecuted => {
                Self::Physical(crate::duration_db::Environment::Remote)
            }
            crate::result::ExecKind::RemoteCacheHit => Self::CacheServed,
            crate::result::ExecKind::Omitted | crate::result::ExecKind::Unknown => Self::Unknown,
        }
    }

    fn fresh_environment(self) -> Option<crate::duration_db::Environment> {
        match self {
            Self::Physical(environment) => Some(environment),
            Self::CacheServed | Self::Unknown => None,
        }
    }

    fn ci_disposition(self) -> &'static str {
        match self {
            Self::Physical(_) => "physical",
            Self::CacheServed => "cache_served",
            Self::Unknown => "unknown",
        }
    }

    fn ci_environment(self) -> &'static str {
        match self {
            Self::Physical(crate::duration_db::Environment::Local) => "local",
            Self::Physical(crate::duration_db::Environment::Remote) | Self::CacheServed => "remote",
            Self::Unknown => "unknown",
        }
    }
}

enum ReporterMessage {
    Finished(Vec<FinishedTest>),
    Discovered(Vec<TestIdentity>),
    TargetDropped(String),
}

/// One finished test, ready for the reporter to write and fold.
struct FinishedTest {
    result: crate::proto::test::TestResult,
    console_name: String,
    /// Stable identity for this repeat/run, including the stress index.
    run_id: RunIdentity,
    /// Semantic identity for duration/flake DB lookups.
    test_id: TestIdentity,
    status: TestVerdict,
    integrity: ResultIntegrity,
    quarantined: bool,
    labels: Vec<String>,
    /// Every fresh, harness-attributed attempt of this test in this run. The
    /// reporter (sole DB writer) records each as an independent observation, so
    /// flaky tests that recover on retry still show their failures in history.
    db_observations: Vec<DbObservation>,
    attempt_history: Vec<AttemptObservation>,
}

/// Run the scheduler to completion, returning the exit code for `buck2 test`.
pub async fn run(
    orch: Orchestrator,
    mut intake: mpsc::UnboundedReceiver<SpecEnvelope>,
    config: RunnerConfig,
    context: crate::cli::SessionContext,
) -> i32 {
    if config.libtest_help_only {
        emit_libtest_help_stdout();
        drain_intake(&mut intake).await;
        return 0;
    }
    if let Some(error) = &config.libtest_usage_error {
        eprintln!("error: {error}");
        drain_intake(&mut intake).await;
        return NONZERO_EXIT;
    }

    let config = Arc::new(config);
    let global_sem = Arc::new(Semaphore::new(config.limits.max_inflight_test_actions));
    let listing_sem = Arc::new(Semaphore::new(config.limits.max_inflight_listings));

    // Read-only duration snapshot for ordering/sharding/batching.
    let duration_db = config.duration_db.persistent_path();

    let estimates = Arc::new(load_db(duration_db));

    // Single reporter task: sole orchestrator result-writer + verdict owner.
    let (report_tx, report_rx) = mpsc::channel::<ReporterMessage>(config.limits.max_report_queue);
    let reporter_db = load_db(duration_db);
    let report_json = config.ci_test_report_json.clone();
    let reporter = tokio::spawn(reporter_task(
        orch.clone(),
        report_rx,
        reporter_db,
        context,
        report_json,
    ));

    // Drive targets concurrently as their specs arrive.
    let mut target_tasks = JoinSet::new();
    let mut received_override_targets = FxHashSet::default();
    while let Some(envelope) = intake.recv().await {
        match envelope {
            SpecEnvelope::Spec(spec) => {
                if let Some(display) = valid_spec_target_display(&spec)
                    && config
                        .cgroup_memory_max_overrides
                        .iter()
                        .any(|candidate| candidate.target == display)
                {
                    received_override_targets.insert(display);
                }
                let ctx = TargetCtx {
                    orch: orch.clone(),
                    config: config.clone(),
                    global_sem: global_sem.clone(),
                    listing_sem: listing_sem.clone(),
                    estimates: estimates.clone(),
                    report_tx: report_tx.clone(),
                };
                target_tasks.spawn(async move { run_target(ctx, *spec).await });
            }
            SpecEnvelope::EndOfRequests => break,
        }
    }
    let missing_override_targets =
        missing_cgroup_override_targets(&config, &received_override_targets);
    for target in &missing_override_targets {
        if let Err(error) = report_tx
            .send(ReporterMessage::TargetDropped(format!(
                "cgroup memory override target was not received: {target}"
            )))
            .await
        {
            eprintln!("quokka: failed to report missing override target {target}: {error}");
        }
    }

    // Await every target's work, then close the reporter and fold the verdict.
    // A panicked target task forces a failing verdict: its tests never reported
    // their results, so folding only what arrived could otherwise read green.
    let any_target_panicked = drain_joinset(&mut target_tasks).await;
    drop(report_tx);
    let mut verdict = reporter.await.unwrap_or(RunVerdict::Fail);
    if any_target_panicked {
        verdict = RunVerdict::Fail;
    }
    verdict.exit_code()
}

fn valid_spec_target_display(spec: &crate::proto::test::ExternalRunnerSpec) -> Option<String> {
    let target = spec.target.as_ref()?;
    target.handle.as_ref()?;
    Some(format!(
        "{}//{}:{}",
        target.cell, target.package, target.target
    ))
}

fn missing_cgroup_override_targets(
    config: &RunnerConfig,
    received: &FxHashSet<String>,
) -> Vec<String> {
    let mut missing: Vec<_> = config
        .cgroup_memory_max_overrides
        .iter()
        .filter(|candidate| !received.contains(&candidate.target))
        .map(|candidate| candidate.target.clone())
        .collect();
    missing.sort_unstable();
    missing.dedup();
    missing
}

async fn drain_intake(intake: &mut mpsc::UnboundedReceiver<SpecEnvelope>) {
    while let Some(envelope) = intake.recv().await {
        if matches!(envelope, SpecEnvelope::EndOfRequests) {
            break;
        }
    }
}

/// Drain a [`JoinSet`], returning whether any task panicked or was cancelled. A
/// panicked unit of work must never be silently dropped: the reporter folds only
/// the results it actually received, so a swallowed panic could leave a falsely
/// green verdict for work that never finished. Callers turn a `true` into a hard
/// failure (the top-level run fails; a per-target drain reports a FATAL result).
async fn drain_joinset(tasks: &mut JoinSet<()>) -> bool {
    let mut failed = false;
    while let Some(joined) = tasks.join_next().await {
        if let Err(e) = joined {
            eprintln!("quokka: a spawned task did not complete: {e}");
            failed = true;
        }
    }
    failed
}

fn load_db(dir: Option<&std::path::Path>) -> DurationDb {
    match dir {
        Some(dir) => DurationDb::load(dir.to_path_buf()),
        None => DurationDb::ephemeral(),
    }
}

/// Running counts the reporter folds into the final session summary.
#[derive(Default)]
struct Tally {
    total: u64,
    passed: u64,
    failed: u64,
    quarantined_failed: u64,
    skipped: u64,
}

/// The live-console line for a failed test: its name plus the complete failure
/// details (panic/assertion text, captured output, and the `[brtr: …]`
/// annotation). Details are omitted only when empty, to avoid a dangling blank
/// line; the full failure is otherwise always printed, never truncated.
fn fail_console_line(name: &str, details: &str) -> String {
    if details.is_empty() {
        format!("FAIL {name}")
    } else {
        format!("FAIL {name}\n{details}")
    }
}

fn console_test_name(run_id: &RunIdentity) -> String {
    format!("{}::{}", run_id.test.target, run_id.to_buck2_name())
}

async fn reporter_task(
    orch: Orchestrator,
    mut rx: mpsc::Receiver<ReporterMessage>,
    mut db: DurationDb,
    context: crate::cli::SessionContext,
    report_json: Option<PathBuf>,
) -> RunVerdict {
    let mut verdict = RunVerdict::Pass;
    let mut tally = Tally::default();
    let mut ci_report_failed = false;
    let mut ci_report = None;
    if let Some(path) = report_json {
        match CiTestReportWriter::new(path.clone(), &context) {
            Ok(writer) => ci_report = Some(writer),
            Err(error) => {
                report_ci_report_failure(
                    &orch,
                    &mut ci_report_failed,
                    format!("failed to start {}: {error}", path.display()),
                )
                .await;
            }
        }
    }
    while let Some(msg) = rx.recv().await {
        match msg {
            ReporterMessage::Discovered(tids) => {
                for tid in tids {
                    db.record_discovered_name(&tid);
                }
            }
            ReporterMessage::TargetDropped(reason) => {
                tally.total += 1;
                tally.failed += 1;
                verdict = RunVerdict::Fail;
                if let Err(e) = orch
                    .console(Level::ERROR, format!("quokka: {reason}"))
                    .await
                {
                    eprintln!("quokka: failed to show dropped target: {e:#}");
                }
            }
            ReporterMessage::Finished(batch) => {
                for finished in batch {
                    let report_error = ci_report
                        .as_mut()
                        .and_then(|writer| writer.write_test(&finished).err());
                    if let Some(error) = report_error {
                        if let Some(writer) = ci_report.take() {
                            let cleanup_error = writer.cleanup(false);
                            let detail = match cleanup_error {
                                Some(cleanup_error) => format!(
                                    "{error}; failed to remove temporary report: {cleanup_error}"
                                ),
                                None => error,
                            };
                            report_ci_report_failure(
                                &orch,
                                &mut ci_report_failed,
                                format!("CI test report disabled: {detail}"),
                            )
                            .await;
                        }
                    }
                    tally.total += 1;
                    match report_disposition(
                        finished.status,
                        finished.integrity,
                        finished.quarantined,
                    ) {
                        ReportDisposition::Passed => tally.passed += 1,
                        ReportDisposition::Skipped => tally.skipped += 1,
                        ReportDisposition::QuarantinedFailure => {
                            tally.quarantined_failed += 1;
                        }
                        ReportDisposition::Failure => {
                            tally.failed += 1;
                            verdict = RunVerdict::Fail;
                            // Live, complete failure feedback on buck2's console:
                            // the name plus the full failure details. Borrow the
                            // result before it is moved into the report below, so
                            // no copy is made. Every non-quarantined failure is
                            // shown; the loop is bounded by the finite number of
                            // reported tests, so this cannot flood without bound.
                            let line =
                                fail_console_line(&finished.console_name, &finished.result.details);
                            // Best-effort: if the downward channel is broken, the
                            // report_test_result below fails too and is logged.
                            if let Err(e) = orch.console(Level::WARN, line).await {
                                eprintln!("quokka: failed to show test failure on console: {e:#}");
                            }
                        }
                    }
                    if let Err(e) = orch.report_test_result(finished.result).await {
                        verdict = RunVerdict::Fail;
                        eprintln!("quokka: failed to report a test result: {e:#}");
                    }
                    // Record EACH fresh attempt as an independent flake/duration sample
                    // (a fail-then-pass flake records as runs+=2/failures+=1, not a clean
                    // pass). The folded `finished.status` drives the verdict/tally above;
                    // the DB sees the per-attempt history.
                    for obs in &finished.db_observations {
                        db.record(
                            obs.env,
                            &finished.test_id,
                            obs.duration,
                            obs.failed,
                            obs.failure_class,
                        );
                    }
                }
            }
        }
    }

    // Final summary to the live console + the session report channel.
    let summary = session_summary(&tally, &context);
    let level = if tally.failed > 0 {
        Level::WARN
    } else {
        Level::INFO
    };
    // The console line is best-effort live feedback; a broken downward channel
    // also breaks the session report below, which is surfaced.
    if let Err(e) = orch.console(level, summary.clone()).await {
        eprintln!("quokka: failed to show test summary on console: {e:#}");
    }
    if let Err(e) = orch
        .report_test_session(summary, context.trace_id.clone())
        .await
    {
        eprintln!("quokka: failed to report the test session: {e:#}");
    }

    if let Err(e) = db.flush() {
        eprintln!("quokka: failed to flush duration DB: {e:#}");
    }
    if let Some(writer) = ci_report {
        if let Err(error) = writer.finish() {
            report_ci_report_failure(
                &orch,
                &mut ci_report_failed,
                format!("failed to finalize CI test report: {error}"),
            )
            .await;
        }
    }
    verdict
}

async fn report_ci_report_failure(orch: &Orchestrator, emitted: &mut bool, detail: String) {
    if *emitted {
        return;
    }
    *emitted = true;
    let message = format!("quokka: {detail}");
    if let Err(error) = orch.console(Level::ERROR, message.clone()).await {
        eprintln!("{message}; failed to show report diagnostic: {error:#}");
    }
}

fn ci_test_value(finished: &FinishedTest) -> serde_json::Value {
    let status = match finished.status {
        TestVerdict::Pass => "pass",
        TestVerdict::Fail => "failure",
        TestVerdict::Skip => "skip",
        TestVerdict::Omitted => "omitted",
        TestVerdict::Fatal | TestVerdict::InfraFailure => "infrastructure_error",
        TestVerdict::Timeout => "timeout",
    };
    let integrity = match finished.integrity {
        ResultIntegrity::Complete => "complete",
        ResultIntegrity::MissingTerminalResult => "missing_terminal_result",
    };
    let attempts = finished
        .attempt_history
        .iter()
        .enumerate()
        .map(|(ordinal, observation)| {
            let outcome = match observation.status {
                TestVerdict::Pass => "pass",
                TestVerdict::Fail => "failure",
                TestVerdict::Skip => "skip",
                TestVerdict::Omitted => "omitted",
                TestVerdict::Fatal | TestVerdict::InfraFailure => "infrastructure_error",
                TestVerdict::Timeout => "timeout",
            };
            let mut value = serde_json::json!({
                "ordinal": ordinal,
                "execution_disposition": observation.execution.ci_disposition(),
                "outcome": outcome,
                "executor_environment": observation.execution.ci_environment(),
            });
            if let AttemptDuration::Measured(duration) = observation.duration {
                value["action_or_batch_duration_seconds"] =
                    serde_json::json!(duration.as_secs_f64());
            }
            if let Some(duration) = observation.framework_case_duration {
                value["duration_seconds"] = serde_json::json!(duration.as_secs_f64());
            }
            value
        })
        .collect::<Vec<_>>();
    let duration =
        finished.result.duration.as_ref().map(|value| {
            value.seconds.max(0) as f64 + f64::from(value.nanos.max(0)) / 1_000_000_000.0
        });
    let framework_case_duration = if finished.integrity == ResultIntegrity::Complete {
        finished
            .attempt_history
            .iter()
            .rev()
            .find(|observation| observation.status == finished.status)
            .and_then(|observation| observation.framework_case_duration)
    } else {
        None
    };
    let mut value = serde_json::json!({
        "target": finished.test_id.target,
        "name": finished.test_id.name,
        "variant": finished.test_id.variant.identity().unwrap_or_else(|| "default".to_owned()),
        "run_name": finished.run_id.to_buck2_name(),
        "repeat_index": finished.run_id.repeat_index,
        "status": status,
        "integrity": integrity,
        "quarantined": finished.quarantined,
        "labels": finished.labels,
        "action_or_batch_duration_seconds": duration,
        "max_memory_used_bytes": finished.result.max_memory_used_bytes,
        "attempts": attempts,
    });
    if let Some(duration) = framework_case_duration {
        value["framework_case_duration_seconds"] = serde_json::json!(duration.as_secs_f64());
    }
    value
}

struct CiTestReportWriter {
    destination: PathBuf,
    temporary: PathBuf,
    backup: PathBuf,
    output: File,
    first_test: bool,
    previous_destination_present: bool,
    destination_installed: bool,
    backup_created: bool,
}

static CI_REPORT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn report_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum CiReportIoPoint {
    Create = 1,
    Header = 2,
    Record = 3,
    FileSync = 4,
    Rename = 5,
    ParentSync = 6,
    RollbackRemove = 7,
    RollbackRestore = 8,
    RollbackParentSync = 9,
}

#[cfg(test)]
static CI_REPORT_FAULT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
#[cfg(test)]
static CI_REPORT_ROLLBACK_FAULT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
#[cfg(test)]
static CI_REPORT_FAULT_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
fn maybe_inject_ci_report_fault(point: CiReportIoPoint) -> Result<(), String> {
    for fault in [&CI_REPORT_FAULT, &CI_REPORT_ROLLBACK_FAULT] {
        if fault
            .compare_exchange(point as u8, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Err(format!("injected CI report {:?} failure", point as u8));
        }
    }
    Ok(())
}

#[cfg(not(test))]
fn maybe_inject_ci_report_fault(_point: CiReportIoPoint) -> Result<(), String> {
    Ok(())
}

impl CiTestReportWriter {
    fn new(path: PathBuf, context: &crate::cli::SessionContext) -> Result<Self, String> {
        let parent = report_parent(&path);
        maybe_inject_ci_report_fault(CiReportIoPoint::Create)?;
        fs::create_dir_all(parent).map_err(|e| format!("create report directory: {e}"))?;
        let destination_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("quokka-ci-test-report");
        let sequence = CI_REPORT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = parent.join(format!(
            ".{destination_name}.{}.{}.tmp",
            std::process::id(),
            sequence
        ));
        let backup = parent.join(format!(
            ".{destination_name}.{}.{}.bak",
            std::process::id(),
            sequence
        ));
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|e| format!("create temporary report {}: {e}", temporary.display()))?;
        let write_result = (|| -> Result<(), String> {
            maybe_inject_ci_report_fault(CiReportIoPoint::Header)?;
            output
                .write_all(b"{\"schema\":\"quokka.ci-test-report.v1\",\"host_platform\":")
                .map_err(|e| format!("write report header: {e}"))?;
            serde_json::to_writer(&mut output, &context.host_platform)
                .map_err(|e| format!("write host platform: {e}"))?;
            output
                .write_all(b",\"trace_id\":")
                .map_err(|e| format!("write report trace prefix: {e}"))?;
            serde_json::to_writer(&mut output, &context.trace_id)
                .map_err(|e| format!("write trace id: {e}"))?;
            output
                .write_all(b",\"tests\":[")
                .map_err(|e| format!("write test array prefix: {e}"))?;
            Ok(())
        })();
        if let Err(error) = write_result {
            return match fs::remove_file(&temporary) {
                Ok(()) => Err(error),
                Err(cleanup_error) if cleanup_error.kind() == std::io::ErrorKind::NotFound => {
                    Err(error)
                }
                Err(cleanup_error) => Err(format!(
                    "{error}; failed to remove temporary report {}: {cleanup_error}",
                    temporary.display()
                )),
            };
        }
        Ok(Self {
            destination: path,
            temporary,
            backup,
            output,
            first_test: true,
            previous_destination_present: false,
            destination_installed: false,
            backup_created: false,
        })
    }

    fn write_test(&mut self, finished: &FinishedTest) -> Result<(), String> {
        maybe_inject_ci_report_fault(CiReportIoPoint::Record)?;
        if !self.first_test {
            self.output
                .write_all(b",")
                .map_err(|e| format!("write test separator: {e}"))?;
        }
        let value = ci_test_value(finished);
        serde_json::to_writer(&mut self.output, &value)
            .map_err(|e| format!("write test record: {e}"))?;
        self.first_test = false;
        Ok(())
    }

    fn finish(mut self) -> Result<(), String> {
        let result = self.finish_inner();
        if let Err(error) = result {
            let rollback_error = self.rollback();
            let rollback_failed = rollback_error.is_err();
            let recovery_path = rollback_failed.then(|| self.recovery_path().to_owned());
            let cleanup_error = self.cleanup(rollback_failed);
            let mut detail = error;
            if let Err(rollback_error) = rollback_error {
                detail.push_str(&format!("; rollback failed: {rollback_error}"));
                if let Some(recovery_path) = recovery_path {
                    detail.push_str(&format!(
                        "; recovery retained at {}",
                        recovery_path.display()
                    ));
                }
            }
            if let Some(cleanup_error) = cleanup_error {
                detail.push_str(&format!("; cleanup failed: {cleanup_error}"));
            }
            return Err(detail);
        }
        Ok(())
    }

    fn finish_inner(&mut self) -> Result<(), String> {
        self.output
            .write_all(b"]}\n")
            .map_err(|e| format!("write report footer: {e}"))?;
        maybe_inject_ci_report_fault(CiReportIoPoint::FileSync)?;
        self.output
            .sync_all()
            .map_err(|e| format!("sync report: {e}"))?;
        self.snapshot_destination()?;
        maybe_inject_ci_report_fault(CiReportIoPoint::Rename)?;
        fs::rename(&self.temporary, &self.destination)
            .map_err(|e| format!("rename report into place: {e}"))?;
        self.destination_installed = true;
        self.sync_parent()?;
        self.remove_backup()?;
        Ok(())
    }

    fn snapshot_destination(&mut self) -> Result<(), String> {
        match fs::metadata(&self.destination) {
            Ok(metadata) => {
                if !metadata.is_file() {
                    return Err(format!(
                        "backup report destination {} is not a regular file",
                        self.destination.display()
                    ));
                }
                fs::copy(&self.destination, &self.backup).map_err(|e| {
                    format!("backup existing report {}: {e}", self.destination.display())
                })?;
                File::open(&self.backup)
                    .and_then(|backup| backup.sync_all())
                    .map_err(|e| format!("sync report backup {}: {e}", self.backup.display()))?;
                self.previous_destination_present = true;
                self.backup_created = true;
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!(
                "inspect existing report {}: {error}",
                self.destination.display()
            )),
        }
    }

    fn sync_parent(&self) -> Result<(), String> {
        maybe_inject_ci_report_fault(CiReportIoPoint::ParentSync)?;
        self.sync_parent_raw()
    }

    fn sync_parent_raw(&self) -> Result<(), String> {
        let parent = report_parent(&self.destination);
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|e| format!("sync report directory: {e}"))
    }

    fn recovery_path(&self) -> &Path {
        if self.backup_created {
            &self.backup
        } else {
            &self.destination
        }
    }

    fn rollback(&mut self) -> Result<(), String> {
        if !self.destination_installed {
            return Ok(());
        }
        if self.previous_destination_present {
            if !self.backup_created {
                return Err("previous report backup is missing".to_owned());
            }
            maybe_inject_ci_report_fault(CiReportIoPoint::RollbackRemove)?;
            match fs::remove_file(&self.destination) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(format!("remove failed report before rollback: {error}"));
                }
            }
            maybe_inject_ci_report_fault(CiReportIoPoint::RollbackRestore)?;
            fs::rename(&self.backup, &self.destination)
                .map_err(|e| format!("restore previous report: {e}"))?;
            self.backup_created = false;
            File::open(&self.destination)
                .and_then(|report| report.sync_all())
                .map_err(|e| format!("sync restored report: {e}"))?;
        } else {
            maybe_inject_ci_report_fault(CiReportIoPoint::RollbackRemove)?;
            match fs::remove_file(&self.destination) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(format!("remove failed report: {error}")),
            }
        }
        maybe_inject_ci_report_fault(CiReportIoPoint::RollbackParentSync)?;
        self.sync_parent_raw()?;
        self.destination_installed = false;
        Ok(())
    }

    fn remove_backup(&mut self) -> Result<(), String> {
        if !self.backup_created {
            return Ok(());
        }
        fs::remove_file(&self.backup)
            .map_err(|e| format!("remove report backup {}: {e}", self.backup.display()))?;
        self.backup_created = false;
        Ok(())
    }

    fn cleanup(&self, preserve_recovery: bool) -> Option<String> {
        let mut detail: Option<String> = None;
        for path in [&self.temporary, &self.backup] {
            if preserve_recovery && path == &self.backup {
                continue;
            }
            if let Err(error) = fs::remove_file(path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                let message = format!(
                    "failed to remove temporary report {}: {error}",
                    path.display()
                );
                if let Some(existing) = &mut detail {
                    existing.push_str("; ");
                    existing.push_str(&message);
                } else {
                    detail = Some(message);
                }
            }
        }
        detail
    }
}

fn session_summary(tally: &Tally, context: &crate::cli::SessionContext) -> String {
    let mut s = format!(
        "test run complete: {} tests, {} passed, {} failed, {} skipped",
        tally.total, tally.passed, tally.failed, tally.skipped
    );
    if tally.quarantined_failed > 0 {
        s.push_str(&format!(
            " ({} quarantined failure(s), not counted)",
            tally.quarantined_failed
        ));
    }
    if let Some(host) = &context.host_platform {
        s.push_str(&format!(" [host={host}]"));
    }
    if let Some(trace) = &context.trace_id {
        s.push_str(&format!(" [trace={trace}]"));
    }
    s
}

/// Shared context passed to each target task.
#[derive(Clone)]
struct TargetCtx {
    orch: Orchestrator,
    config: Arc<RunnerConfig>,
    global_sem: Arc<Semaphore>,
    listing_sem: Arc<Semaphore>,
    estimates: Arc<DurationDb>,
    report_tx: mpsc::Sender<ReporterMessage>,
}

/// The per-target policy derived once from labels + config.
struct TargetPlan {
    spec: Arc<TargetSpec>,
    translator: Box<dyn Translator>,
    ignored: IgnoredPolicy,
    cache_class: CacheClass,
    retry: RetryPolicy,
    quarantine: QuarantineStatus,
    timeout: Duration,
    profile: SchedulingProfile,
    owner: Owner,
    batch_mode: Option<batching::BatchMode>,
    /// Effective repetition for this target: the global `--stress` if set,
    /// otherwise stress implied by a `rust:stress` label, otherwise `Once`.
    repeat: RepeatKind,
}

impl TargetPlan {
    fn derive(
        spec: Arc<TargetSpec>,
        config: &RunnerConfig,
        registry: &TranslatorRegistry,
    ) -> Result<Self, String> {
        let labels: &[String] = &spec.labels;
        let Some(translator) = registry.resolve(&spec.test_type, config) else {
            return Err(format!(
                "no translator registered for test_type '{}'",
                spec.test_type
            ));
        };
        // The `rust:stress` label means "run this target repeatedly". A global
        // `--stress N` takes precedence (it stresses every target); otherwise the
        // label opts this target into the configured per-label repetition count.
        let repeat = if config.stress.is_stress() {
            config.stress
        } else if labels
            .iter()
            .any(|l| l.split_once(':').unwrap_or(("", l)).1 == "stress")
        {
            RepeatKind::Stress(config.stress_label_reps)
        } else {
            RepeatKind::Once
        };
        Ok(TargetPlan {
            translator,
            ignored: config.ignored,
            cache_class: caching::cache_class(labels),
            retry: policy::retry_policy(labels, config.flaky_attempts),
            quarantine: policy::quarantine_status(labels),
            timeout: config.per_test_timeout,
            profile: profile_from_labels(labels).unwrap_or_else(|e| {
                eprintln!(
                    "quokka: conflict resolving labels for {}: {}",
                    spec.display, e
                );
                SchedulingProfile::default()
            }),
            owner: policy::owner(labels),
            batch_mode: target_batch_mode(labels),
            repeat,
            spec,
        })
    }

    fn listing_profile(&self, config: &RunnerConfig) -> SchedulingProfile {
        let mut p = SchedulingProfile::default();
        if !self.translator.declares_executor_overrides() {
            // Default execution.
        } else if config.local_debug {
            p.hardware.local_debug = true;
        } else {
            p.hardware.listing_only = true;
        }
        p
    }

    fn testing_profile(&self, config: &RunnerConfig) -> SchedulingProfile {
        let mut p = self.profile.clone();
        if !self.translator.declares_executor_overrides() {
            p.hardware = Default::default();
            p.local_resources.clear();
        } else if config.local_debug {
            p.hardware = Default::default();
            p.hardware.local_debug = true;
            // Retain local_resources? Yes.
        }
        p
    }

    fn quarantined(&self) -> bool {
        self.quarantine == QuarantineStatus::Quarantined
    }
}

fn target_batch_mode(labels: &[String]) -> Option<batching::BatchMode> {
    if labels
        .iter()
        .any(|l| l.split_once(':').unwrap_or(("", l)).1 == "big")
    {
        Some(batching::BatchMode::PerTest)
    } else {
        None
    }
}

fn validate_cgroup_memory_overrides(
    plan: &TargetPlan,
    config: &RunnerConfig,
    tests: &[TestCase],
) -> Result<(), String> {
    let target_overrides: Vec<_> = config
        .cgroup_memory_max_overrides
        .iter()
        .filter(|candidate| candidate.target == plan.spec.display)
        .collect();
    if target_overrides.is_empty() {
        return Ok(());
    }
    if plan.spec.test_type != "rust" || !plan.spec.labels.iter().any(|label| label == "rust:big") {
        return Err(format!(
            "cgroup memory overrides require a rust:big singleton target: {}",
            plan.spec.display
        ));
    }
    for candidate in target_overrides {
        let match_count = tests
            .iter()
            .filter(|test| test.name == candidate.test)
            .count();
        if match_count != 1 {
            return Err(format!(
                "cgroup memory override matched {match_count} tests, expected exactly one: {}|{}",
                candidate.target, candidate.test
            ));
        }
    }
    Ok(())
}

async fn run_target(ctx: TargetCtx, spec_proto: crate::proto::test::ExternalRunnerSpec) {
    let target_display =
        valid_spec_target_display(&spec_proto).unwrap_or_else(|| "<unknown target>".to_owned());
    let spec = match TargetSpec::from_proto(spec_proto) {
        Ok(spec) => spec,
        Err(e) => {
            let reason = format!("dropping malformed spec for {target_display}: {e}");
            if let Err(send_error) = ctx
                .report_tx
                .send(ReporterMessage::TargetDropped(reason))
                .await
            {
                eprintln!("quokka: failed to report malformed spec: {send_error}");
            }
            return;
        }
    };
    let registry = TranslatorRegistry::new();
    let plan = match TargetPlan::derive(spec.clone(), &ctx.config, &registry) {
        Ok(plan) => Arc::new(plan),
        Err(reason) => {
            report_target_failure_for_spec(
                &ctx,
                &spec,
                false,
                RepeatKind::Once,
                TestVerdict::Fatal,
                reason,
            )
            .await;
            return;
        }
    };

    let panic = std::panic::AssertUnwindSafe(run_per_test_target(&ctx, plan.clone()))
        .catch_unwind()
        .await;
    if let Err(payload) = panic {
        report_target_failure_for_spec(
            &ctx,
            &spec,
            false,
            plan.repeat,
            TestVerdict::Fatal,
            format!(
                "target {} panicked: {}",
                spec.display,
                panic_payload_details(payload.as_ref()),
            ),
        )
        .await;
    }
}

fn panic_payload_details(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

async fn run_per_test_target(ctx: &TargetCtx, plan: Arc<TargetPlan>) {
    let config = &ctx.config;
    // ---- Listing (uncacheable, bounded, infra-retried) ----
    // The listing is always issued uncacheable under the current runner policy.
    // A transient RE-queue timeout on the listing is
    // retried (bounded) like a test action; on exhaustion it is an InfraFailure
    // (a countable failure) — never a silent OMITTED, which would drop the whole
    // target's tests as a green pass.
    let listing_outcome = match plan.translator.listing_strategy() {
        ListingStrategy::PerTestListing { request_args, .. } => {
            let listing_args = (request_args)(plan.ignored, &config.extra_test_args);
            let mut infra_attempt = 0u32;
            let outcome = loop {
                let resource_marker_token = ResourceMarkerToken::from_stable_fields(
                    "listing",
                    std::iter::once(plan.spec.display.as_str())
                        .chain(listing_args.iter().map(String::as_str)),
                );
                let request = build_listing_request(
                    &plan.spec,
                    &listing_args,
                    &plan.listing_profile(config),
                    config.listing_timeout,
                    config.cgroup_granularity,
                    config.cgroup_memory_max,
                    &resource_marker_token,
                    &config.extra_env,
                );
                let response = {
                    let _permit = ctx.listing_sem.acquire().await.expect("listing semaphore");
                    ctx.orch.execute2(request).await
                };
                match response {
                    Ok(response) => match decode_response(response) {
                        Execute2Outcome::CancelledQueueTimeout
                            if infra_attempt + 1 < INFRA_MAX_ATTEMPTS =>
                        {
                            infra_attempt += 1;
                            continue;
                        }
                        outcome => break Ok((outcome, resource_marker_token)),
                    },
                    Err(e) => break Err(e),
                }
            };
            Some(outcome)
        }
        _ => None,
    };

    let tests = match plan.translator.listing_strategy() {
        ListingStrategy::WholeTarget { name } | ListingStrategy::WholeBinary { name } => {
            vec![TestCase::new(
                (*name).to_string(),
                crate::listing::TestCaseKind::Test,
                false,
            )]
        }
        ListingStrategy::PerTestListing { parse, .. } => match listing_outcome {
            Some(Ok((Execute2Outcome::Completed(action), resource_marker_token))) => {
                let resource_failure =
                    parse_resource_failures(&action.stderr, &resource_marker_token)
                        .into_iter()
                        .next();
                if let Some(failure) = resource_failure {
                    let (status, details) = match failure {
                        ResourceFailure::LogicalTimeout { index, details }
                            if config.cgroup_granularity
                                == crate::execution::CgroupGranularity::LogicalTest
                                && index == 0 =>
                        {
                            (TestVerdict::Timeout, details)
                        }
                        ResourceFailure::LogicalOom { index, details }
                            if config.cgroup_granularity
                                == crate::execution::CgroupGranularity::LogicalTest
                                && index == 0 =>
                        {
                            (TestVerdict::Fail, details)
                        }
                        ResourceFailure::ActionOom { details }
                            if config.cgroup_granularity
                                == crate::execution::CgroupGranularity::Action =>
                        {
                            (TestVerdict::Fail, details)
                        }
                        ResourceFailure::ActionTimeout { details }
                            if config.cgroup_granularity
                                == crate::execution::CgroupGranularity::Action =>
                        {
                            (TestVerdict::Timeout, details)
                        }
                        ResourceFailure::LogicalTimeout { details, .. }
                        | ResourceFailure::LogicalOom { details, .. }
                        | ResourceFailure::ActionTimeout { details }
                        | ResourceFailure::ActionOom { details } => (
                            TestVerdict::InfraFailure,
                            format!("impossible resource marker: {details}"),
                        ),
                        ResourceFailure::Setup { details } => (TestVerdict::InfraFailure, details),
                        ResourceFailure::Malformed { details } => (
                            TestVerdict::InfraFailure,
                            format!("malformed resource marker: {details}"),
                        ),
                    };
                    report_target_failure(ctx, &plan, status, details).await;
                    return;
                }
                match action.status {
                    ProcessOutcome::Finished { exit_code: 0 } => {
                        match (parse)(&action.stdout, plan.ignored) {
                            Ok(tests) => tests,
                            Err(e) => {
                                report_target_failure(
                                    ctx,
                                    &plan,
                                    TestVerdict::Fatal,
                                    format!("listing parse failed: {e}"),
                                )
                                .await;
                                return;
                            }
                        }
                    }
                    ProcessOutcome::Finished { exit_code } => {
                        report_target_failure(
                            ctx,
                            &plan,
                            TestVerdict::Fatal,
                            format!(
                                "listing exited {exit_code}\n{}",
                                String::from_utf8_lossy(&action.stderr)
                            ),
                        )
                        .await;
                        return;
                    }
                    ProcessOutcome::TimedOut { .. } => {
                        report_target_failure(
                            ctx,
                            &plan,
                            TestVerdict::Timeout,
                            "listing timed out".into(),
                        )
                        .await;
                        return;
                    }
                }
            }
            Some(Ok((Execute2Outcome::CancelledQueueTimeout, _))) => {
                report_target_failure(
                    ctx,
                    &plan,
                    TestVerdict::InfraFailure,
                    "listing RE queue timeout (retries exhausted)".into(),
                )
                .await;
                return;
            }
            Some(Ok((Execute2Outcome::CancelledUnspecified, _))) => {
                report_target_failure(ctx, &plan, TestVerdict::Omitted, "listing cancelled".into())
                    .await;
                return;
            }
            Some(Ok((Execute2Outcome::Malformed(reason), _))) => {
                report_target_failure(
                    ctx,
                    &plan,
                    TestVerdict::InfraFailure,
                    format!("listing failed: {}", reason.detail()),
                )
                .await;
                return;
            }

            Some(Err(e)) => {
                report_target_failure(
                    ctx,
                    &plan,
                    TestVerdict::Fatal,
                    format!("listing RPC failed: {e:#}"),
                )
                .await;
                return;
            }
            None => unreachable!(),
        },
    };
    if let Err(details) = validate_cgroup_memory_overrides(&plan, config, &tests) {
        report_target_failure(ctx, &plan, TestVerdict::Fatal, details).await;
        return;
    }

    // ---- Shard filter (deterministic hash of target ⊕ test name) ----
    let kept = shard_filter(&plan.spec.display, &tests, config);
    if kept.is_empty() {
        // Nothing to run in this shard; still report discovery of zero tests.
        if let Err(e) = ctx
            .orch
            .report_tests_discovered(plan.spec.handle_proto(), plan.spec.suite.clone(), vec![])
            .await
        {
            eprintln!(
                "quokka: failed to report discovery for {}: {e:#}",
                plan.spec.display
            );
        }
        return;
    }

    // Report discovery of exactly this shard's tests (so reported >= discovered).
    let discovered: Vec<String> = kept.iter().map(|t| t.name.clone()).collect();

    let discovered_tids: Vec<TestIdentity> = tests
        .iter()
        .map(|t| TestIdentity {
            target: plan.spec.display.clone(),
            name: t.name.clone(),
            variant: config.variant.clone(),
        })
        .collect();
    // A closed reporter channel means the reporter task died; that is detected
    // by the run loop (which forces a failing verdict), so a dropped send here
    // cannot leave a falsely green run.
    if let Err(error) = ctx
        .report_tx
        .send(ReporterMessage::Discovered(discovered_tids))
        .await
    {
        eprintln!(
            "quokka: failed to report discovered tests for {}: {error}",
            plan.spec.display
        );
    }

    if let Err(e) = ctx
        .orch
        .report_tests_discovered(
            plan.spec.handle_proto(),
            plan.spec.suite.clone(),
            discovered,
        )
        .await
    {
        eprintln!(
            "quokka: failed to report discovery for {}: {e:#}",
            plan.spec.display
        );
    }

    if config.libtest_list_only {
        emit_list_only_stdout(&config.effective_libtest_args(), &kept);
        return;
    }

    // ---- Batch + order (longest-first) ----
    let execution_batches = build_batches(
        &plan.spec.display,
        &kept,
        config,
        &ctx.estimates,
        plan.translator.parser_capability(),
        plan.batch_mode,
    );

    // ---- Fan out actions (bounded, per-target permit) ----
    let per_target_sem = Arc::new(Semaphore::new(config.limits.max_inflight_per_target));
    let mut actions = JoinSet::new();
    let repeats = plan.repeat.count();
    for batch in execution_batches {
        let expected_members = match &batch {
            batching::TestSelection::All => kept.iter().map(|t| t.name.clone()).collect(),
            batching::TestSelection::Explicit(g) => g.clone(),
        };
        for repeat_index in 0..repeats {
            let ctx = ctx.clone();
            let plan = plan.clone();
            let selection = batch.clone();
            let expected_members = expected_members.clone();
            let per_target_sem = per_target_sem.clone();
            actions.spawn(async move {
                let finished = execute_test_action(
                    &ctx,
                    &plan,
                    selection,
                    expected_members,
                    repeat_index,
                    0,
                    per_target_sem,
                    PriorActionState::default(),
                )
                .await;
                // A closed reporter channel means the reporter task died, which
                // the run loop detects and turns into a failing verdict; a
                // dropped result here cannot leave a falsely green run.
                if let Err(error) = ctx
                    .report_tx
                    .send(ReporterMessage::Finished(finished))
                    .await
                {
                    eprintln!("quokka: failed to report finished tests: {error}");
                }
            });
        }
    }
    if drain_joinset(&mut actions).await {
        report_target_failure(
            ctx,
            &plan,
            TestVerdict::Fatal,
            "a test action task panicked".into(),
        )
        .await;
    }
}

fn emit_list_only_stdout(user_args: &[String], tests: &[crate::listing::TestCase]) {
    let output = libtest_list_output(user_args, tests);
    if output.is_empty() {
        return;
    }
    let mut stdout = std::io::stdout().lock();
    if let Err(e) = stdout
        .write_all(output.as_bytes())
        .and_then(|_| stdout.flush())
    {
        eprintln!("quokka: failed to write list output: {e}");
    }
}

fn emit_libtest_help_stdout() {
    let output = libtest_help_output("quokka");
    let mut stdout = std::io::stdout().lock();
    if let Err(e) = stdout
        .write_all(output.as_bytes())
        .and_then(|_| stdout.flush())
    {
        eprintln!("quokka: failed to write help output: {e}");
    }
}

/// Keep only the tests this shard owns. Membership is a pure function of test
/// identity and shard count — `fnv1a(target ⊕ name) % count` — NOT historical
/// duration. A duration-balanced (LPT) partition would be load-optimal, but it
/// reads the mutable advisory DB: two shard invocations that observed different
/// DB snapshots (or wrote per-shard DBs that diverged) would compute different
/// partitions, so the union across shards could DROP a test or run it TWICE. A
/// content-stable hash makes the union provably the full set and the shards
/// provably disjoint, independent of DB state, listing order, or timing. The DB
/// still informs within-shard ordering (longest-first) in [`build_batches`].
fn shard_filter(
    target: &str,
    tests: &[crate::listing::TestCase],
    config: &RunnerConfig,
) -> Vec<crate::listing::TestCase> {
    if !config.shard.is_sharded() {
        return tests.to_vec();
    }
    tests
        .iter()
        .filter(|t| {
            let key = format!("{target}\u{1}{}", t.name);
            (fnv1a(key.as_bytes()) % u64::from(config.shard.count)) as u16 == config.shard.index
        })
        .cloned()
        .collect()
}

/// Collapse tests into batched actions and order them longest-first.
fn build_batches(
    target: &str,
    tests: &[crate::listing::TestCase],
    config: &RunnerConfig,
    db: &DurationDb,
    capability: crate::translator::DemuxCapability,
    target_batch_mode: Option<batching::BatchMode>,
) -> Vec<batching::TestSelection<String>> {
    #[derive(Clone)]
    struct LocalBatchInput {
        name: String,
        estimate: DurationEstimate,
    }

    impl batching::Batchable for LocalBatchInput {
        fn weight_ms(&self) -> u64 {
            self.estimate.weight_ms(UNSEEN_WEIGHT_MS)
        }
        fn p50_ms(&self) -> u64 {
            self.estimate.p50_ms(0)
        }
    }

    let inputs: Vec<LocalBatchInput> = tests
        .iter()
        .map(|t| LocalBatchInput {
            name: t.name.to_owned(),
            estimate: db.estimate(
                None,
                &TestIdentity {
                    target: target.to_owned(),
                    name: t.name.to_owned(),
                    variant: config.variant.clone(),
                },
            ),
        })
        .collect();

    let batch_mode =
        if config.cgroup_granularity == crate::execution::CgroupGranularity::LogicalTest {
            // Logical-test supervision needs one requested name and one logical
            // command at index zero. Force the action selection shape before any
            // Execute2 request is built.
            batching::BatchMode::PerTest
        } else {
            match capability {
                crate::translator::DemuxCapability::NameAttributable => {
                    target_batch_mode.unwrap_or(config.batch_mode)
                }
                crate::translator::DemuxCapability::SingletonOnly => batching::BatchMode::PerTest,
            }
        };

    use crate::batching::Batcher;
    let selections = if batch_mode == batching::BatchMode::Target && config.shard.is_sharded() {
        vec![batching::TestSelection::Explicit(inputs)]
    } else {
        batch_mode.partition(&inputs)
    };
    let mut batches: Vec<batching::TestSelection<String>> = selections
        .into_iter()
        .map(|selection| match selection {
            batching::TestSelection::All => batching::TestSelection::All,
            batching::TestSelection::Explicit(group) => {
                batching::TestSelection::Explicit(group.into_iter().map(|t| t.name).collect())
            }
        })
        .collect();
    // Longest-first by the heaviest member of each batch.
    batches.sort_by_key(|selection| {
        std::cmp::Reverse(match selection {
            batching::TestSelection::All => tests
                .iter()
                .map(|t| {
                    db.estimate(
                        None,
                        &TestIdentity {
                            target: target.to_owned(),
                            name: t.name.to_owned(),
                            variant: config.variant.clone(),
                        },
                    )
                    .weight_ms(UNSEEN_WEIGHT_MS)
                })
                .max()
                .unwrap_or(0),
            batching::TestSelection::Explicit(group) => group
                .iter()
                .map(|n| {
                    db.estimate(
                        None,
                        &TestIdentity {
                            target: target.to_owned(),
                            name: n.to_owned(),
                            variant: config.variant.clone(),
                        },
                    )
                    .weight_ms(UNSEEN_WEIGHT_MS)
                })
                .max()
                .unwrap_or(0),
        })
    });
    batches
}

/// The result of running one group (batch) of testcases once, after bounded
/// infra retries.
enum GroupOutcome {
    /// The action completed; per-name observations were decoded from harness
    /// output. A name absent from the map has no parsed terminal result. `raw`
    /// carries the action's process outcome for fail-closed classification.
    Observed {
        observations: FxHashMap<String, PerTestObservation>,
        raw: ProcessOutcome,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        action_oom: Option<String>,
        execution_time: Duration,
        max_memory: Option<u64>,
        execution: AttemptExecution,
    },
    /// The whole action failed (cancellation / RPC error); the status applies to
    /// every member of the group and is never a fresh DB observation.
    GroupFailed {
        status: TestVerdict,
        details: String,
        execution: AttemptExecution,
        duration: AttemptDuration,
        disposition: GroupFailureDisposition,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GroupFailureDisposition {
    Incomplete,
    CompleteActionTimeout,
}

impl GroupFailureDisposition {
    fn integrity(self) -> ResultIntegrity {
        match self {
            Self::Incomplete => ResultIntegrity::MissingTerminalResult,
            Self::CompleteActionTimeout => ResultIntegrity::Complete,
        }
    }
}

/// Verdict for a requested test name with no parsed terminal result.
///
/// - `whole_unit`: the reconciled name is a synthetic whole-unit name
///   (`<doctests>`/`<binary>`) whose verdict IS the process exit code — there is
///   no per-test line to expect for it (an empty doc-test run, or a custom
///   binary whose exit code is the whole result).
///
/// Otherwise a clean action exit cannot establish that this requested test
/// passed, so the missing terminal result is fatal.
fn absent_name_verdict(
    raw: ProcessOutcome,
    whole_unit: bool,
    singleton: bool,
    stdout: &[u8],
    stderr: &[u8],
) -> (TestVerdict, String) {
    match raw {
        ProcessOutcome::Finished { exit_code: 0 } if whole_unit => {
            (TestVerdict::Pass, String::new())
        }
        ProcessOutcome::Finished { exit_code: 0 } => (
            TestVerdict::Fatal,
            details_with_streams(
                "harness exited 0 but emitted no parseable result for this test".to_owned(),
                stdout,
                stderr,
            ),
        ),
        // A nonzero exit for a singleton is that test's failure; for a
        // multi-member batch it cannot be attributed, so the member is FATAL and
        // the caller's per-test isolation re-run resolves it with a precise
        // singleton exit code.
        ProcessOutcome::Finished { .. } if singleton => (
            TestVerdict::Fail,
            details_with_streams(
                "test process exited nonzero with no harness-reported result".to_owned(),
                stdout,
                stderr,
            ),
        ),
        ProcessOutcome::Finished { .. } => (
            TestVerdict::Fatal,
            details_with_streams(
                "test produced no result in batch output".to_owned(),
                &[],
                &[],
            ),
        ),
        ProcessOutcome::TimedOut { .. } => (
            TestVerdict::Timeout,
            details_with_streams("execution timed out".to_owned(), stdout, stderr),
        ),
    }
}

fn details_with_streams(mut details: String, stdout: &[u8], stderr: &[u8]) -> String {
    for (label, output) in [("stdout", stdout), ("stderr", stderr)] {
        let output = String::from_utf8_lossy(output).trim_end().to_owned();
        if output.is_empty() {
            continue;
        }
        if !details.is_empty() {
            details.push('\n');
        }
        details.push_str("---- ");
        details.push_str(label);
        details.push_str(" ----\n");
        details.push_str(&output);
    }
    details
}

/// Per-name best-so-far across attempts (pass-if-any-pass): once a test passes in
/// any attempt that pass is kept, so a later flaky failure never overwrites it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BestObs {
    status: TestVerdict,
    integrity: ResultIntegrity,
    details: String,
    duration: AttemptDuration,
    max_memory: Option<u64>,
}

#[derive(Default)]
struct PriorActionState {
    best: FxHashMap<String, BestObs>,
    attempts: FxHashMap<String, Vec<DbObservation>>,
    attempt_history: FxHashMap<String, Vec<AttemptObservation>>,
}

impl BestObs {
    fn missing() -> Self {
        BestObs {
            status: TestVerdict::Fatal,
            integrity: ResultIntegrity::MissingTerminalResult,
            details: "test produced no result".to_owned(),
            duration: AttemptDuration::Unmeasured,
            max_memory: None,
        }
    }

    fn into_outcome(self) -> TestOutcome {
        TestOutcome {
            status: self.status,
            integrity: self.integrity,
            details: self.details,
            duration: self.duration,
            max_memory: self.max_memory,
        }
    }
}

fn merge_best(best: &mut FxHashMap<String, BestObs>, name: &str, incoming: BestObs) {
    if incoming.integrity == ResultIntegrity::MissingTerminalResult {
        best.insert(name.to_owned(), incoming);
        return;
    }
    match best.get(name) {
        Some(existing) if existing.integrity == ResultIntegrity::MissingTerminalResult => {}
        // Keep an earlier pass over any later result (pass-if-any-pass).
        Some(existing) if !existing.status.is_failure() => {}
        _ => {
            best.insert(name.to_owned(), incoming);
        }
    }
}

async fn acquire_per_target_admission(
    per_target_sem: &Semaphore,
) -> tokio::sync::SemaphorePermit<'_> {
    per_target_sem
        .acquire()
        .await
        .expect("per-target semaphore")
}

/// Run one group of testcases once, with bounded infra (RE-queue-timeout) retries.
async fn run_group(
    ctx: &TargetCtx,
    plan: &TargetPlan,
    selection: &batching::TestSelection<String>,
    names: &[String],
    repeat_index: u32,
    execution_attempt_index: u32,
    per_target_sem: &Arc<Semaphore>,
) -> GroupOutcome {
    let config = &ctx.config;
    let mut infra_attempt = 0u32;
    loop {
        let target_permit = acquire_per_target_admission(per_target_sem).await;
        let attempt_index = execution_attempt_index + infra_attempt;
        let mut has_unseen = false;
        if config.duration_db.cache_busts_unseen_tests() {
            for name in names {
                let test_id = TestIdentity {
                    target: plan.spec.display.to_owned(),
                    name: name.to_owned(),
                    variant: config.variant.clone(),
                };
                if matches!(
                    ctx.estimates.estimate(None, &test_id),
                    crate::duration_db::DurationEstimate::Unseen
                ) {
                    has_unseen = true;
                    break;
                }
            }
        }
        let mut caching = crate::caching::TestExecutionCaching::resolve(
            plan.cache_class,
            config.variant.is_default(),
            plan.repeat.is_stress(),
            attempt_index,
        );
        if has_unseen {
            caching = crate::caching::TestExecutionCaching::Disabled;
        }
        let name_refs = match selection {
            batching::TestSelection::All => Vec::new(),
            batching::TestSelection::Explicit(selected_names) => {
                selected_names.iter().map(String::as_str).collect()
            }
        };
        let commands = vec![crate::execution::LogicalCommand {
            index: 0,
            appended: plan.translator.execution_args(
                &name_refs,
                plan.ignored,
                &config.extra_test_args,
            ),
        }];

        let repeat_count = if plan.repeat.is_stress() {
            Some(u64::from(repeat_index))
        } else {
            None
        };

        let testcases = match &selection {
            batching::TestSelection::All => vec![],
            batching::TestSelection::Explicit(g) => {
                if g.len() == 1 {
                    g.clone()
                } else {
                    let mut modules: Vec<&str> = g
                        .iter()
                        .map(|t| {
                            if let Some(idx) = t.rfind("::") {
                                &t[..idx]
                            } else {
                                "(root)"
                            }
                        })
                        .collect();
                    modules.sort_unstable();
                    modules.dedup();

                    let mods_str = if modules.len() <= 3 {
                        modules.join(", ")
                    } else {
                        format!(
                            "{}, {}, {} and {} more",
                            modules[0],
                            modules[1],
                            modules[2],
                            modules.len() - 3
                        )
                    };
                    vec![format!("{} ({} tests)", mods_str, g.len())]
                }
            }
        };
        let resource_marker_token = ResourceMarkerToken::from_stable_fields(
            "testing",
            std::iter::once(plan.spec.display.as_str()).chain(
                commands
                    .iter()
                    .flat_map(|command| command.appended.iter().map(String::as_str)),
            ),
        );
        let cgroup_memory_max = match cgroup_memory_max_for_action(
            config.cgroup_memory_max,
            &config.cgroup_memory_max_overrides,
            &plan.spec.display,
            names,
        ) {
            Ok(value) => value,
            Err(details) => {
                return GroupOutcome::GroupFailed {
                    status: TestVerdict::Fatal,
                    details,
                    execution: AttemptExecution::Unknown,
                    duration: AttemptDuration::Unmeasured,
                    disposition: GroupFailureDisposition::Incomplete,
                };
            }
        };
        let request = build_testing_request(TestingRequest {
            target: crate::proto::test::ConfiguredTargetHandle {
                id: plan.spec.handle.0,
            },
            suite: plan.spec.suite.clone(),
            testcases,
            base_command: crate::execution::build_cmd(&plan.spec, &[]),
            commands,
            env: crate::execution::build_test_env(
                &plan.spec,
                &config.extra_env,
                &config.declared_output_env,
            ),
            variant: config.variant.identity(),
            repeat_count,
            profile: plan.testing_profile(config),
            caching,
            logical_timeout: plan.timeout,
            cgroup_granularity: config.cgroup_granularity,
            cgroup_memory_max,
            resource_marker_token: resource_marker_token.clone(),
        });

        let response = {
            // The per-target permit also bounds request preparation. Acquire the
            // scarce global permit only immediately before issuing the RPC so a
            // target parked on its own gate cannot pin the global bound.
            let _global = ctx.global_sem.acquire().await.expect("global semaphore");
            ctx.orch.execute2(request).await
        };
        drop(target_permit);

        match response {
            Ok(response) => match decode_response(response) {
                Execute2Outcome::Completed(action) => {
                    let execution = AttemptExecution::from_exec_kind(action.exec_kind);
                    let mut observations = plan
                        .translator
                        .parse_results(&action.stdout, &action.stderr);
                    let resource_failures =
                        parse_resource_failures(&action.stderr, &resource_marker_token);
                    let mut action_oom = None;
                    for failure in resource_failures {
                        match failure {
                            ResourceFailure::LogicalTimeout { index, details } => {
                                if config.cgroup_granularity
                                    != crate::execution::CgroupGranularity::LogicalTest
                                    || names.len() != 1
                                    || index != 0
                                {
                                    return GroupOutcome::GroupFailed {
                                        status: TestVerdict::InfraFailure,
                                        details: format!(
                                            "impossible logical resource marker index={index}: {details}"
                                        ),
                                        execution,
                                        duration: AttemptDuration::Measured(action.execution_time),
                                        disposition: GroupFailureDisposition::Incomplete,
                                    };
                                }
                                let Some(name) = names.get(index) else {
                                    return GroupOutcome::GroupFailed {
                                        status: TestVerdict::InfraFailure,
                                        details: format!(
                                            "resource supervisor reported unknown logical index {index}"
                                        ),
                                        execution,
                                        duration: AttemptDuration::Measured(action.execution_time),
                                        disposition: GroupFailureDisposition::Incomplete,
                                    };
                                };
                                observations.insert(
                                    name.clone(),
                                    PerTestObservation {
                                        status: TestVerdict::Timeout,
                                        details,
                                        framework_case_duration: None,
                                    },
                                );
                            }
                            ResourceFailure::LogicalOom { index, details } => {
                                if config.cgroup_granularity
                                    != crate::execution::CgroupGranularity::LogicalTest
                                    || names.len() != 1
                                    || index != 0
                                {
                                    return GroupOutcome::GroupFailed {
                                        status: TestVerdict::InfraFailure,
                                        details: format!(
                                            "impossible logical resource marker index={index}: {details}"
                                        ),
                                        execution,
                                        duration: AttemptDuration::Measured(action.execution_time),
                                        disposition: GroupFailureDisposition::Incomplete,
                                    };
                                }
                                let Some(name) = names.get(index) else {
                                    return GroupOutcome::GroupFailed {
                                        status: TestVerdict::InfraFailure,
                                        details: format!(
                                            "resource supervisor reported unknown logical index {index}"
                                        ),
                                        execution,
                                        duration: AttemptDuration::Measured(action.execution_time),
                                        disposition: GroupFailureDisposition::Incomplete,
                                    };
                                };
                                observations.insert(
                                    name.clone(),
                                    PerTestObservation {
                                        status: TestVerdict::Fail,
                                        details,
                                        framework_case_duration: None,
                                    },
                                );
                            }
                            ResourceFailure::ActionTimeout { details } => {
                                if config.cgroup_granularity
                                    != crate::execution::CgroupGranularity::Action
                                {
                                    return GroupOutcome::GroupFailed {
                                        status: TestVerdict::InfraFailure,
                                        details: format!(
                                            "impossible action resource marker: {details}"
                                        ),
                                        execution,
                                        duration: AttemptDuration::Measured(action.execution_time),
                                        disposition: GroupFailureDisposition::Incomplete,
                                    };
                                }
                                return GroupOutcome::GroupFailed {
                                    status: TestVerdict::Timeout,
                                    details,
                                    execution,
                                    duration: AttemptDuration::Measured(action.execution_time),
                                    disposition: GroupFailureDisposition::CompleteActionTimeout,
                                };
                            }
                            ResourceFailure::ActionOom { details } => {
                                if config.cgroup_granularity
                                    != crate::execution::CgroupGranularity::Action
                                {
                                    return GroupOutcome::GroupFailed {
                                        status: TestVerdict::InfraFailure,
                                        details: format!(
                                            "impossible action resource marker: {details}"
                                        ),
                                        execution,
                                        duration: AttemptDuration::Measured(action.execution_time),
                                        disposition: GroupFailureDisposition::Incomplete,
                                    };
                                }
                                action_oom = Some(details);
                            }
                            ResourceFailure::Setup { details } => {
                                return GroupOutcome::GroupFailed {
                                    status: TestVerdict::InfraFailure,
                                    details,
                                    execution,
                                    duration: AttemptDuration::Measured(action.execution_time),
                                    disposition: GroupFailureDisposition::Incomplete,
                                };
                            }
                            ResourceFailure::Malformed { details } => {
                                return GroupOutcome::GroupFailed {
                                    status: TestVerdict::InfraFailure,
                                    details: format!("malformed resource marker: {details}"),
                                    execution,
                                    duration: AttemptDuration::Measured(action.execution_time),
                                    disposition: GroupFailureDisposition::Incomplete,
                                };
                            }
                        }
                    }
                    return GroupOutcome::Observed {
                        observations,
                        raw: action.status,
                        stdout: action.stdout,
                        stderr: action.stderr,
                        action_oom,
                        execution_time: action.execution_time,
                        max_memory: action.max_memory_used_bytes,
                        execution,
                    };
                }
                Execute2Outcome::CancelledQueueTimeout => {
                    if infra_attempt + 1 < INFRA_MAX_ATTEMPTS {
                        infra_attempt += 1;
                        continue;
                    }
                    return GroupOutcome::GroupFailed {
                        status: TestVerdict::InfraFailure,
                        details: "RE queue timeout (retries exhausted)".to_owned(),
                        execution: AttemptExecution::Unknown,
                        duration: AttemptDuration::Unmeasured,
                        disposition: GroupFailureDisposition::Incomplete,
                    };
                }
                Execute2Outcome::CancelledUnspecified => {
                    return GroupOutcome::GroupFailed {
                        status: TestVerdict::Omitted,
                        details: "run cancelled".to_owned(),
                        execution: AttemptExecution::Unknown,
                        duration: AttemptDuration::Unmeasured,
                        disposition: GroupFailureDisposition::Incomplete,
                    };
                }
                Execute2Outcome::Malformed(reason) => {
                    return GroupOutcome::GroupFailed {
                        status: TestVerdict::InfraFailure,
                        details: reason.detail(),
                        execution: AttemptExecution::Unknown,
                        duration: AttemptDuration::Unmeasured,
                        disposition: GroupFailureDisposition::Incomplete,
                    };
                }
            },
            Err(e) => {
                return GroupOutcome::GroupFailed {
                    status: TestVerdict::Fatal,
                    details: format!("Execute2 RPC failed: {e:#}"),
                    execution: AttemptExecution::Unknown,
                    duration: AttemptDuration::Unmeasured,
                    disposition: GroupFailureDisposition::Incomplete,
                };
            }
        }
    }
}

/// Execute one (possibly batched) test action and return one result per expected
/// name. Folds the best result per name across flaky retries (a test that passes
/// on any attempt stays passed), narrows each retry to only the still-failing
/// members, and — when a multi-member batch still has failures and the batch
/// failure policy is `RerunPerTestToIsolate` — re-runs those members singly so a
/// crashed neighbour can be diagnosed without allowing a later singleton pass
/// to erase an incomplete batch observation.
async fn execute_test_action(
    ctx: &TargetCtx,
    plan: &TargetPlan,
    selection: batching::TestSelection<String>,
    expected_members: Vec<String>,
    repeat_index: u32,
    first_attempt_index: u32,
    per_target_sem: Arc<Semaphore>,
    prior: PriorActionState,
) -> Vec<FinishedTest> {
    let PriorActionState {
        mut best,
        mut attempts,
        mut attempt_history,
    } = prior;
    let mut pending: Vec<String> = expected_members.clone();
    let mut failure_attempt = 0u32;
    let mut action_timeout_attempts = 0u32;
    let mut execution_attempt = 0u32;

    loop {
        let mut group_failure_disposition = None;
        let current_selection = if failure_attempt == 0 {
            selection.clone()
        } else {
            batching::TestSelection::Explicit(pending.clone())
        };

        match run_group(
            ctx,
            plan,
            &current_selection,
            &pending,
            repeat_index,
            first_attempt_index + execution_attempt,
            &per_target_sem,
        )
        .await
        {
            GroupOutcome::Observed {
                observations,
                raw,
                stdout,
                stderr,
                action_oom,
                execution_time,
                max_memory,
                execution,
            } => {
                let is_whole_target = matches!(
                    plan.translator.listing_strategy(),
                    ListingStrategy::WholeTarget { .. } | ListingStrategy::WholeBinary { .. }
                );

                let test_names: Vec<String> = if is_whole_target
                    && !observations.is_empty()
                    && pending.len() == 1
                    && (pending[0] == crate::translator::DOCTEST_RESULT_NAME
                        || pending[0] == crate::translator::BINARY_RESULT_NAME)
                {
                    observations.keys().cloned().collect()
                } else {
                    pending.clone()
                };

                let is_batched = expected_members.len() > 1;
                let singleton = !is_batched || test_names.len() == 1;
                for name in &test_names {
                    let (status, details, framework_case_duration, from_harness, integrity) =
                        if let Some(details) = &action_oom {
                            (
                                if singleton {
                                    TestVerdict::Fail
                                } else {
                                    TestVerdict::Fatal
                                },
                                details.clone(),
                                None,
                                false,
                                if singleton {
                                    ResultIntegrity::Complete
                                } else {
                                    ResultIntegrity::MissingTerminalResult
                                },
                            )
                        } else {
                            match observations.get(name) {
                                Some(obs) => (
                                    obs.status,
                                    obs.details.clone(),
                                    obs.framework_case_duration,
                                    true,
                                    ResultIntegrity::Complete,
                                ),
                                None => {
                                    let (status, details) = absent_name_verdict(
                                        raw,
                                        is_whole_target,
                                        singleton,
                                        &stdout,
                                        &stderr,
                                    );
                                    let integrity = if is_whole_target {
                                        ResultIntegrity::Complete
                                    } else {
                                        ResultIntegrity::MissingTerminalResult
                                    };
                                    (status, details, None, false, integrity)
                                }
                            }
                        };
                    // A fresh, harness-attributed attempt is an independent flake
                    // observation. A synthesized status (a batch member with no
                    // output, or an exit-code-only verdict) is NOT recorded: it is
                    // not a clean per-test signal, and a crashed batch must not
                    // inflate an innocent member's flake rate.
                    if let (true, Some(env)) = (from_harness, execution.fresh_environment()) {
                        attempts
                            .entry(name.clone())
                            .or_default()
                            .push(DbObservation {
                                duration: execution_time,
                                failed: status.is_failure(),
                                failure_class: result::failure_class(status),
                                env,
                            });
                    }
                    attempt_history
                        .entry(name.clone())
                        .or_default()
                        .push(AttemptObservation {
                            status,
                            duration: AttemptDuration::Measured(execution_time),
                            framework_case_duration,
                            details: details.clone(),
                            execution,
                        });
                    merge_best(
                        &mut best,
                        name,
                        BestObs {
                            status,
                            integrity,
                            details,
                            duration: AttemptDuration::Measured(execution_time),
                            max_memory,
                        },
                    );
                }
            }
            GroupOutcome::GroupFailed {
                status,
                details,
                execution,
                duration,
                disposition,
            } => {
                group_failure_disposition = Some(disposition);
                for name in &pending {
                    attempt_history
                        .entry(name.clone())
                        .or_default()
                        .push(AttemptObservation {
                            status,
                            duration,
                            framework_case_duration: None,
                            details: details.clone(),
                            execution,
                        });
                    merge_best(
                        &mut best,
                        name,
                        BestObs {
                            status,
                            integrity: disposition.integrity(),
                            details: details.clone(),
                            duration,
                            max_memory: None,
                        },
                    );
                }
            }
        }

        let still_failing: Vec<String> = expected_members
            .iter()
            .filter(|n| best.get(*n).map(|b| b.status.is_failure()).unwrap_or(true))
            .cloned()
            .collect();
        if still_failing.is_empty() {
            break;
        }
        if group_failure_disposition == Some(GroupFailureDisposition::CompleteActionTimeout) {
            action_timeout_attempts += 1;
            if action_timeout_attempts < INFRA_MAX_ATTEMPTS {
                execution_attempt += 1;
                pending = still_failing;
                continue;
            }
            break;
        }
        // Flaky-retry budget: re-run only the still-failing members.
        let mut retry_pending = Vec::new();
        for name in &still_failing {
            let base_attempts = plan.retry.max_attempts();
            let is_flake = {
                let test_id = TestIdentity {
                    target: plan.spec.display.to_owned(),
                    name: name.to_owned(),
                    variant: ctx.config.variant.clone(),
                };
                ctx.estimates
                    .flake(None, &test_id)
                    .map(|f| f.failures > 0)
                    .unwrap_or(false)
            };
            let toml_attempts = if is_flake {
                ctx.config
                    .quokka_config
                    .flaky_retry
                    .as_ref()
                    .map(|c| c.attempts)
                    .unwrap_or(0)
            } else {
                0
            };
            let allowed_attempts = base_attempts.max(toml_attempts);
            if failure_attempt + 1 < allowed_attempts {
                retry_pending.push(name.clone());
            }
        }

        if !retry_pending.is_empty() {
            failure_attempt += 1;
            execution_attempt += 1;
            pending = retry_pending;
            continue;
        }
        // Retries exhausted. Isolate a multi-member batch's remaining failures so
        // a crashed neighbour does not mis-attribute results to innocent members.
        let is_batched = expected_members.len() > 1;
        if is_batched
            && ctx.config.batch_failure_policy == BatchFailurePolicy::RerunPerTestToIsolate
        {
            return isolate_failures(
                ctx,
                plan,
                &expected_members,
                &still_failing,
                BatchAccum {
                    best: &best,
                    attempts: &mut attempts,
                    attempt_history: &mut attempt_history,
                },
                repeat_index,
                first_attempt_index + execution_attempt + 1,
                &per_target_sem,
            )
            .await;
        }
        break;
    }

    let mut finished = Vec::with_capacity(expected_members.len());
    for name in &expected_members {
        let outcome = best
            .get(name)
            .cloned()
            .unwrap_or_else(BestObs::missing)
            .into_outcome();
        let observations = attempts.remove(name).unwrap_or_default();
        let history = attempt_history.remove(name).unwrap_or_default();
        finished.push(
            make_finished(
                ctx,
                plan,
                name,
                repeat_index,
                outcome,
                observations,
                history,
            )
            .await,
        );
    }
    finished
}

/// The batch's per-name accumulation handed to [`isolate_failures`].
struct BatchAccum<'a> {
    best: &'a FxHashMap<String, BestObs>,
    attempts: &'a mut FxHashMap<String, Vec<DbObservation>>,
    attempt_history: &'a mut FxHashMap<String, Vec<AttemptObservation>>,
}

/// Re-run each still-failing member of a batch as its own singleton action to
/// add per-name diagnostics; non-failing members are reported from the batch's
/// best-so-far, and each singleton inherits the batch observation so an
/// incomplete result remains absorbing. Singletons cannot re-isolate.
async fn isolate_failures(
    ctx: &TargetCtx,
    plan: &TargetPlan,
    all_names: &[String],
    still_failing: &[String],
    accum: BatchAccum<'_>,
    repeat_index: u32,
    first_attempt_index: u32,
    per_target_sem: &Arc<Semaphore>,
) -> Vec<FinishedTest> {
    let failing: FxHashSet<&str> = still_failing.iter().map(String::as_str).collect();
    let mut finished = Vec::with_capacity(all_names.len());
    for name in all_names {
        if failing.contains(name.as_str()) {
            continue;
        }
        let outcome = accum
            .best
            .get(name)
            .cloned()
            .unwrap_or_else(BestObs::missing)
            .into_outcome();
        let observations = accum.attempts.remove(name).unwrap_or_default();
        let history = accum.attempt_history.remove(name).unwrap_or_default();
        finished.push(
            make_finished(
                ctx,
                plan,
                name,
                repeat_index,
                outcome,
                observations,
                history,
            )
            .await,
        );
    }
    let mut isolated_futures = Vec::with_capacity(still_failing.len());
    for name in still_failing {
        let mut prior_best = FxHashMap::default();
        if let Some(best) = accum.best.get(name) {
            prior_best.insert(name.clone(), best.clone());
        }
        let mut prior_attempts = FxHashMap::default();
        if let Some(attempts) = accum.attempts.remove(name) {
            prior_attempts.insert(name.clone(), attempts);
        }
        let mut prior_history = FxHashMap::default();
        if let Some(history) = accum.attempt_history.remove(name) {
            prior_history.insert(name.clone(), history);
        }
        isolated_futures.push(Box::pin(execute_test_action(
            ctx,
            plan,
            batching::TestSelection::Explicit(vec![name.clone()]),
            vec![name.clone()],
            repeat_index,
            first_attempt_index,
            per_target_sem.clone(),
            PriorActionState {
                best: prior_best,
                attempts: prior_attempts,
                attempt_history: prior_history,
            },
        )));
    }
    let isolated = join_all(isolated_futures).await;
    for single in isolated {
        finished.extend(single);
    }
    finished
}

/// Build one `FinishedTest`, routing oversized logs to CAS and annotating
/// failures with owner/contacts/oncall and flake history.
async fn make_finished(
    ctx: &TargetCtx,
    plan: &TargetPlan,
    base_name: &str,
    repeat_index: u32,
    outcome: TestOutcome,
    db_observations: Vec<DbObservation>,
    attempt_history: Vec<AttemptObservation>,
) -> FinishedTest {
    let test_id = TestIdentity {
        target: plan.spec.display.to_owned(),
        name: base_name.to_owned(),
        variant: ctx.config.variant.clone(),
    };
    let run_id = RunIdentity {
        test: test_id.clone(),
        repeat: plan.repeat,
        repeat_index,
    };
    let mut details = finalize_details(ctx, DetailKind::of(outcome.status), outcome.details).await;
    append_attempt_history(&mut details, &attempt_history);
    if outcome.status.is_failure() {
        let annotation = failure_annotation(plan, ctx, &test_id);
        if !annotation.is_empty() {
            details.push('\n');
            details.push_str(&annotation);
        }
    }

    let result = build_test_result(
        &run_id,
        plan.spec.handle_proto(),
        outcome.status,
        match outcome.duration {
            AttemptDuration::Measured(duration) => Some(duration),
            AttemptDuration::Unmeasured => None,
        },
        details,
        outcome.max_memory,
    );
    FinishedTest {
        result,
        console_name: console_test_name(&run_id),
        run_id,
        test_id,
        status: outcome.status,
        integrity: outcome.integrity,
        quarantined: plan.quarantined(),
        labels: plan.spec.labels.clone(),
        db_observations,
        attempt_history,
    }
}

fn append_attempt_history(details: &mut String, attempts: &[AttemptObservation]) {
    if attempts.len() < 2 {
        return;
    }
    if !details.is_empty() {
        details.push('\n');
    }
    details.push_str("quokka attempt history:");
    for (index, attempt) in attempts.iter().enumerate() {
        use std::fmt::Write;
        let first_line = attempt.details.lines().next().unwrap_or_default();
        if let Err(error) = write!(details, "\nattempt {}: {:?}", index + 1, attempt.status) {
            eprintln!("quokka: failed to append attempt status: {error}");
        }
        match attempt.duration {
            AttemptDuration::Measured(duration) => {
                if let Err(error) = write!(details, " duration_ms={}", duration.as_millis()) {
                    eprintln!("quokka: failed to append attempt duration: {error}");
                }
            }
            AttemptDuration::Unmeasured => details.push_str(" duration=unmeasured"),
        }
        if !first_line.is_empty() {
            details.push_str(" details=");
            details.extend(first_line.chars().take(500));
        }
    }
}

/// Routing/owner/flake annotation appended to a failing test's details, e.g.
/// `[brtr: target=root//rust/foo:foo | target_platform=cfg:macos-arm64 |
/// owner=spreadsheets | oncall=sheets |
/// flaky_history: failed 4/20 recent runs, last=Timeout]`. Flake data is read
/// from the pre-run snapshot. Only failures are annotated (by the caller), so
/// passing-test output stays clean.
fn failure_annotation(plan: &TargetPlan, ctx: &TargetCtx, test_id: &TestIdentity) -> String {
    let mut parts: Vec<String> = vec![format!("target={}", plan.spec.display)];
    if let Some(target_platform) = &plan.spec.target_platform {
        parts.push(format!("target_platform={target_platform}"));
    }
    if let Owner::Team(team) = &plan.owner {
        parts.push(format!("owner={team}"));
    }
    if !plan.spec.contacts.is_empty() {
        parts.push(format!("contacts=[{}]", plan.spec.contacts.join(",")));
    }
    if let Some(oncall) = &plan.spec.oncall {
        parts.push(format!("oncall={oncall}"));
    }
    if let Some(flake) = ctx.estimates.flake(None, &test_id)
        && flake.runs > 0
    {
        let mut s = format!(
            "flaky_history: failed {}/{} recent runs",
            flake.failures, flake.runs
        );
        if let Some(class) = flake.last_failure_class {
            s.push_str(&format!(", last={class:?}"));
        }
        parts.push(s);
    }
    format!("[brtr: {}]", parts.join(" | "))
}

/// Whether a details blob belongs to a failing test. A failure is never
/// truncated inline: its complete output is printed even when it is also
/// uploaded to CAS, so a developer sees the whole failure without fetching the
/// artifact. Passing-test logs keep the bounded inline preview.
#[derive(Clone, Copy)]
enum DetailKind {
    Failure,
    Passing,
}

impl DetailKind {
    fn of(status: TestVerdict) -> Self {
        if status.is_failure() {
            DetailKind::Failure
        } else {
            DetailKind::Passing
        }
    }
}

/// Inline small logs; upload large ones to CAS and leave a short pointer.
/// Failing tests keep their full output inline regardless of size, so a failure
/// is always printed in full (see [`DetailKind`]).
async fn finalize_details(ctx: &TargetCtx, kind: DetailKind, details: String) -> String {
    match result::route_log(details.len(), ctx.config.cas_inline_limit) {
        result::LogRouting::Inline => details,
        result::LogRouting::UploadToCas => upload_details_to_cas(ctx, kind, details).await,
    }
}

/// The inline portion of a CAS-uploaded log: the complete text for a failure
/// (so the full failure is always printed), a bounded head for a passing test.
fn inline_body(kind: DetailKind, details: &str) -> String {
    match kind {
        DetailKind::Failure => details.to_owned(),
        DetailKind::Passing => truncate(details),
    }
}

async fn upload_details_to_cas(ctx: &TargetCtx, kind: DetailKind, details: String) -> String {
    let size = details.len();
    let path = std::env::temp_dir().join(format!(
        "brtr-log-{}-{}.txt",
        std::process::id(),
        // A cheap content-derived suffix avoids collisions without RNG.
        fnv1a(details.as_bytes())
    ));
    if let Err(e) = std::fs::write(&path, &details) {
        return format!(
            "[log {size} bytes; CAS upload skipped: {e}]\n{}",
            inline_body(kind, &details)
        );
    }
    match ctx
        .orch
        .upload_file_to_cas(
            path.to_string_lossy().into_owned(),
            /* ttl_seconds */ 7 * 24 * 3600,
            "rust-test".to_owned(),
        )
        .await
    {
        Ok(digest) => {
            if let Err(error) = std::fs::remove_file(&path) {
                eprintln!(
                    "quokka: failed to remove temporary CAS log {}: {error}",
                    path.display()
                );
            }
            let msg = format!(
                "[log {size} bytes uploaded to CAS: {}/{}]",
                digest.hash, digest.size_bytes
            );
            if let Err(error) = ctx.orch.attach_info_message(msg.clone()).await {
                eprintln!("quokka: failed to attach CAS log info: {error:#}");
            }
            format!("{msg}\n{}", inline_body(kind, &details))
        }
        Err(e) => {
            if let Err(error) = std::fs::remove_file(&path) {
                eprintln!(
                    "quokka: failed to remove temporary CAS log {}: {error}",
                    path.display()
                );
            }
            format!(
                "[log {size} bytes; CAS upload failed: {e:#}]\n{}",
                inline_body(kind, &details)
            )
        }
    }
}

fn truncate(details: &str) -> String {
    const HEAD: usize = 4096;
    if details.len() <= HEAD {
        return details.to_owned();
    }
    let mut end = HEAD;
    while !details.is_char_boundary(end) {
        end -= 1;
    }
    // `end` is a verified char boundary, so `get` always yields the head slice.
    let head = details.get(..end).unwrap_or(details);
    format!("{head}\n…[truncated, full log in CAS]")
}

/// Report a single target-level failure (listing failure, malformed output).
async fn report_target_failure(
    ctx: &TargetCtx,
    plan: &TargetPlan,
    status: TestVerdict,
    details: String,
) {
    report_target_failure_for_spec(
        ctx,
        &plan.spec,
        plan.quarantined(),
        plan.repeat,
        status,
        details,
    )
    .await;
}

async fn report_target_failure_for_spec(
    ctx: &TargetCtx,
    spec: &TargetSpec,
    quarantined: bool,
    repeat: RepeatKind,
    status: TestVerdict,
    details: String,
) {
    let name = format!("{} (listing)", spec.suite);
    if let Err(e) = ctx
        .orch
        .report_tests_discovered(spec.handle_proto(), spec.suite.clone(), vec![name.clone()])
        .await
    {
        eprintln!(
            "quokka: failed to report the failing-target listing for {}: {e:#}",
            spec.display
        );
    }
    let details = finalize_details(ctx, DetailKind::of(status), details).await;
    let test_id = TestIdentity {
        target: spec.display.clone(),
        name: spec.display.clone(), // Target failures use the target name as the base test name.
        variant: ctx.config.variant.clone(),
    };
    let run_id = RunIdentity {
        test: test_id.clone(),
        repeat,
        repeat_index: 0,
    };
    let result = build_test_result(&run_id, spec.handle_proto(), status, None, details, None);
    // A closed reporter channel means the reporter task died, which the run loop
    // detects and turns into a failing verdict; this drop cannot hide a failure.
    if let Err(e) = ctx
        .report_tx
        .send(ReporterMessage::Finished(vec![FinishedTest {
            result,
            console_name: console_test_name(&run_id),
            run_id,
            test_id,
            status,
            integrity: ResultIntegrity::MissingTerminalResult,
            quarantined,
            labels: spec.labels.clone(),
            // A listing/target-level failure is not a per-test duration sample.
            db_observations: Vec::new(),
            attempt_history: Vec::new(),
        }]))
        .await
    {
        eprintln!(
            "quokka: failed to queue target failure for {}: {e}",
            spec.display
        );
    }
}

/// Whole-target / whole-binary targets: no listing, one action (run all),
/// reported as a single synthetic test (exit-code verdict). Honors stress.

/// FNV-1a over bytes, used for deterministic shard/file-name derivation without
/// RNG (which is unavailable in this environment and undesirable for determinism).
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batching::{BatchMode, TestSelection};
    use crate::cli::SessionContext;
    use crate::listing::TestCaseKind;
    use crate::translator::DemuxCapability;
    use std::num::NonZeroUsize;

    fn tally() -> Tally {
        Tally {
            total: 2,
            passed: 1,
            failed: 1,
            quarantined_failed: 0,
            skipped: 0,
        }
    }

    #[test]
    fn ci_report_keeps_framework_case_time_separate_from_action_time() {
        let test_id = TestIdentity {
            target: "root//rust/example:tests".to_owned(),
            name: "module::case".to_owned(),
            variant: crate::variant::Variant::Default,
        };
        let run_id = RunIdentity {
            test: test_id.clone(),
            repeat: RepeatKind::Once,
            repeat_index: 0,
        };
        let finished = FinishedTest {
            result: build_test_result(
                &run_id,
                crate::proto::test::ConfiguredTargetHandle { id: 1 },
                TestVerdict::Pass,
                Some(Duration::from_millis(500)),
                String::new(),
                None,
            ),
            console_name: console_test_name(&run_id),
            run_id,
            test_id,
            status: TestVerdict::Pass,
            integrity: ResultIntegrity::Complete,
            quarantined: false,
            labels: Vec::new(),
            db_observations: Vec::new(),
            attempt_history: vec![AttemptObservation {
                status: TestVerdict::Pass,
                duration: AttemptDuration::Measured(Duration::from_millis(500)),
                framework_case_duration: Some(Duration::from_millis(125)),
                details: String::new(),
                execution: AttemptExecution::Physical(crate::duration_db::Environment::Remote),
            }],
        };

        let report = ci_test_value(&finished);
        assert_eq!(report["action_or_batch_duration_seconds"], 0.5);
        assert_eq!(report["framework_case_duration_seconds"], 0.125);
        assert_eq!(
            report["attempts"][0]["action_or_batch_duration_seconds"],
            0.5
        );
        assert_eq!(report["attempts"][0]["duration_seconds"], 0.125);
    }

    fn ci_finished_fixture() -> FinishedTest {
        let test_id = TestIdentity {
            target: "root//rust/example:tests".to_owned(),
            name: "writer_case".to_owned(),
            variant: crate::variant::Variant::Default,
        };
        let run_id = RunIdentity {
            test: test_id.clone(),
            repeat: RepeatKind::Once,
            repeat_index: 0,
        };
        FinishedTest {
            result: build_test_result(
                &run_id,
                crate::proto::test::ConfiguredTargetHandle { id: 1 },
                TestVerdict::Pass,
                Some(Duration::from_millis(1)),
                String::new(),
                None,
            ),
            console_name: console_test_name(&run_id),
            run_id,
            test_id,
            status: TestVerdict::Pass,
            integrity: ResultIntegrity::Complete,
            quarantined: false,
            labels: Vec::new(),
            db_observations: Vec::new(),
            attempt_history: Vec::new(),
        }
    }

    #[test]
    fn ci_report_write_failures_preserve_previous_destination() {
        let _fault_guard = CI_REPORT_FAULT_LOCK.lock().expect("fault lock");
        let points = [
            CiReportIoPoint::Create,
            CiReportIoPoint::Header,
            CiReportIoPoint::Record,
            CiReportIoPoint::FileSync,
            CiReportIoPoint::Rename,
            CiReportIoPoint::ParentSync,
        ];
        for point in points {
            let root = std::env::temp_dir().join(format!(
                "quokka-ci-writer-{}-{}",
                std::process::id(),
                point as u8
            ));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("writer test directory");
            let destination = root.join("report.json");
            let sentinel = b"previous report bytes";
            fs::write(&destination, sentinel).expect("sentinel report");
            CI_REPORT_FAULT.store(point as u8, Ordering::SeqCst);
            let finished = ci_finished_fixture();
            let verdict_before = finished.status;
            let context = SessionContext {
                host_platform: None,
                trace_id: None,
            };
            let error = match point {
                CiReportIoPoint::Create | CiReportIoPoint::Header => {
                    CiTestReportWriter::new(destination.clone(), &context)
                        .err()
                        .expect("injected construction failure")
                }
                CiReportIoPoint::Record => {
                    let mut writer =
                        CiTestReportWriter::new(destination.clone(), &context).expect("writer");
                    let error = writer
                        .write_test(&finished)
                        .expect_err("injected record failure");
                    assert!(writer.cleanup(false).is_none());
                    error
                }
                CiReportIoPoint::FileSync
                | CiReportIoPoint::Rename
                | CiReportIoPoint::ParentSync => {
                    let mut writer =
                        CiTestReportWriter::new(destination.clone(), &context).expect("writer");
                    writer.write_test(&finished).expect("record");
                    writer.finish().err().expect("injected finish failure")
                }
                // This input array is intentionally limited to publication
                // points; rollback-specific faults are covered below.
                CiReportIoPoint::RollbackRemove
                | CiReportIoPoint::RollbackRestore
                | CiReportIoPoint::RollbackParentSync => {
                    unreachable!("rollback fault is not a publication failure")
                }
            };
            assert!(error.contains("injected CI report"), "{error}");
            assert_eq!(finished.status, verdict_before);
            assert_eq!(
                fs::read(&destination).expect("destination remains"),
                sentinel
            );
            let residue = fs::read_dir(&root)
                .expect("report directory")
                .filter_map(Result::ok)
                .map(|entry| entry.file_name())
                .filter(|name| name != "report.json")
                .collect::<Vec<_>>();
            assert!(
                residue.is_empty(),
                "report residue for {point:?}: {residue:?}"
            );
            CI_REPORT_FAULT.store(0, Ordering::SeqCst);
            CI_REPORT_ROLLBACK_FAULT.store(0, Ordering::SeqCst);
            fs::remove_dir_all(root).expect("remove writer test directory");
        }
    }

    #[test]
    fn ci_report_rollback_failures_retain_recovery_artifact() {
        let _fault_guard = CI_REPORT_FAULT_LOCK.lock().expect("fault lock");
        let points = [
            CiReportIoPoint::RollbackRemove,
            CiReportIoPoint::RollbackRestore,
            CiReportIoPoint::RollbackParentSync,
        ];
        for point in points {
            let root = std::env::temp_dir().join(format!(
                "quokka-ci-rollback-{}-{}",
                std::process::id(),
                point as u8
            ));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("writer test directory");
            let destination = root.join("report.json");
            let sentinel = b"previous report bytes";
            fs::write(&destination, sentinel).expect("sentinel report");

            let context = SessionContext {
                host_platform: None,
                trace_id: None,
            };
            let finished = ci_finished_fixture();
            let verdict_before = finished.status;
            let mut writer =
                CiTestReportWriter::new(destination.clone(), &context).expect("writer");
            writer.write_test(&finished).expect("record");
            let backup = writer.backup.clone();

            // Make finish_inner fail after installing the new report, then fail
            // the selected rollback operation. The writer must retain the only
            // known-good bytes and name where they can be recovered.
            CI_REPORT_FAULT.store(CiReportIoPoint::ParentSync as u8, Ordering::SeqCst);
            CI_REPORT_ROLLBACK_FAULT.store(point as u8, Ordering::SeqCst);
            let error = writer.finish().err().expect("rollback failure");
            assert!(error.contains("rollback failed"), "{error}");
            assert!(error.contains("recovery retained at"), "{error}");
            assert_eq!(finished.status, verdict_before);

            match point {
                CiReportIoPoint::RollbackRemove | CiReportIoPoint::RollbackRestore => {
                    assert_eq!(fs::read(&backup).expect("recovery backup"), sentinel);
                    assert!(error.contains(&backup.display().to_string()), "{error}");
                    assert!(backup.exists(), "recovery backup was removed");
                }
                CiReportIoPoint::RollbackParentSync => {
                    assert_eq!(
                        fs::read(&destination).expect("restored destination"),
                        sentinel
                    );
                    assert!(
                        error.contains(&destination.display().to_string()),
                        "{error}"
                    );
                    assert!(!backup.exists(), "restored backup was not consumed");
                }
                _ => unreachable!("only rollback fault points are tested"),
            }
            let temporary_residue = fs::read_dir(&root)
                .expect("report directory")
                .filter_map(Result::ok)
                .map(|entry| entry.file_name())
                .any(|name| name.to_string_lossy().ends_with(".tmp"));
            assert!(!temporary_residue, "temporary report was not removed");

            if backup.exists() {
                fs::remove_file(&backup).expect("remove retained recovery backup");
            }
            fs::remove_dir_all(root).expect("remove writer test directory");
            CI_REPORT_FAULT.store(0, Ordering::SeqCst);
            CI_REPORT_ROLLBACK_FAULT.store(0, Ordering::SeqCst);
        }
    }

    #[test]
    fn summary_labels_the_host_platform() {
        // The session-wide platform the runner executes on is rendered as
        // `[host=…]`, distinct from a target's build-for platform.
        let context = SessionContext {
            host_platform: Some("mac".to_owned()),
            trace_id: None,
        };
        let summary = session_summary(&tally(), &context);
        assert!(summary.ends_with(" [host=mac]"), "got: {summary}");
    }

    #[test]
    fn summary_omits_host_platform_when_absent() {
        let context = SessionContext {
            host_platform: None,
            trace_id: None,
        };
        let summary = session_summary(&tally(), &context);
        assert!(!summary.contains("[host="), "got: {summary}");
    }

    #[test]
    fn big_label_selects_per_test_batching() {
        assert_eq!(
            target_batch_mode(&["rust:big".to_owned()]),
            Some(BatchMode::PerTest)
        );
        assert_eq!(target_batch_mode(&["rust:gpu".to_owned()]), None);
    }

    #[test]
    fn absent_cgroup_memory_override_target_fails_validation() {
        let mut config = crate::cli::parse(
            [
                "runner",
                "--executor-fd",
                "1",
                "--orchestrator-fd",
                "2",
                "--",
                "ignored",
                "--cgroup-memory-max-override",
                "root//x:tests|suite::large|3221225472",
            ]
            .iter()
            .map(|value| value.to_string())
            .collect(),
        )
        .expect("config")
        .config;
        let mut received = FxHashSet::default();
        assert_eq!(
            missing_cgroup_override_targets(&config, &received),
            vec!["root//x:tests".to_owned()]
        );

        received.insert("root//x:tests".to_owned());
        assert!(missing_cgroup_override_targets(&config, &received).is_empty());

        config.cgroup_memory_max_overrides.clear();
        assert!(missing_cgroup_override_targets(&config, &received).is_empty());
    }

    fn override_validation_config(test: &str) -> RunnerConfig {
        let mut config = crate::cli::parse(
            [
                "runner",
                "--executor-fd",
                "1",
                "--orchestrator-fd",
                "2",
                "--",
                "ignored",
            ]
            .iter()
            .map(|value| value.to_string())
            .collect(),
        )
        .expect("config")
        .config;
        config
            .cgroup_memory_max_overrides
            .push(crate::execution::CgroupMemoryMaxOverride {
                target: "root//p:t".to_owned(),
                test: test.to_owned(),
                memory_max: crate::execution::CgroupMemoryMax::default(),
            });
        config
    }

    fn override_validation_plan(test_type: &str, labels: &[&str]) -> TargetPlan {
        let spec = Arc::new(TargetSpec {
            handle: crate::ids::TargetHandle(1),
            suite: "root//p:t".to_owned(),
            display: "root//p:t".to_owned(),
            target_platform: None,
            test_type: test_type.to_owned(),
            command: vec![crate::spec::CommandArg::Verbatim("./t".to_owned())].into_boxed_slice(),
            env: Vec::new().into_boxed_slice(),
            labels: labels.iter().map(|label| (*label).to_owned()).collect(),
            contacts: Vec::new(),
            oncall: None,
        });
        let mut registry = TranslatorRegistry::new();
        if test_type == "python" {
            registry.register("python", |_| {
                Box::new(crate::translator::CustomBinaryTranslator)
            });
        }
        let config = crate::cli::parse(
            [
                "runner",
                "--executor-fd",
                "1",
                "--orchestrator-fd",
                "2",
                "--",
                "ignored",
            ]
            .iter()
            .map(|value| value.to_string())
            .collect(),
        )
        .expect("config")
        .config;
        TargetPlan::derive(spec, &config, &registry).expect("translator")
    }

    #[test]
    fn cgroup_memory_override_requires_exact_rust_big_singleton() {
        let singleton = vec![TestCase::new(
            "suite::large".to_owned(),
            TestCaseKind::Test,
            false,
        )];
        let config = override_validation_config("suite::large");
        let mut exact_plan = override_validation_plan("rust", &["rust:big"]);
        // Admission is based on the exact capability identity; singleton
        // scheduling is enforced separately by `build_batches`.
        exact_plan.batch_mode = None;
        assert!(validate_cgroup_memory_overrides(&exact_plan, &config, &singleton).is_ok());

        for (test_type, labels) in [
            ("rust", vec!["big"]),
            ("rust", vec!["python:big"]),
            ("rust", Vec::new()),
            ("python", vec!["rust:big"]),
        ] {
            let plan = override_validation_plan(test_type, &labels);
            let error = validate_cgroup_memory_overrides(&plan, &config, &singleton)
                .expect_err("invalid override capability");
            assert!(error.contains("rust:big singleton"), "{error}");
        }

        let missing = Vec::new();
        let error = validate_cgroup_memory_overrides(&exact_plan, &config, &missing)
            .expect_err("missing discovered test");
        assert!(error.contains("matched 0 tests"), "{error}");

        let duplicate = vec![
            TestCase::new("suite::large".to_owned(), TestCaseKind::Test, false),
            TestCase::new("suite::large".to_owned(), TestCaseKind::Test, false),
        ];
        let error = validate_cgroup_memory_overrides(&exact_plan, &config, &duplicate)
            .expect_err("duplicate discovered test");
        assert!(error.contains("matched 2 tests"), "{error}");
    }

    #[test]
    fn big_batch_override_keeps_each_test_singleton() {
        let mut config = crate::cli::parse(
            [
                "runner",
                "--executor-fd",
                "1",
                "--orchestrator-fd",
                "2",
                "--",
                "ignored",
                "--batch-mode",
                "fixed-chunk",
                "--chunk-size",
                "2",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        )
        .expect("config")
        .config;
        config.batch_mode = BatchMode::FixedChunk {
            size: NonZeroUsize::new(2).unwrap(),
        };
        let tests = ["a", "b", "c"]
            .into_iter()
            .map(|name| TestCase::new(name.to_owned(), TestCaseKind::Test, false))
            .collect::<Vec<_>>();

        let actual = build_batches(
            "target",
            &tests,
            &config,
            &DurationDb::ephemeral(),
            DemuxCapability::NameAttributable,
            Some(BatchMode::PerTest),
        );

        assert_eq!(
            actual,
            vec![
                TestSelection::Explicit(vec!["a".to_owned()]),
                TestSelection::Explicit(vec!["b".to_owned()]),
                TestSelection::Explicit(vec!["c".to_owned()]),
            ]
        );
    }

    #[test]
    fn logical_cgroup_forces_singletons_before_action_construction() {
        let mut config = crate::cli::parse(
            [
                "runner",
                "--executor-fd",
                "1",
                "--orchestrator-fd",
                "2",
                "--",
                "ignored",
                "--batch-mode",
                "fixed-chunk",
                "--chunk-size",
                "2",
                "--cgroup-granularity",
                "logical-test",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        )
        .expect("config")
        .config;
        config.batch_mode = BatchMode::FixedChunk {
            size: NonZeroUsize::new(2).expect("nonzero"),
        };
        let tests = ["a", "b", "c"]
            .into_iter()
            .map(|name| TestCase::new(name.to_owned(), TestCaseKind::Test, false))
            .collect::<Vec<_>>();

        let actual = build_batches(
            "target",
            &tests,
            &config,
            &DurationDb::ephemeral(),
            DemuxCapability::NameAttributable,
            None,
        );

        assert_eq!(
            actual,
            vec![
                TestSelection::Explicit(vec!["a".to_owned()]),
                TestSelection::Explicit(vec!["b".to_owned()]),
                TestSelection::Explicit(vec!["c".to_owned()]),
            ]
        );
    }

    #[test]
    fn action_cgroup_keeps_fixed_chunk_batching() {
        let mut config = crate::cli::parse(
            [
                "runner",
                "--executor-fd",
                "1",
                "--orchestrator-fd",
                "2",
                "--",
                "ignored",
                "--batch-mode",
                "fixed-chunk",
                "--chunk-size",
                "2",
                "--cgroup-granularity",
                "action",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        )
        .expect("config")
        .config;
        config.batch_mode = BatchMode::FixedChunk {
            size: NonZeroUsize::new(2).expect("nonzero"),
        };
        let tests = ["a", "b", "c"]
            .into_iter()
            .map(|name| TestCase::new(name.to_owned(), TestCaseKind::Test, false))
            .collect::<Vec<_>>();

        let actual = build_batches(
            "target",
            &tests,
            &config,
            &DurationDb::ephemeral(),
            DemuxCapability::NameAttributable,
            None,
        );

        let groups = actual
            .into_iter()
            .map(|selection| match selection {
                TestSelection::Explicit(names) => names,
                TestSelection::All => panic!("fixed-chunk mode must enumerate selected tests"),
            })
            .collect::<Vec<_>>();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups.iter().filter(|group| group.len() == 2).count(), 1);
        assert_eq!(groups.iter().filter(|group| group.len() == 1).count(), 1);
        let mut members = groups.iter().flatten().cloned().collect::<Vec<_>>();
        members.sort_unstable();
        assert_eq!(members, vec!["a", "b", "c"]);
    }

    fn verdict(raw: ProcessOutcome, whole_unit: bool, singleton: bool) -> TestVerdict {
        absent_name_verdict(raw, whole_unit, singleton, b"", b"").0
    }

    #[test]
    fn synthesized_singleton_failure_keeps_action_stderr() {
        let (status, details) = absent_name_verdict(
            ProcessOutcome::Finished { exit_code: 137 },
            false,
            true,
            b"",
            b"nobie rust test cgroup OOM: memory.max=3145728000\n",
        );

        assert_eq!(status, TestVerdict::Fail);
        assert!(details.contains("test process exited nonzero"));
        assert!(details.contains("nobie rust test cgroup OOM"));
    }

    #[test]
    fn per_test_name_without_terminal_result_never_passes() {
        assert_eq!(
            [0, 1, 101, -1].map(|exit_code| verdict(
                ProcessOutcome::Finished { exit_code },
                false,
                true,
            )),
            [
                TestVerdict::Fatal,
                TestVerdict::Fail,
                TestVerdict::Fail,
                TestVerdict::Fail,
            ]
        );
    }

    #[test]
    fn whole_unit_synthetic_name_trusts_the_exit_code() {
        assert_eq!(
            [
                verdict(ProcessOutcome::Finished { exit_code: 0 }, true, true),
                verdict(ProcessOutcome::Finished { exit_code: 101 }, true, true),
                verdict(
                    ProcessOutcome::TimedOut {
                        after: Duration::from_secs(1),
                    },
                    true,
                    true,
                ),
            ],
            [TestVerdict::Pass, TestVerdict::Fail, TestVerdict::Timeout]
        );
    }

    #[test]
    fn missing_terminal_result_is_a_failure_even_when_status_is_omitted() {
        assert_eq!(
            report_disposition(
                TestVerdict::Omitted,
                ResultIntegrity::MissingTerminalResult,
                true,
            ),
            ReportDisposition::Failure
        );
    }

    #[test]
    fn missing_terminal_is_absorbing_but_complete_timeout_can_recover() {
        let missing = BestObs {
            status: TestVerdict::Fatal,
            integrity: ResultIntegrity::MissingTerminalResult,
            details: "test produced no result".to_owned(),
            duration: AttemptDuration::Unmeasured,
            max_memory: None,
        };
        let pass = BestObs {
            status: TestVerdict::Pass,
            integrity: ResultIntegrity::Complete,
            details: String::new(),
            duration: AttemptDuration::Measured(Duration::from_secs(1)),
            max_memory: Some(1024),
        };
        let mut timeout = pass.clone();
        let mut missing_then_pass = FxHashMap::default();
        merge_best(&mut missing_then_pass, "test", missing.clone());
        merge_best(&mut missing_then_pass, "test", pass.clone());
        timeout.status = TestVerdict::Timeout;
        let mut timeout_then_pass = FxHashMap::default();
        merge_best(&mut timeout_then_pass, "test", timeout);
        merge_best(&mut timeout_then_pass, "test", pass.clone());

        assert_eq!(
            [
                missing_then_pass.remove("test"),
                timeout_then_pass.remove("test"),
            ],
            [Some(missing), Some(pass)]
        );
    }

    #[test]
    fn nonzero_exit_fails_a_singleton_and_fatals_an_unattributable_batch() {
        let raw = ProcessOutcome::Finished { exit_code: 101 };
        assert_eq!(
            [verdict(raw, false, true), verdict(raw, false, false)],
            [TestVerdict::Fail, TestVerdict::Fatal]
        );
    }

    #[tokio::test]
    async fn joinset_drain_fails_closed_for_panicked_and_cancelled_work() {
        let mut completed = JoinSet::new();
        completed.spawn(async {});

        let mut panicked = JoinSet::new();
        panicked.spawn(async { panic!("test panic") });

        let mut cancelled = JoinSet::new();
        let cancelled_task = cancelled.spawn(std::future::pending());
        cancelled_task.abort();

        assert_eq!(
            [
                drain_joinset(&mut completed).await,
                drain_joinset(&mut panicked).await,
                drain_joinset(&mut cancelled).await,
            ],
            [false, true, true]
        );
    }

    #[tokio::test]
    async fn per_target_admission_bounds_preparation_work() {
        let semaphore = Arc::new(Semaphore::new(2));
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_active = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let results = join_all((0..3).map(|index| {
            let semaphore = semaphore.clone();
            let barrier = barrier.clone();
            let active = active.clone();
            let max_active = max_active.clone();
            async move {
                let _admission = acquire_per_target_admission(&semaphore).await;
                let current = active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                max_active.fetch_max(current, std::sync::atomic::Ordering::SeqCst);
                if index < 2 {
                    barrier.wait().await;
                }
                active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                index
            }
        }))
        .await;

        #[derive(Debug, PartialEq)]
        struct AdmissionEvidence {
            results: Vec<usize>,
            max_active: usize,
            active: usize,
        }

        assert_eq!(
            AdmissionEvidence {
                results,
                max_active: max_active.load(std::sync::atomic::Ordering::SeqCst),
                active: active.load(std::sync::atomic::Ordering::SeqCst),
            },
            AdmissionEvidence {
                results: vec![0, 1, 2],
                max_active: 2,
                active: 0,
            }
        );
    }

    #[test]
    fn timeout_is_a_timeout_regardless_of_source() {
        let raw = ProcessOutcome::TimedOut {
            after: Duration::from_secs(1),
        };
        assert_eq!(
            [
                verdict(raw, true, true),
                verdict(raw, true, false),
                verdict(raw, false, true),
                verdict(raw, false, false),
            ],
            [TestVerdict::Timeout; 4]
        );
    }

    #[test]
    fn fail_console_line_carries_full_details() {
        let line = fail_console_line("my::test", "panicked at 'boom'\nstack");
        assert_eq!(line, "FAIL my::test\npanicked at 'boom'\nstack");
    }

    #[test]
    fn fail_console_line_omits_empty_details() {
        // No trailing newline when there is nothing to print after the name.
        assert_eq!(fail_console_line("my::test", ""), "FAIL my::test");
    }

    #[test]
    fn console_test_name_preserves_target_and_synthetic_name() {
        let run_id = RunIdentity {
            test: TestIdentity {
                target: "root//testing/parrot:gpui".to_owned(),
                name: "<binary>".to_owned(),
                variant: crate::variant::Variant::Default,
            },
            repeat: RepeatKind::Once,
            repeat_index: 0,
        };
        assert_eq!(
            console_test_name(&run_id),
            "root//testing/parrot:gpui::<binary>"
        );
    }

    #[test]
    fn failure_details_are_kept_in_full_when_offloaded_to_cas() {
        // A failure larger than the inline preview head must still be printed
        // in full; only passing-test logs keep the bounded preview.
        let big = "x".repeat(8192);
        assert_eq!(inline_body(DetailKind::Failure, &big), big);

        let preview = inline_body(DetailKind::Passing, &big);
        assert!(preview.len() < big.len(), "passing log should be truncated");
        assert!(
            preview.ends_with("…[truncated, full log in CAS]"),
            "got: {preview}"
        );
    }

    #[test]
    fn small_details_are_identical_for_both_kinds() {
        let small = "assertion failed: left == right";
        assert_eq!(inline_body(DetailKind::Failure, small), small);
        assert_eq!(inline_body(DetailKind::Passing, small), small);
    }
}
