//! The scheduler: the orchestration that turns intake specs into per-test
//! `Execute2` calls and reported results, under bounded, fair concurrency.
//!
//! Concurrency model: each target is driven by its own task; each action
//! acquires its per-target permit FIRST and then the scarce global permit,
//! immediately before issuing its `Execute2`. The order matters for fairness:
//! because a target's actions cannot enter the global queue until they hold one
//! of that target's per-target permits, at most `max_inflight_per_target`
//! actions of any single target are ever queued on the (FIFO) global semaphore
//! at once. So a 20k-test target and 200 small targets interleave with a bounded
//! gap — a big target cannot monopolize the global queue ahead of a later small
//! one — and the global bound is never pinned by an action parked on a per-target
//! gate. Within a target, actions are spawned longest-first (by historical p95)
//! so the long tail starts early. (The work is gRPC-bound, so this bounded-
//! interleaving guarantee from tokio's FIFO primitives is sufficient; a
//! hand-rolled SoA selection loop would buy no measurable locality.)
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

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;
use tracing::Level;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::batching::{self, BatchFailurePolicy};
use crate::caching::{self, CacheClass};
use crate::cli::RunnerConfig;
use crate::duration_db::{DurationDb, DurationEstimate};
use crate::execution::{TestingRequest, build_listing_request, build_testing_request};
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
use crate::translator::{ListingStrategy, PerTestObservation, Translator, TranslatorRegistry};
use crate::variant::RepeatKind;

const NONZERO_EXIT: i32 = 32;
/// Bounded retry budget for transient `RE_QUEUE_TIMEOUT` cancellations,
/// independent of a target's test-failure retry policy.
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
    details: String,
    duration: Duration,
    max_memory: Option<u64>,
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

enum ReporterMessage {
    Finished(Vec<FinishedTest>),
    Discovered(Vec<TestIdentity>),
}

/// One finished test, ready for the reporter to write and fold.
struct FinishedTest {
    result: crate::proto::test::TestResult,
    /// Semantic identity for duration/flake DB lookups.
    test_id: TestIdentity,
    status: TestVerdict,
    quarantined: bool,
    /// Every fresh, harness-attributed attempt of this test in this run. The
    /// reporter (sole DB writer) records each as an independent observation, so
    /// flaky tests that recover on retry still show their failures in history.
    db_observations: Vec<DbObservation>,
}

/// Run the scheduler to completion, returning the exit code for `buck2 test`.
pub async fn run(
    orch: Orchestrator,
    mut intake: mpsc::UnboundedReceiver<SpecEnvelope>,
    config: RunnerConfig,
    context: crate::cli::SessionContext,
) -> i32 {
    let config = Arc::new(config);
    let global_sem = Arc::new(Semaphore::new(config.limits.max_inflight_test_actions));
    let listing_sem = Arc::new(Semaphore::new(config.limits.max_inflight_listings));

    // Read-only duration snapshot for ordering/sharding/batching.
    let estimates = Arc::new(load_db(config.duration_db.as_deref()));

    // Single reporter task: sole orchestrator result-writer + verdict owner.
    let (report_tx, report_rx) = mpsc::channel::<ReporterMessage>(config.limits.max_report_queue);
    let reporter_db = load_db(config.duration_db.as_deref());
    let reporter = tokio::spawn(reporter_task(orch.clone(), report_rx, reporter_db, context));

    // Drive targets concurrently as their specs arrive.
    let mut target_tasks = JoinSet::new();
    while let Some(envelope) = intake.recv().await {
        match envelope {
            SpecEnvelope::Spec(spec) => {
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

/// Max live `FAIL` console lines emitted before suppression, so a wholly broken
/// run cannot flood buck2's console (the final summary still reports the total).
const MAX_CONSOLE_FAILURES: u64 = 100;

async fn reporter_task(
    orch: Orchestrator,
    mut rx: mpsc::Receiver<ReporterMessage>,
    mut db: DurationDb,
    context: crate::cli::SessionContext,
) -> RunVerdict {
    let mut verdict = RunVerdict::Pass;
    let mut tally = Tally::default();
    while let Some(msg) = rx.recv().await {
        match msg {
            ReporterMessage::Discovered(tids) => {
                for tid in tids {
                    db.record_discovered_name(&tid);
                }
            }
            ReporterMessage::Finished(batch) => {
                for finished in batch {
                    tally.total += 1;
                    let failure = finished.status.is_failure();
                    match finished.status {
                        TestVerdict::Pass => tally.passed += 1,
                        TestVerdict::Skip | TestVerdict::Omitted => tally.skipped += 1,
                        _ => {}
                    }
                    // Capture the name for a live console line before the result is moved.
                    let fail_name = if failure && !finished.quarantined {
                        Some(finished.result.name.clone())
                    } else {
                        None
                    };
                    if let Err(e) = orch.report_test_result(finished.result).await {
                        eprintln!("quokka: failed to report a test result: {e:#}");
                    }
                    if failure {
                        if finished.quarantined {
                            tally.quarantined_failed += 1;
                        } else {
                            tally.failed += 1;
                            verdict = RunVerdict::Fail;
                        }
                    }
                    // Live failure feedback on buck2's console (bounded to avoid flooding).
                    if let Some(name) = fail_name
                        && tally.failed <= MAX_CONSOLE_FAILURES
                    {
                        let _ = orch.console(Level::WARN, format!("FAIL {name}")).await;
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
    let _ = orch.console(level, summary.clone()).await;
    let _ = orch
        .report_test_session(summary, context.trace_id.clone())
        .await;

    if let Err(e) = db.flush() {
        eprintln!("quokka: failed to flush duration DB: {e:#}");
    }
    verdict
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
    if let Some(platform) = &context.host_platform {
        s.push_str(&format!(" [platform={platform}]"));
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
    /// Effective repetition for this target: the global `--stress` if set,
    /// otherwise stress implied by a `rust:stress` label, otherwise `Once`.
    repeat: RepeatKind,
}

impl TargetPlan {
    fn derive(spec: Arc<TargetSpec>, config: &RunnerConfig, registry: &TranslatorRegistry) -> Self {
        let labels: &[String] = &spec.labels;
        let translator = registry
            .resolve(&spec.test_type, config)
            .unwrap_or_else(|| {
                panic!("No translator registered for test_type: {}", spec.test_type)
            });
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
        TargetPlan {
            translator,
            ignored: config.ignored,
            cache_class: caching::cache_class(labels),
            retry: policy::retry_policy(labels, config.flaky_attempts),
            quarantine: policy::quarantine_status(labels),
            timeout: policy::test_timeout(labels).resolve(config.per_test_timeout),
            profile: profile_from_labels(labels).unwrap_or_else(|e| {
                eprintln!(
                    "quokka: conflict resolving labels for {}: {}",
                    spec.display, e
                );
                SchedulingProfile::default()
            }),
            owner: policy::owner(labels),
            repeat,
            spec,
        }
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

async fn run_target(ctx: TargetCtx, spec_proto: crate::proto::test::ExternalRunnerSpec) {
    let spec = match TargetSpec::from_proto(spec_proto) {
        Ok(spec) => spec,
        Err(e) => {
            eprintln!("quokka: dropping malformed spec: {e}");
            return;
        }
    };
    let registry = TranslatorRegistry::new();
    let plan = Arc::new(TargetPlan::derive(spec, &ctx.config, &registry));

    run_per_test_target(&ctx, plan).await;
}

async fn run_per_test_target(ctx: &TargetCtx, plan: Arc<TargetPlan>) {
    let config = &ctx.config;
    // ---- Listing (uncacheable, bounded, infra-retried) ----
    // The listing is always issued uncacheable: its output is the test-name set
    // on stdout, which buck2 drops on a cache hit, so a cached listing would
    // replay as zero tests. A transient RE-queue timeout on the listing is
    // retried (bounded) like a test action; on exhaustion it is an InfraFailure
    // (a countable failure) — never a silent OMITTED, which would drop the whole
    // target's tests as a green pass.
    let listing_outcome = match plan.translator.listing_strategy() {
        ListingStrategy::PerTestListing { request_args, .. } => {
            let listing_args = (request_args)(plan.ignored, &config.extra_test_args);
            let mut infra_attempt = 0u32;
            let outcome = loop {
                let request = build_listing_request(
                    &plan.spec,
                    &listing_args,
                    &plan.listing_profile(config),
                    config.listing_timeout,
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
                        outcome => break Ok(outcome),
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
            vec![TestCase {
                name: (*name).to_string(),
                ignored: false,
            }]
        }
        ListingStrategy::PerTestListing { parse, .. } => match listing_outcome {
            Some(Ok(Execute2Outcome::Completed(action))) => match action.status {
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
            },
            Some(Ok(Execute2Outcome::CancelledQueueTimeout)) => {
                report_target_failure(
                    ctx,
                    &plan,
                    TestVerdict::InfraFailure,
                    "listing RE queue timeout (retries exhausted)".into(),
                )
                .await;
                return;
            }
            Some(Ok(Execute2Outcome::CancelledUnspecified)) => {
                report_target_failure(ctx, &plan, TestVerdict::Omitted, "listing cancelled".into())
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

    // ---- Shard filter (deterministic hash of target ⊕ test name) ----
    let kept = shard_filter(&plan.spec.display, &tests, config);
    if kept.is_empty() {
        // Nothing to run in this shard; still report discovery of zero tests.
        let _ = ctx
            .orch
            .report_tests_discovered(plan.spec.handle_proto(), plan.spec.suite.clone(), vec![])
            .await;
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
    let _ = ctx
        .report_tx
        .send(ReporterMessage::Discovered(discovered_tids))
        .await;

    let _ = ctx
        .orch
        .report_tests_discovered(
            plan.spec.handle_proto(),
            plan.spec.suite.clone(),
            discovered,
        )
        .await;

    if config.libtest_list_only {
        return;
    }

    // ---- Batch + order (longest-first) ----
    let execution_batches = build_batches(
        &plan.spec.display,
        &kept,
        config,
        &ctx.estimates,
        plan.translator.parser_capability(),
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
                    per_target_sem,
                )
                .await;
                let _ = ctx
                    .report_tx
                    .send(ReporterMessage::Finished(finished))
                    .await;
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

    let batch_mode = match capability {
        crate::translator::DemuxCapability::NameAttributable => config.batch_mode,
        crate::translator::DemuxCapability::SingletonOnly => batching::BatchMode::PerTest,
    };

    use crate::batching::Batcher;
    let mut batches: Vec<batching::TestSelection<String>> = batch_mode
        .partition(&inputs)
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
    /// output. A name absent from the map produced no output line: either the
    /// run was a cache replay (buck2 drops stdout on a cache hit, keeping only
    /// the cached exit status), or — within a multi-member batch — that member
    /// crashed before reporting. `raw` carries the action's exit status so the
    /// caller can fall back to it (exit code is authoritative and cache-stable).
    Observed {
        observations: FxHashMap<String, PerTestObservation>,
        raw: ProcessOutcome,
        execution_time: Duration,
        max_memory: Option<u64>,
        fresh: bool,
        env: crate::duration_db::Environment,
    },
    /// The whole action failed (cancellation / RPC error); the status applies to
    /// every member of the group and is never a fresh DB observation.
    GroupFailed {
        status: TestVerdict,
        details: String,
    },
}

/// Per-name best-so-far across attempts (pass-if-any-pass): once a test passes in
/// any attempt that pass is kept, so a later flaky failure never overwrites it.
#[derive(Clone)]
struct BestObs {
    status: TestVerdict,
    details: String,
    duration: Duration,
    max_memory: Option<u64>,
}

impl BestObs {
    fn missing() -> Self {
        BestObs {
            status: TestVerdict::Fatal,
            details: "test produced no result".to_owned(),
            duration: Duration::ZERO,
            max_memory: None,
        }
    }

    fn into_outcome(self) -> TestOutcome {
        TestOutcome {
            status: self.status,
            details: self.details,
            duration: self.duration,
            max_memory: self.max_memory,
        }
    }
}

fn merge_best(best: &mut FxHashMap<String, BestObs>, name: &str, incoming: BestObs) {
    match best.get(name) {
        // Keep an earlier pass over any later result (pass-if-any-pass).
        Some(existing) if !existing.status.is_failure() => {}
        _ => {
            best.insert(name.to_owned(), incoming);
        }
    }
}

/// Run one group of testcases once, with bounded infra (RE-queue-timeout) retries.
async fn run_group(
    ctx: &TargetCtx,
    plan: &TargetPlan,
    selection: &batching::TestSelection<String>,
    names: &[String],
    repeat_index: u32,
    failure_attempt: u32,
    per_target_sem: &Arc<Semaphore>,
) -> GroupOutcome {
    let config = &ctx.config;
    let mut infra_attempt = 0u32;
    loop {
        let attempt_index = failure_attempt + infra_attempt;
        let mut has_unseen = false;
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
        let mut caching = crate::caching::TestExecutionCaching::resolve(
            plan.cache_class,
            config.variant.is_default(),
            plan.repeat.is_stress(),
            attempt_index,
        );
        if has_unseen {
            caching = crate::caching::TestExecutionCaching::Disabled;
        }
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let exec_args =
            plan.translator
                .execution_args(&name_refs, plan.ignored, &config.extra_test_args);

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
        let request = build_testing_request(TestingRequest {
            target: crate::proto::test::ConfiguredTargetHandle {
                id: plan.spec.handle.0,
            },
            suite: plan.spec.suite.clone(),
            testcases,
            cmd: crate::execution::build_cmd(&plan.spec, &exec_args),
            env: crate::execution::build_env(&plan.spec, &config.extra_env),
            variant: config.variant.identity(),
            repeat_count,
            profile: plan.testing_profile(config),
            caching,
            timeout: plan.timeout,
        });

        let response = {
            // Acquire the per-target permit FIRST, then the scarce global permit
            // immediately before issuing the RPC. Holding the global permit while
            // parked on a per-target gate would pin the global bound (hold-and-
            // wait); taking per-target first also caps a target's presence in the
            // global FIFO to its per-target bound, so a 20k-test target cannot
            // monopolize the queue ahead of a later small target. Order is
            // identical at both call sites, so it stays deadlock-free.
            let _target = per_target_sem
                .acquire()
                .await
                .expect("per-target semaphore");
            let _global = ctx.global_sem.acquire().await.expect("global semaphore");
            ctx.orch.execute2(request).await
        };

        match response {
            Ok(response) => match decode_response(response) {
                Execute2Outcome::Completed(action) => {
                    return GroupOutcome::Observed {
                        observations: plan
                            .translator
                            .parse_results(&action.stdout, &action.stderr),
                        raw: action.status,
                        execution_time: action.execution_time,
                        max_memory: action.max_memory_used_bytes,
                        fresh: action.exec_kind.is_fresh_run(),
                        env: match action.exec_kind {
                            crate::result::ExecKind::RemoteExecuted
                            | crate::result::ExecKind::RemoteCacheHit => {
                                crate::duration_db::Environment::Remote
                            }
                            _ => crate::duration_db::Environment::Local,
                        },
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
                    };
                }
                Execute2Outcome::CancelledUnspecified => {
                    return GroupOutcome::GroupFailed {
                        status: TestVerdict::Omitted,
                        details: "run cancelled".to_owned(),
                    };
                }
            },
            Err(e) => {
                return GroupOutcome::GroupFailed {
                    status: TestVerdict::Fatal,
                    details: format!("Execute2 RPC failed: {e:#}"),
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
/// crashed neighbour cannot mis-FATAL innocent tests.
async fn execute_test_action(
    ctx: &TargetCtx,
    plan: &TargetPlan,
    selection: batching::TestSelection<String>,
    expected_members: Vec<String>,
    repeat_index: u32,
    per_target_sem: Arc<Semaphore>,
) -> Vec<FinishedTest> {
    let mut best: FxHashMap<String, BestObs> = FxHashMap::default();
    // Per-name fresh, harness-attributed attempts, recorded into the flake DB.
    let mut attempts: FxHashMap<String, Vec<DbObservation>> = FxHashMap::default();
    let mut pending: Vec<String> = expected_members.clone();
    let mut failure_attempt = 0u32;

    loop {
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
            failure_attempt,
            &per_target_sem,
        )
        .await
        {
            GroupOutcome::Observed {
                observations,
                raw,
                execution_time,
                max_memory,
                fresh,
                env,
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
                    let (status, details, from_harness) = match observations.get(name) {
                        Some(obs) => (obs.status, obs.details.clone(), true),
                        // No harness line attributed this name. The exit status
                        // is authoritative and survives result caching (buck2
                        // drops stdout on a cache hit), so a clean exit means the
                        // member passed. A nonzero exit for a singleton is that
                        // test's failure; for a multi-member batch it cannot be
                        // attributed, so the member is FATAL and the caller's
                        // per-test isolation re-run resolves it with a precise
                        // singleton exit code.
                        None => {
                            let (status, details) = match raw {
                                ProcessOutcome::Finished { exit_code: 0 } => {
                                    (TestVerdict::Pass, String::new())
                                }
                                ProcessOutcome::Finished { .. } if singleton => {
                                    (TestVerdict::Fail, String::new())
                                }
                                ProcessOutcome::Finished { .. } => (
                                    TestVerdict::Fatal,
                                    "test produced no result in batch output".to_owned(),
                                ),
                                ProcessOutcome::TimedOut { .. } => {
                                    (TestVerdict::Timeout, "execution timed out".to_owned())
                                }
                            };
                            (status, details, false)
                        }
                    };
                    // A fresh, harness-attributed attempt is an independent flake
                    // observation. A synthesized status (a batch member with no
                    // output, or an exit-code-only verdict) is NOT recorded: it is
                    // not a clean per-test signal, and a crashed batch must not
                    // inflate an innocent member's flake rate.
                    if fresh && from_harness {
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
                    merge_best(
                        &mut best,
                        name,
                        BestObs {
                            status,
                            details,
                            duration: execution_time,
                            max_memory,
                        },
                    );
                }
            }
            GroupOutcome::GroupFailed { status, details } => {
                for name in &pending {
                    merge_best(
                        &mut best,
                        name,
                        BestObs {
                            status,
                            details: details.clone(),
                            duration: Duration::ZERO,
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
                },
                repeat_index,
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
        finished.push(make_finished(ctx, plan, name, repeat_index, outcome, observations).await);
    }
    finished
}

/// The batch's per-name accumulation handed to [`isolate_failures`]: the folded
/// best result (read) and the fresh attempt observations (drained for the
/// non-failing members it reports). Bundled to keep the arg list small.
struct BatchAccum<'a> {
    best: &'a FxHashMap<String, BestObs>,
    attempts: &'a mut FxHashMap<String, Vec<DbObservation>>,
}

/// Re-run each still-failing member of a batch as its own singleton action to
/// attribute the failure precisely; non-failing members are reported from the
/// batch's best-so-far. Singletons cannot re-isolate (`names.len() == 1`).
async fn isolate_failures(
    ctx: &TargetCtx,
    plan: &TargetPlan,
    all_names: &[String],
    still_failing: &[String],
    accum: BatchAccum<'_>,
    repeat_index: u32,
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
        finished.push(make_finished(ctx, plan, name, repeat_index, outcome, observations).await);
    }
    for name in still_failing {
        let single = Box::pin(execute_test_action(
            ctx,
            plan,
            batching::TestSelection::Explicit(vec![name.clone()]),
            vec![name.clone()],
            repeat_index,
            per_target_sem.clone(),
        ))
        .await;
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
    let mut details = finalize_details(ctx, outcome.details).await;
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
        Some(outcome.duration),
        details,
        outcome.max_memory,
    );
    FinishedTest {
        result,
        test_id,
        status: outcome.status,
        quarantined: plan.quarantined(),
        db_observations,
    }
}

/// Routing/owner/flake annotation appended to a failing test's details, e.g.
/// `[brtr: target=root//rust/foo:foo | owner=spreadsheets | oncall=sheets |
/// flaky_history: failed 4/20 recent runs, last=Timeout]`. Flake data is read
/// from the pre-run snapshot. Only failures are annotated (by the caller), so
/// passing-test output stays clean.
fn failure_annotation(plan: &TargetPlan, ctx: &TargetCtx, test_id: &TestIdentity) -> String {
    let mut parts: Vec<String> = vec![format!("target={}", plan.spec.display)];
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

/// Inline small logs; upload large ones to CAS and leave a short pointer.
async fn finalize_details(ctx: &TargetCtx, details: String) -> String {
    match result::route_log(details.len(), ctx.config.cas_inline_limit) {
        result::LogRouting::Inline => details,
        result::LogRouting::UploadToCas => upload_details_to_cas(ctx, details).await,
    }
}

async fn upload_details_to_cas(ctx: &TargetCtx, details: String) -> String {
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
            truncate(&details)
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
            let _ = std::fs::remove_file(&path);
            let msg = format!(
                "[log {size} bytes uploaded to CAS: {}/{}]",
                digest.hash, digest.size_bytes
            );
            let _ = ctx.orch.attach_info_message(msg.clone()).await;
            format!("{msg}\n{}", truncate(&details))
        }
        Err(e) => {
            let _ = std::fs::remove_file(&path);
            format!(
                "[log {size} bytes; CAS upload failed: {e:#}]\n{}",
                truncate(&details)
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
    let name = format!("{} (listing)", plan.spec.suite);
    let _ = ctx
        .orch
        .report_tests_discovered(
            plan.spec.handle_proto(),
            plan.spec.suite.clone(),
            vec![name.clone()],
        )
        .await;
    let details = finalize_details(ctx, details).await;
    let test_id = TestIdentity {
        target: plan.spec.display.clone(),
        name: plan.spec.display.clone(), // Target failures use the target name as the base test name.
        variant: ctx.config.variant.clone(),
    };
    let run_id = RunIdentity {
        test: test_id.clone(),
        repeat: plan.repeat,
        repeat_index: 0,
    };
    let result = build_test_result(
        &run_id,
        plan.spec.handle_proto(),
        status,
        None,
        details,
        None,
    );
    let _ = ctx
        .report_tx
        .send(ReporterMessage::Finished(vec![FinishedTest {
            result,
            test_id,
            status,
            quarantined: plan.quarantined(),
            // A listing/target-level failure is not a per-test duration sample.
            db_observations: Vec::new(),
        }]))
        .await;
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
