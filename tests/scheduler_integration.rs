//! End-to-end scheduler test against an in-process mock `TestOrchestrator`.
//!
//! This exercises the real gRPC stack (tonic over an in-memory duplex), the
//! real `Orchestrator` client, and the real scheduler: intake → cacheable
//! listing → per-test fanout → per-name decode → result reporting →
//! end_of_test_results. The mock plays buck2: it answers `Execute2(Listing)`
//! with a libtest JSON listing and `Execute2(Testing)` with libtest JSON run
//! events, and records every reported result + the final exit code.

use std::sync::Arc;
use std::sync::Mutex;

use quokka::cli;
use quokka::executor_server::SpecEnvelope;
use quokka::orchestrator::Orchestrator;
use quokka::proto::test::test_orchestrator_server::{TestOrchestrator, TestOrchestratorServer};
use quokka::proto::test::{
    ConfiguredTarget, ConfiguredTargetHandle, Empty, EndOfTestResultsRequest, ExecuteRequest2,
    ExecuteResponse2, ExecutionResult2, ExecutionStatus, ExecutionStream, ExternalRunnerSpec,
    ExternalRunnerSpecValue, PrepareForLocalExecutionRequest, PrepareForLocalExecutionResponse,
    ReportTestResultRequest, ReportTestSessionRequest, ReportTestsDiscoveredRequest, TestResult,
    TestStatus, UploadFileToCasRequest, UploadFileToCasResponse, execute_response2,
    execution_status, external_runner_spec_value, test_stage,
};
use quokka::run::drive_to_completion;
use quokka::transport::{DuplexChannel, connect_client, serve_connection};
use tonic::{Request, Response, Status};

#[derive(Default)]
struct Recorded {
    results: Vec<TestResult>,
    discovered: Vec<(String, Vec<String>)>,
    end_exit_code: Option<i32>,
    /// Testcases of every `Execute2(Testing)` action, in order.
    testing_calls: Vec<Vec<String>>,
    /// How many times each test has appeared in a Testing action (for flaky-once).
    testing_seen: std::collections::HashMap<String, u32>,
}

/// A mock buck2 orchestrator. For each test name, a status is canned; the mock
/// answers listing and testing executes accordingly.
struct MockBuck2 {
    /// name -> "ok" | "failed" | "ignored"
    test_events: Vec<(String, &'static str)>,
    recorded: Arc<Mutex<Recorded>>,
    /// If set, this test's output is omitted when it runs inside a multi-member
    /// batch (simulating a mid-batch crash), but reported normally when run alone.
    omit_in_batch: Option<String>,
    /// If set, this test fails the first time it appears in a Testing action and
    /// passes thereafter (simulating a flaky test that passes on retry).
    flaky_once: Option<String>,
    /// If true, every Testing execution returns exit 0 with EMPTY stdout/stderr,
    /// simulating a buck2 cache hit (buck2 drops an action's streams on a replay,
    /// keeping only the cached exit status). The verdict must come from the exit
    /// code, not the (absent) harness output.
    cache_replay: bool,
}

fn inline_result(stdout: String, exit_code: i32) -> ExecuteResponse2 {
    use quokka::proto::data::{CommandExecutionKind, LocalCommand, command_execution_kind};
    use quokka::proto::test::ExecutionDetails;
    ExecuteResponse2 {
        response: Some(execute_response2::Response::Result(ExecutionResult2 {
            status: Some(ExecutionStatus {
                status: Some(execution_status::Status::Finished(exit_code)),
            }),
            stdout: Some(ExecutionStream {
                item: Some(quokka::proto::test::execution_stream::Item::Inline(
                    stdout.into_bytes(),
                )),
            }),
            stderr: None,
            outputs: vec![],
            start_time: None,
            execution_time: Some(prost_types::Duration {
                seconds: 0,
                nanos: 1_000_000,
            }),
            // A real, fresh local execution — so the runner classifies it as a
            // fresh run and records it into the duration/flake DB (a cache replay
            // would carry a RemoteCommand with cache_hit=true instead).
            execution_details: Some(ExecutionDetails {
                execution_kind: Some(CommandExecutionKind {
                    command: Some(command_execution_kind::Command::LocalCommand(
                        LocalCommand {
                            action_digest: "test-action".to_owned(),
                        },
                    )),
                }),
            }),
            max_memory_used_bytes: Some(1024),
        })),
    }
}

#[tonic::async_trait]
impl TestOrchestrator for MockBuck2 {
    async fn execute2(
        &self,
        request: Request<ExecuteRequest2>,
    ) -> Result<Response<ExecuteResponse2>, Status> {
        let req = request.into_inner();
        let stage = req
            .test_executable
            .clone()
            .and_then(|e| e.stage)
            .and_then(|s| s.item)
            .expect("stage");
        let response = match stage {
            test_stage::Item::Listing(_) => {
                // Answer with a libtest JSON listing of all known tests.
                let mut out = String::from("{ \"type\": \"suite\", \"event\": \"discovery\" }\n");
                for (name, _) in &self.test_events {
                    out.push_str(&format!(
                        "{{ \"type\": \"test\", \"event\": \"discovered\", \"name\": \"{name}\", \"ignore\": false }}\n"
                    ));
                }
                inline_result(out, 0)
            }
            test_stage::Item::Testing(_testing) => {
                let mut effective_testcases = Vec::new();
                if let Some(exec) = req.test_executable.as_ref() {
                    for arg in &exec.cmd {
                        if let Some(content) = &arg.content {
                            if let Some(quokka::proto::test::arg_value_content::Value::SpecValue(sv)) = &content.value {
                                if let Some(quokka::proto::test::external_runner_spec_value::Value::Verbatim(v)) = &sv.value {
                                    if self.test_events.iter().any(|(n, _)| n == v) {
                                        effective_testcases.push(v.clone());
                                    }
                                }
                            }
                        }
                    }
                }
                if effective_testcases.is_empty() {
                    effective_testcases = self.test_events.iter().map(|(n, _)| n.clone()).collect();
                }
                let mut rec = self.recorded.lock().expect("recorded mutex poisoned");
                rec.testing_calls.push(effective_testcases.clone());
                if self.cache_replay {
                    // Cache hit: exit 0, no streams. The runner must read PASS
                    // from the exit status alone (buck2 returns empty stdout).
                    drop(rec);
                    return Ok(Response::new(inline_result(String::new(), 0)));
                }
                let batch = effective_testcases.len() > 1;
                let mut out = String::new();
                let mut any_fail = false;
                let mut crashed = false;
                for name in &effective_testcases {
                    // Simulate a mid-batch crash: omit this member only in a batch.
                    // A real crash aborts the harness process, so the action also
                    // exits nonzero (libtest never exits 0 with a member missing);
                    // the missing member is then unattributable and isolated.
                    if batch && self.omit_in_batch.as_deref() == Some(name.as_str()) {
                        crashed = true;
                        continue;
                    }
                    let seen = {
                        let c = rec.testing_seen.entry(name.clone()).or_insert(0);
                        *c += 1;
                        *c
                    };
                    let canned = self
                        .test_events
                        .iter()
                        .find(|(n, _)| n == name)
                        .map(|(_, ev)| *ev)
                        .unwrap_or("ok");
                    // flaky_once: fail on first appearance, pass afterwards.
                    let event = if self.flaky_once.as_deref() == Some(name.as_str()) && seen == 1 {
                        "failed"
                    } else {
                        canned
                    };
                    if event == "failed" {
                        any_fail = true;
                        out.push_str(&format!(
                            "{{ \"type\": \"test\", \"name\": \"{name}\", \"event\": \"failed\", \"stdout\": \"boom\" }}\n"
                        ));
                    } else {
                        out.push_str(&format!(
                            "{{ \"type\": \"test\", \"name\": \"{name}\", \"event\": \"{event}\" }}\n"
                        ));
                    }
                }
                inline_result(out, if any_fail || crashed { 101 } else { 0 })
            }
        };
        Ok(Response::new(response))
    }

    async fn report_test_result(
        &self,
        request: Request<ReportTestResultRequest>,
    ) -> Result<Response<Empty>, Status> {
        if let Some(result) = request.into_inner().result {
            self.recorded
                .lock()
                .expect("recorded mutex poisoned")
                .results
                .push(result);
        }
        Ok(Response::new(Empty {}))
    }

    async fn report_tests_discovered(
        &self,
        request: Request<ReportTestsDiscoveredRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        if let Some(testing) = req.testing {
            self.recorded
                .lock()
                .expect("recorded mutex poisoned")
                .discovered
                .push((testing.suite, testing.testcases));
        }
        Ok(Response::new(Empty {}))
    }

    async fn report_test_session(
        &self,
        _request: Request<ReportTestSessionRequest>,
    ) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }

    async fn end_of_test_results(
        &self,
        request: Request<EndOfTestResultsRequest>,
    ) -> Result<Response<Empty>, Status> {
        self.recorded
            .lock()
            .expect("recorded mutex poisoned")
            .end_exit_code = Some(request.into_inner().exit_code);
        Ok(Response::new(Empty {}))
    }

    async fn prepare_for_local_execution(
        &self,
        _request: Request<PrepareForLocalExecutionRequest>,
    ) -> Result<Response<PrepareForLocalExecutionResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn attach_info_message(
        &self,
        _request: Request<quokka::proto::test::AttachInfoMessageRequest>,
    ) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }

    async fn upload_file_to_cas(
        &self,
        _request: Request<UploadFileToCasRequest>,
    ) -> Result<Response<UploadFileToCasResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
}

fn verbatim(s: &str) -> ExternalRunnerSpecValue {
    ExternalRunnerSpecValue {
        value: Some(external_runner_spec_value::Value::Verbatim(s.to_owned())),
    }
}

fn libtest_spec(handle: i64) -> ExternalRunnerSpec {
    libtest_spec_labeled(handle, vec![])
}

fn libtest_spec_labeled(handle: i64, labels: Vec<String>) -> ExternalRunnerSpec {
    ExternalRunnerSpec {
        target: Some(ConfiguredTarget {
            handle: Some(ConfiguredTargetHandle { id: handle }),
            cell: "root".into(),
            package: "rust/foo".into(),
            target: "foo".into(),
            configuration: "cfg".into(),
            package_project_relative_path: "rust/foo".into(),
            test_config_unification_rollout: false,
            package_oncall: None,
        }),
        test_type: "rust".into(),
        command: vec![verbatim("./foo-test")],
        env: Default::default(),
        labels,
        contacts: vec![],
        oncall: None,
        working_dir_cell: "root".into(),
    }
}

fn test_context() -> cli::SessionContext {
    cli::SessionContext {
        host_platform: None,
        trace_id: None,
    }
}

fn test_config() -> cli::RunnerConfig {
    let inv = cli::parse(
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
        .map(|s| s.to_string())
        .collect(),
    )
    .expect("config");
    inv.config
}

/// Spin up the mock orchestrator on an in-memory duplex and return a connected
/// `Orchestrator` client plus the recording handle and a server join handle.
async fn mock_orchestrator(
    test_events: Vec<(String, &'static str)>,
) -> (
    Orchestrator,
    Arc<Mutex<Recorded>>,
    tokio::task::JoinHandle<()>,
) {
    mock_orchestrator_full(test_events, None, None, false).await
}

/// A mock that answers every Testing execution as a cache replay: exit 0 with
/// empty stdout/stderr (the listing still returns real test names, since the
/// runner always issues the listing uncacheable).
async fn mock_orchestrator_replay(
    test_events: Vec<(String, &'static str)>,
) -> (
    Orchestrator,
    Arc<Mutex<Recorded>>,
    tokio::task::JoinHandle<()>,
) {
    mock_orchestrator_full(test_events, None, None, true).await
}

async fn mock_orchestrator_full(
    test_events: Vec<(String, &'static str)>,
    omit_in_batch: Option<String>,
    flaky_once: Option<String>,
    cache_replay: bool,
) -> (
    Orchestrator,
    Arc<Mutex<Recorded>>,
    tokio::task::JoinHandle<()>,
) {
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);

    let mock = MockBuck2 {
        test_events,
        recorded: recorded.clone(),
        omit_in_batch,
        flaky_once,
        cache_replay,
    };
    let router = tonic::transport::Server::builder().add_service(
        TestOrchestratorServer::new(mock)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX),
    );
    let (read, write) = tokio::io::split(server_io);
    let server_conn = DuplexChannel::new(read, write);
    let server = tokio::spawn(async move {
        let _ = serve_connection(server_conn, router, std::future::pending::<()>()).await;
    });

    let channel = connect_client(client_io, "orchestrator")
        .await
        .expect("connect");
    let orch = Orchestrator::new(channel);
    (orch, recorded, server)
}

#[tokio::test]
async fn fans_out_and_reports_each_test_with_correct_verdict() {
    let events = vec![
        ("alpha".to_string(), "ok"),
        ("beta_fails".to_string(), "failed"),
        ("gamma".to_string(), "ok"),
    ];
    let (orch, recorded, _server) = mock_orchestrator(events).await;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(7))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, test_config(), test_context()).await;

    let rec = recorded.lock().expect("recorded mutex poisoned");
    // One result per discovered test (cardinality conserved).
    assert_eq!(rec.results.len(), 3, "expected one result per test");
    let by_name: std::collections::HashMap<&str, i32> = rec
        .results
        .iter()
        .map(|r| (r.name.as_str(), r.status))
        .collect();
    assert_eq!(by_name["alpha"], TestStatus::Pass as i32);
    assert_eq!(by_name["beta_fails"], TestStatus::Fail as i32);
    assert_eq!(by_name["gamma"], TestStatus::Pass as i32);

    // Discovery reported the shard's tests.
    assert_eq!(rec.discovered.len(), 1);
    assert_eq!(rec.discovered[0].1.len(), 3);

    // A failing test fails the run, and end_of_test_results was sent.
    assert_eq!(rec.end_exit_code, Some(32));
}

#[tokio::test]
async fn all_passing_yields_zero_exit() {
    let events = vec![("a".to_string(), "ok"), ("b".to_string(), "ok")];
    let (orch, recorded, _server) = mock_orchestrator(events).await;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(1))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, test_config(), test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert_eq!(rec.results.len(), 2);
    assert!(
        rec.results
            .iter()
            .all(|r| r.status == TestStatus::Pass as i32)
    );
    assert_eq!(rec.end_exit_code, Some(0));
}

#[tokio::test]
async fn target_batching_still_reports_each_test() {
    // Batch all tests into one Execute2; per-name decode must still produce a
    // result for every test.
    let events = vec![
        ("a".to_string(), "ok"),
        ("b".to_string(), "failed"),
        ("c".to_string(), "ok"),
        ("d".to_string(), "ok"),
    ];
    let (orch, recorded, _server) = mock_orchestrator(events).await;
    let mut config = test_config();
    config.batch_mode = quokka::batching::BatchMode::Target;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(3))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert_eq!(rec.results.len(), 4, "batched run must report every member");
    assert_eq!(rec.end_exit_code, Some(32));
}

#[tokio::test]
async fn batch_isolation_reruns_missing_member_singly() {
    // A batched action omits `y`'s result (mid-batch crash). With the default
    // RerunPerTestToIsolate policy, `y` is re-run alone (where it is reported ok),
    // so an innocent member is not mis-FATAL'd and the run is green.
    let events = vec![("x".to_string(), "ok"), ("y".to_string(), "ok")];
    let (orch, recorded, _server) =
        mock_orchestrator_full(events, Some("y".to_string()), None, false).await;
    let mut config = test_config();
    config.batch_mode = quokka::batching::BatchMode::Target;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(11))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert_eq!(rec.results.len(), 2, "every member must be reported once");
    let by_name: std::collections::HashMap<&str, i32> = rec
        .results
        .iter()
        .map(|r| (r.name.as_str(), r.status))
        .collect();
    assert_eq!(by_name["x"], TestStatus::Pass as i32);
    assert_eq!(
        by_name["y"],
        TestStatus::Pass as i32,
        "isolated rerun should pass y"
    );
    assert_eq!(rec.end_exit_code, Some(0));
    // y was re-run as its own singleton action after the batch omitted it.
    assert!(
        rec.testing_calls
            .iter()
            .any(|tc| tc == &vec!["y".to_string()]),
        "expected an isolated singleton action for y, got {:?}",
        rec.testing_calls
    );
}

#[tokio::test]
async fn flaky_member_passes_on_retry_without_failing_siblings() {
    // In a batch [p, f], f fails on its first run and passes on retry. The target
    // is flaky (retries allowed). The best-across-attempts fold must keep p's
    // pass (never re-running/flipping it) and report f as a pass after the
    // narrowed retry, yielding a green run.
    let events = vec![("p".to_string(), "ok"), ("f".to_string(), "ok")];
    let (orch, recorded, _server) =
        mock_orchestrator_full(events, None, Some("f".to_string()), false).await;
    let mut config = test_config();
    config.batch_mode = quokka::batching::BatchMode::Target;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec_labeled(
            13,
            vec!["rust:flaky".to_string()],
        ))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert_eq!(rec.results.len(), 2);
    let by_name: std::collections::HashMap<&str, i32> = rec
        .results
        .iter()
        .map(|r| (r.name.as_str(), r.status))
        .collect();
    assert_eq!(by_name["p"], TestStatus::Pass as i32);
    assert_eq!(
        by_name["f"],
        TestStatus::Pass as i32,
        "f should pass on retry"
    );
    assert_eq!(rec.end_exit_code, Some(0));
    // The retry must be narrowed to only the still-failing member: p is run once,
    // f appears in the initial batch and again in the narrowed retry.
    let p_runs = rec
        .testing_calls
        .iter()
        .filter(|tc| tc.contains(&"p".to_string()))
        .count();
    assert_eq!(
        p_runs, 1,
        "passed sibling p must not be re-run; calls={:?}",
        rec.testing_calls
    );
}

#[tokio::test]
async fn flake_db_records_each_fresh_attempt_not_just_the_folded_best() {
    // A test that fails on attempt 0 and passes on retry must leave a FAILURE in
    // the flake history (runs=2, failures=1) — recording only the folded best
    // result would log it as a clean pass, blinding the flake DB to exactly the
    // recover-on-retry flakes it exists to surface. The passing sibling records a
    // single clean run.
    let dir = std::env::temp_dir().join(format!("tpx_flakedb_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let events = vec![("steady".to_string(), "ok"), ("flaky".to_string(), "ok")];
    let (orch, _recorded, _server) =
        mock_orchestrator_full(events, None, Some("flaky".to_string()), false).await;
    let mut config = test_config();
    config.duration_db = Some(dir.clone());

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec_labeled(
            21,
            vec!["rust:flaky".to_string()],
        ))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, config, test_context()).await;

    let flake = std::fs::read_to_string(dir.join("flake.json")).expect("flake.json written");
    // The flaky test ran twice (fail then pass): both fresh attempts recorded.
    assert!(
        flake.contains(r#""runs":2,"failures":1"#),
        "flaky test should record 2 runs / 1 failure, got: {flake}"
    );
    // The steady test ran once and passed.
    assert!(
        flake.contains(r#""runs":1,"failures":0"#),
        "steady test should record 1 run / 0 failures, got: {flake}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn cached_replay_with_empty_output_reports_pass_from_exit_code() {
    // buck2 returns empty stdout/stderr on a cache hit, keeping only the cached
    // exit status. A per-test execution that exits 0 with no harness output is a
    // PASS (the exit code is authoritative), NOT a FATAL "no result in output".
    // This is the steady-state warm-cache path: getting it wrong turns every
    // cached pass into a fatal failure on the next invocation.
    let events = vec![("a".to_string(), "ok"), ("b".to_string(), "ok")];
    let (orch, recorded, _server) = mock_orchestrator_replay(events).await;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(3))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, test_config(), test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert_eq!(rec.results.len(), 2, "every discovered test still reported");
    assert!(
        rec.results
            .iter()
            .all(|r| r.status == TestStatus::Pass as i32),
        "empty-output exit-0 cache replay must be PASS, got {:?}",
        rec.results
            .iter()
            .map(|r| (r.name.clone(), r.status))
            .collect::<Vec<_>>()
    );
    assert_eq!(rec.end_exit_code, Some(0));
}
