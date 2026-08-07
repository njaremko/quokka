//! End-to-end scheduler test against an in-process mock `TestOrchestrator`.
//!
//! This exercises the real gRPC stack (tonic over an in-memory duplex), the
//! real `Orchestrator` client, and the real scheduler: intake → uncacheable
//! listing → per-test fanout → per-name decode → result reporting →
//! end_of_test_results. The mock plays buck2: it answers `Execute2(Listing)`
//! with a libtest JSON listing and `Execute2(Testing)` with libtest JSON run
//! events, and records every reported result + the final exit code.

use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

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
use quokka::variant::RepeatKind;
use tonic::{Request, Response, Status};

#[derive(Default)]
struct Recorded {
    report_attempts: Vec<TestResult>,
    results: Vec<TestResult>,
    rejected_report_name: Option<String>,
    discovered: Vec<(String, Vec<String>)>,
    end_exit_code: Option<i32>,
    /// Verbatim argv of every `Execute2(Listing)` action, in order.
    listing_calls: Vec<Vec<String>>,
    /// Testcases of every `Execute2(Testing)` action, in order.
    testing_calls: Vec<Vec<String>>,
    /// Appended argv groups encoded into every Testing action, in order.
    testing_commands: Vec<Vec<Vec<String>>>,
    /// How many times each test has appeared in a Testing action (for flaky-once).
    testing_seen: std::collections::HashMap<String, u32>,
    /// Captured disable_test_execution_caching flags for each Testing call.
    disable_caching_calls: Vec<bool>,
    active_testing_calls: usize,
    max_active_testing_calls: usize,
}

struct ActiveTestingCall {
    recorded: Arc<Mutex<Recorded>>,
}

impl ActiveTestingCall {
    fn start(
        recorded: Arc<Mutex<Recorded>>,
        testcases: Vec<String>,
        disable_caching: bool,
    ) -> Self {
        {
            let mut rec = recorded.lock().expect("recorded mutex poisoned");
            rec.testing_calls.push(testcases);
            rec.disable_caching_calls.push(disable_caching);
            rec.active_testing_calls += 1;
            rec.max_active_testing_calls =
                rec.max_active_testing_calls.max(rec.active_testing_calls);
        }
        Self { recorded }
    }
}

impl Drop for ActiveTestingCall {
    fn drop(&mut self) {
        let mut rec = self.recorded.lock().expect("recorded mutex poisoned");
        rec.active_testing_calls -= 1;
    }
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
    forced_stdout: Option<String>,
    /// If set, every Testing execution exits nonzero without libtest output but
    /// with this action stderr, simulating a wrapper-level crash or OOM.
    stderr_crash: Option<String>,
    testing_delay: Duration,
    injected_testing_response: InjectedTestingResponse,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InjectedTestingResponse {
    Normal,
    EmptyCachedPass,
    NamedCachedPass,
    DifferentNamedCachedPass,
    EmptyCachedFailure,
    EmptyUnknownPass,
    FirstActionTimeout,
    CancelledUnspecified,
}

fn command_verbatim_args(req: &ExecuteRequest2) -> Vec<String> {
    let mut args = command_appended_args(req)
        .into_iter()
        .next()
        .unwrap_or_default();
    args.insert(0, "mock-test-binary".to_owned());
    args
}

fn command_appended_args(req: &ExecuteRequest2) -> Vec<Vec<String>> {
    let args: Vec<String> = req.test_executable
        .as_ref()
        .map(|exec| {
            exec.cmd
                .iter()
                .filter_map(|arg| {
                    let content = arg.content.as_ref()?;
                    match content.value.as_ref()? {
                        quokka::proto::test::arg_value_content::Value::SpecValue(sv) => {
                            match sv.value.as_ref()? {
                                quokka::proto::test::external_runner_spec_value::Value::Verbatim(v) => {
                                    Some(v.clone())
                                }
                                _ => None,
                            }
                        }
                        _ => None,
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    if args.get(0).map(String::as_str) == Some("/bin/sh")
        && args.get(1).map(String::as_str) == Some("-c")
    {
        return args
            .get(2)
            .into_iter()
            .flat_map(|script| script.lines())
            .filter(|line| line.starts_with("quokka_run_logical "))
            .filter_map(|line| line.split_once("\"$@\""))
            .map(|(_, suffix)| {
                suffix
                    .strip_suffix(" || overall_status=$?")
                    .unwrap_or(suffix)
                    .split_whitespace()
                    .map(|arg| {
                        arg.strip_prefix('\'')
                            .and_then(|arg| arg.strip_suffix('\''))
                            .unwrap_or(arg)
                            .to_owned()
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
    }
    vec![args]
}

fn resource_marker_token(req: &ExecuteRequest2) -> String {
    req.test_executable
        .as_ref()
        .into_iter()
        .flat_map(|exec| exec.cmd.iter())
        .filter_map(|arg| arg.content.as_ref()?.value.as_ref())
        .find_map(|value| match value {
            quokka::proto::test::arg_value_content::Value::SpecValue(value) => {
                match value.value.as_ref()? {
                    quokka::proto::test::external_runner_spec_value::Value::Verbatim(script) => {
                        script.lines().find_map(|line| {
                            line.strip_prefix("quokka_resource_marker_token='")
                                .and_then(|value| value.strip_suffix('\''))
                                .map(str::to_owned)
                        })
                    }
                    _ => None,
                }
            }
            _ => None,
        })
        .expect("supervised request has a resource marker token")
}

fn listed_tests<'a>(
    test_events: &'a [(String, &'static str)],
    cmd_args: &[String],
) -> Vec<(&'a String, &'static str)> {
    let mut filters = Vec::new();
    let mut skips = Vec::new();
    let mut exact = false;
    let mut include_ignored = false;
    let mut ignored_only = false;

    let args = cmd_args
        .iter()
        .skip(1)
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut i = 0;
    while i < args.len() {
        let arg = args[i];
        if let Some(skip) = arg.strip_prefix("--skip=") {
            skips.push(skip.to_owned());
            i += 1;
            continue;
        }
        match arg {
            "--list" => i += 1,
            "--exact" => {
                exact = true;
                i += 1;
            }
            "--include-ignored" => {
                include_ignored = true;
                i += 1;
            }
            "--ignored" => {
                ignored_only = true;
                i += 1;
            }
            "--skip" => {
                if let Some(value) = args.get(i + 1) {
                    skips.push((*value).to_owned());
                }
                i += 2;
            }
            "--format" | "--color" | "--test-threads" | "--logfile" | "--shuffle-seed" | "-Z" => {
                i += 2;
            }
            flag if flag.starts_with("--format=")
                || flag.starts_with("--color=")
                || flag.starts_with("--test-threads=")
                || flag.starts_with("--logfile=")
                || flag.starts_with("--shuffle-seed=") =>
            {
                i += 1;
            }
            flag if flag.starts_with('-') => i += 1,
            filter => {
                filters.push(filter.to_owned());
                i += 1;
            }
        }
    }

    test_events
        .iter()
        .filter(|(name, event)| {
            let ignored = *event == "ignored";
            let ignored_selected = if ignored_only {
                ignored
            } else {
                include_ignored || !ignored
            };
            let filter_selected = filters.is_empty()
                || filters.iter().any(|filter| {
                    if exact {
                        name == filter
                    } else {
                        name.contains(filter)
                    }
                });
            let skipped = skips.iter().any(|skip| name.contains(skip));
            ignored_selected && filter_selected && !skipped
        })
        .map(|(name, event)| (name, *event))
        .collect()
}

fn inline_result(stdout: String, exit_code: i32) -> ExecuteResponse2 {
    inline_result_with_stderr(stdout, None, exit_code)
}

fn inline_result_with_stderr(
    stdout: String,
    stderr: Option<String>,
    exit_code: i32,
) -> ExecuteResponse2 {
    inline_result_with_execution_kind(stdout, stderr, exit_code, false)
}

fn inline_result_with_execution_kind(
    stdout: String,
    stderr: Option<String>,
    exit_code: i32,
    remote_cache_hit: bool,
) -> ExecuteResponse2 {
    use quokka::proto::data::{
        CommandExecutionKind, LocalCommand, RemoteCommand, command_execution_kind,
    };
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
            stderr: stderr.map(|stderr| ExecutionStream {
                item: Some(quokka::proto::test::execution_stream::Item::Inline(
                    stderr.into_bytes(),
                )),
            }),
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
                    command: Some(if remote_cache_hit {
                        command_execution_kind::Command::RemoteCommand(RemoteCommand {
                            action_digest: "cached-test-action".to_owned(),
                            cache_hit: true,
                            cache_hit_type: 0,
                        })
                    } else {
                        command_execution_kind::Command::LocalCommand(LocalCommand {
                            action_digest: "test-action".to_owned(),
                        })
                    }),
                }),
            }),
            max_memory_used_bytes: Some(1024),
        })),
    }
}

fn empty_unknown_result(exit_code: i32) -> ExecuteResponse2 {
    ExecuteResponse2 {
        response: Some(execute_response2::Response::Result(ExecutionResult2 {
            status: Some(ExecutionStatus {
                status: Some(execution_status::Status::Finished(exit_code)),
            }),
            stdout: None,
            stderr: None,
            outputs: vec![],
            start_time: None,
            execution_time: None,
            execution_details: None,
            max_memory_used_bytes: None,
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
                let cmd_args = command_verbatim_args(&req);
                self.recorded
                    .lock()
                    .expect("recorded mutex poisoned")
                    .listing_calls
                    .push(cmd_args.clone());
                // Answer with a libtest JSON listing of all known tests.
                let mut out = String::from("{ \"type\": \"suite\", \"event\": \"discovery\" }\n");
                for (name, event) in listed_tests(&self.test_events, &cmd_args) {
                    let kind = if event == "bench" {
                        "benchmark"
                    } else {
                        "test"
                    };
                    let ignored = event == "ignored";
                    out.push_str(&format!(
                        "{{ \"type\": \"{kind}\", \"event\": \"discovered\", \"name\": \"{name}\", \"ignore\": {ignored} }}\n"
                    ));
                }
                inline_result(out, 0)
            }
            test_stage::Item::Testing(_) => {
                let appended_commands = command_appended_args(&req);
                let mut effective_testcases = appended_commands
                    .iter()
                    .flatten()
                    .filter(|arg| self.test_events.iter().any(|(name, _)| name == *arg))
                    .cloned()
                    .collect::<Vec<_>>();
                if effective_testcases.is_empty() {
                    effective_testcases = self.test_events.iter().map(|(n, _)| n.clone()).collect();
                }
                let _active_testing_call = ActiveTestingCall::start(
                    self.recorded.clone(),
                    effective_testcases.clone(),
                    req.disable_test_execution_caching,
                );
                if !self.testing_delay.is_zero() {
                    tokio::time::sleep(self.testing_delay).await;
                }
                let mut rec = self.recorded.lock().expect("recorded mutex poisoned");
                rec.testing_commands.push(appended_commands);
                if rec.testing_calls.len() == 1 {
                    let injected = match self.injected_testing_response {
                        InjectedTestingResponse::Normal => None,
                        InjectedTestingResponse::EmptyCachedPass => Some(
                            inline_result_with_execution_kind(String::new(), None, 0, true),
                        ),
                        InjectedTestingResponse::NamedCachedPass => Some(
                            inline_result_with_execution_kind(
                                effective_testcases
                                    .iter()
                                    .map(|name| {
                                        format!(
                                            "{{ \"type\": \"test\", \"name\": \"{name}\", \"event\": \"ok\" }}\n"
                                        )
                                    })
                                    .collect::<Vec<_>>()
                                    .concat(),
                                None,
                                0,
                                true,
                            ),
                        ),
                        InjectedTestingResponse::DifferentNamedCachedPass => Some(
                            inline_result_with_execution_kind(
                                "{ \"type\": \"test\", \"name\": \"other\", \"event\": \"ok\" }\n"
                                    .to_owned(),
                                None,
                                0,
                                true,
                            ),
                        ),
                        InjectedTestingResponse::EmptyCachedFailure => Some(
                            inline_result_with_execution_kind(String::new(), None, 1, true),
                        ),
                        InjectedTestingResponse::EmptyUnknownPass => Some(empty_unknown_result(0)),
                        InjectedTestingResponse::FirstActionTimeout => Some(
                            inline_result_with_stderr(
                                String::new(),
                                Some(format!(
                                    "quokka resource {} test action timeout: seconds=300\n",
                                    resource_marker_token(&req)
                                )),
                                137,
                            ),
                        ),
                        InjectedTestingResponse::CancelledUnspecified => Some(ExecuteResponse2 {
                            response: Some(execute_response2::Response::Cancelled(
                                quokka::proto::test::Cancelled { reason: None },
                            )),
                        }),
                    };
                    if let Some(response) = injected {
                        drop(rec);
                        return Ok(Response::new(response));
                    }
                }
                if let Some(stdout) = &self.forced_stdout {
                    drop(rec);
                    return Ok(Response::new(inline_result(stdout.clone(), 0)));
                }
                if let Some(stderr) = &self.stderr_crash {
                    let stderr =
                        stderr.replace("{resource_marker_token}", &resource_marker_token(&req));
                    drop(rec);
                    return Ok(Response::new(inline_result_with_stderr(
                        String::new(),
                        Some(stderr),
                        137,
                    )));
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
                    if event == "bench" {
                        out.push_str(&format!(
                            "{{ \"type\": \"bench\", \"name\": \"{name}\", \"median\": 1.0, \"deviation\": 0.0 }}\n"
                        ));
                    } else if event == "failed" {
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
            let mut rec = self.recorded.lock().expect("recorded mutex poisoned");
            rec.report_attempts.push(result.clone());
            if rec.rejected_report_name.as_deref() == Some(result.name.as_str()) {
                return Err(Status::unavailable("rejected result"));
            }
            rec.results.push(result);
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

fn ci_report_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "quokka-scheduler-integration-{}-{name}.json",
        std::process::id()
    ))
}

fn semantic_ci_report(mut report: serde_json::Value) -> serde_json::Value {
    let Some(records) = report
        .get_mut("tests")
        .and_then(|value| value.as_array_mut())
    else {
        return report;
    };
    records.sort_by_key(|record| {
        (
            record["target"].as_str().unwrap_or_default().to_owned(),
            record["name"].as_str().unwrap_or_default().to_owned(),
            record["variant"].as_str().unwrap_or_default().to_owned(),
            record["run_name"].as_str().unwrap_or_default().to_owned(),
            record["repeat_index"].as_u64().unwrap_or_default(),
            record["status"].as_str().unwrap_or_default().to_owned(),
            record["integrity"].as_str().unwrap_or_default().to_owned(),
            record["attempts"].to_string(),
        )
    });
    report
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
    mock_orchestrator_full(test_events, None, None, None).await
}

async fn mock_orchestrator_with_stdout(
    test_events: Vec<(String, &'static str)>,
    stdout: String,
) -> (
    Orchestrator,
    Arc<Mutex<Recorded>>,
    tokio::task::JoinHandle<()>,
) {
    mock_orchestrator_full(test_events, None, None, Some(stdout)).await
}

async fn mock_orchestrator_stderr_crash(
    test_events: Vec<(String, &'static str)>,
    stderr: String,
) -> (
    Orchestrator,
    Arc<Mutex<Recorded>>,
    tokio::task::JoinHandle<()>,
) {
    mock_orchestrator_full_with_stderr(
        test_events,
        None,
        None,
        None,
        Some(stderr),
        Duration::ZERO,
        InjectedTestingResponse::Normal,
    )
    .await
}

async fn mock_orchestrator_with_testing_delay(
    test_events: Vec<(String, &'static str)>,
    testing_delay: Duration,
) -> (
    Orchestrator,
    Arc<Mutex<Recorded>>,
    tokio::task::JoinHandle<()>,
) {
    mock_orchestrator_full_with_stderr(
        test_events,
        None,
        None,
        None,
        None,
        testing_delay,
        InjectedTestingResponse::Normal,
    )
    .await
}

async fn mock_orchestrator_with_empty_cache_hit(
    test_events: Vec<(String, &'static str)>,
) -> (
    Orchestrator,
    Arc<Mutex<Recorded>>,
    tokio::task::JoinHandle<()>,
) {
    mock_orchestrator_full_with_stderr(
        test_events,
        None,
        None,
        None,
        None,
        Duration::ZERO,
        InjectedTestingResponse::EmptyCachedPass,
    )
    .await
}

async fn mock_orchestrator_with_injected_testing_response(
    test_events: Vec<(String, &'static str)>,
    injected_testing_response: InjectedTestingResponse,
) -> (
    Orchestrator,
    Arc<Mutex<Recorded>>,
    tokio::task::JoinHandle<()>,
) {
    mock_orchestrator_full_with_stderr(
        test_events,
        None,
        None,
        None,
        None,
        Duration::ZERO,
        injected_testing_response,
    )
    .await
}

async fn mock_orchestrator_full(
    test_events: Vec<(String, &'static str)>,
    omit_in_batch: Option<String>,
    flaky_once: Option<String>,
    forced_stdout: Option<String>,
) -> (
    Orchestrator,
    Arc<Mutex<Recorded>>,
    tokio::task::JoinHandle<()>,
) {
    mock_orchestrator_full_with_stderr(
        test_events,
        omit_in_batch,
        flaky_once,
        forced_stdout,
        None,
        Duration::ZERO,
        InjectedTestingResponse::Normal,
    )
    .await
}

async fn mock_orchestrator_full_with_stderr(
    test_events: Vec<(String, &'static str)>,
    omit_in_batch: Option<String>,
    flaky_once: Option<String>,
    forced_stdout: Option<String>,
    stderr_crash: Option<String>,
    testing_delay: Duration,
    injected_testing_response: InjectedTestingResponse,
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
        forced_stdout,
        stderr_crash,
        testing_delay,
        injected_testing_response,
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
async fn malformed_spec_is_target_scoped_and_never_executes() {
    let events = vec![("unused".to_owned(), "ok")];
    let (orch, recorded, _server) = mock_orchestrator(events).await;
    let mut spec = libtest_spec(2);
    spec.command.push(ExternalRunnerSpecValue { value: None });
    let mut env_spec = libtest_spec(3);
    env_spec
        .env
        .insert("BROKEN".to_owned(), ExternalRunnerSpecValue { value: None });
    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx.send(SpecEnvelope::Spec(Box::new(spec))).unwrap();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(env_spec)))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, test_config(), test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert!(rec.listing_calls.is_empty());
    assert!(rec.testing_calls.is_empty());
    assert!(rec.results.is_empty());
    assert_eq!(rec.end_exit_code, Some(32));
}

#[tokio::test]
async fn report_rejection_fails_session_without_stopping_later_reports() {
    #[derive(Debug, PartialEq)]
    struct DeliveryEvidence {
        report_attempts: Vec<TestResult>,
        accepted_results: Vec<TestResult>,
        end_exit_code: Option<i32>,
    }

    fn expected_result(name: &str) -> TestResult {
        TestResult {
            name: name.to_owned(),
            status: TestStatus::Pass as i32,
            msg: None,
            target: Some(ConfiguredTargetHandle { id: 1 }),
            duration: Some(prost_types::Duration {
                seconds: 0,
                nanos: 1_000_000,
            }),
            details: String::new(),
            max_memory_used_bytes: Some(1024),
        }
    }

    let events = vec![
        ("a_rejected".to_owned(), "ok"),
        ("b_accepted".to_owned(), "ok"),
    ];
    let (orch, recorded, _server) = mock_orchestrator(events).await;
    recorded
        .lock()
        .expect("recorded mutex poisoned")
        .rejected_report_name = Some("a_rejected".to_owned());

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(1))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    let mut config = test_config();
    config.batch_mode = quokka::batching::BatchMode::Target;
    drive_to_completion(orch, intake_rx, config, test_context()).await;

    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert_eq!(
        DeliveryEvidence {
            report_attempts: rec.report_attempts.clone(),
            accepted_results: rec.results.clone(),
            end_exit_code: rec.end_exit_code,
        },
        DeliveryEvidence {
            report_attempts: vec![expected_result("a_rejected"), expected_result("b_accepted"),],
            accepted_results: vec![expected_result("b_accepted")],
            end_exit_code: Some(32),
        }
    );
}

#[tokio::test]
async fn missing_per_test_results_fail_closed_for_empty_and_non_json_stdout() {
    #[derive(Debug, PartialEq)]
    struct MissingResultEvidence {
        results: Vec<TestResult>,
        end_exit_code: Option<i32>,
    }

    async fn run(labels: Vec<String>, stdout: String) -> MissingResultEvidence {
        let events = vec![("alpha".to_owned(), "ok"), ("beta".to_owned(), "ok")];
        let (orch, recorded, _server) = mock_orchestrator_with_stdout(events, stdout).await;
        let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
        intake_tx
            .send(SpecEnvelope::Spec(Box::new(libtest_spec_labeled(
                1, labels,
            ))))
            .unwrap();
        intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();
        drive_to_completion(orch, intake_rx, test_config(), test_context()).await;
        let rec = recorded.lock().expect("recorded mutex poisoned");
        MissingResultEvidence {
            results: rec.results.clone(),
            end_exit_code: rec.end_exit_code,
        }
    }

    fn expected_result(name: &str, stdout: Option<&str>) -> TestResult {
        let mut details =
            "harness exited 0 but emitted no parseable result for this test".to_owned();
        if let Some(stdout) = stdout {
            details.push_str("\n---- stdout ----\n");
            details.push_str(stdout);
        }
        details.push_str(
            "\nquokka attempt history:\n\
             attempt 1: Fatal duration_ms=1 details=harness exited 0 but emitted no parseable result for this test\n\
             attempt 2: Fatal duration_ms=1 details=harness exited 0 but emitted no parseable result for this test\n\
             [brtr: target=root//rust/foo:foo | target_platform=cfg]",
        );
        TestResult {
            name: name.to_owned(),
            status: TestStatus::Fatal as i32,
            msg: None,
            target: Some(ConfiguredTargetHandle { id: 1 }),
            duration: Some(prost_types::Duration {
                seconds: 0,
                nanos: 1_000_000,
            }),
            details,
            max_memory_used_bytes: Some(1024),
        }
    }

    fn expected(stdout: Option<&str>) -> MissingResultEvidence {
        MissingResultEvidence {
            results: vec![
                expected_result("alpha", stdout),
                expected_result("beta", stdout),
            ],
            end_exit_code: Some(32),
        }
    }

    let non_json_stdout = "this is not json\nrandom harness noise";
    assert_eq!(
        [
            run(vec![], String::new()).await,
            run(
                vec![],
                "this is not json\nrandom harness noise\n".to_owned(),
            )
            .await,
            run(vec!["rust:quarantined".to_owned()], String::new()).await,
            run(
                vec!["rust:quarantined".to_owned()],
                "this is not json\nrandom harness noise\n".to_owned(),
            )
            .await,
        ],
        [
            expected(None),
            expected(Some(non_json_stdout)),
            expected(None),
            expected(Some(non_json_stdout)),
        ]
    );
}

#[tokio::test]
async fn singleton_crash_without_harness_result_reports_action_stderr() {
    let events = vec![("ooms".to_string(), "ok")];
    let stderr =
        "nobie rust test cgroup OOM: memory.max=3145728000 oom=0->1 oom_kill=0->3\n".to_string();
    let (orch, recorded, _server) = mock_orchestrator_stderr_crash(events, stderr).await;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(1))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, test_config(), test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert_eq!(rec.results.len(), 1);
    assert_eq!(rec.results[0].name, "ooms");
    assert_eq!(rec.results[0].status, TestStatus::Fail as i32);
    assert_eq!(
        rec.results[0].details,
        "test process exited nonzero with no harness-reported result\n---- stderr ----\nnobie rust test cgroup OOM: memory.max=3145728000 oom=0->1 oom_kill=0->3\n[brtr: target=root//rust/foo:foo | target_platform=cfg]"
    );
    assert_eq!(rec.end_exit_code, Some(32));
}

#[tokio::test]
async fn fixed_chunk_uses_one_harness_command_for_all_selected_names() {
    let events = vec![("alpha".to_string(), "ok"), ("beta".to_string(), "ok")];
    let (orch, recorded, _server) = mock_orchestrator(events).await;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(42))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, test_config(), test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    let results: Vec<(String, i32)> = rec
        .results
        .iter()
        .map(|result| (result.name.clone(), result.status))
        .collect();
    assert_eq!(
        (
            results,
            rec.testing_calls.clone(),
            rec.testing_commands.clone(),
            rec.disable_caching_calls.clone(),
            rec.end_exit_code,
        ),
        (
            vec![
                ("alpha".to_owned(), TestStatus::Pass as i32),
                ("beta".to_owned(), TestStatus::Pass as i32),
            ],
            vec![vec!["alpha".to_owned(), "beta".to_owned()]],
            vec![vec![vec![
                "alpha".to_owned(),
                "beta".to_owned(),
                "--exact".to_owned(),
                "--test-threads=1".to_owned(),
                "--color=never".to_owned(),
                "-Z".to_owned(),
                "unstable-options".to_owned(),
                "--format".to_owned(),
                "json".to_owned(),
            ]]],
            vec![false],
            Some(0),
        )
    );
}

#[tokio::test]
async fn empty_cached_success_never_reports_missing_selected_member_as_pass() {
    let events = vec![("cached".to_string(), "ok")];
    let (orch, recorded, _server) = mock_orchestrator_with_empty_cache_hit(events).await;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(42))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, test_config(), test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    let results: Vec<(String, i32)> = rec
        .results
        .iter()
        .map(|result| (result.name.clone(), result.status))
        .collect();
    assert_eq!(
        (
            results,
            rec.testing_calls.clone(),
            rec.disable_caching_calls.clone(),
            rec.end_exit_code,
        ),
        (
            vec![("cached".to_owned(), TestStatus::Fatal as i32)],
            vec![vec!["cached".to_owned()]],
            vec![false],
            Some(32),
        )
    );
}

#[tokio::test]
async fn differently_named_cached_success_never_reports_missing_selected_member_as_pass() {
    let events = vec![("cached".to_string(), "ok")];
    let (orch, recorded, _server) = mock_orchestrator_with_injected_testing_response(
        events,
        InjectedTestingResponse::DifferentNamedCachedPass,
    )
    .await;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(42))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, test_config(), test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    let results: Vec<(String, i32)> = rec
        .results
        .iter()
        .map(|result| (result.name.clone(), result.status))
        .collect();
    assert_eq!(
        (
            results,
            rec.testing_calls.clone(),
            rec.disable_caching_calls.clone(),
            rec.end_exit_code,
        ),
        (
            vec![("cached".to_owned(), TestStatus::Fatal as i32)],
            vec![vec!["cached".to_owned()]],
            vec![false],
            Some(32),
        )
    );
}

#[tokio::test]
async fn empty_cached_failure_never_reports_pass() {
    let events = vec![("cached".to_string(), "ok")];
    let (orch, recorded, _server) = mock_orchestrator_with_injected_testing_response(
        events,
        InjectedTestingResponse::EmptyCachedFailure,
    )
    .await;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(42))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, test_config(), test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    let results: Vec<(String, i32)> = rec
        .results
        .iter()
        .map(|result| (result.name.clone(), result.status))
        .collect();
    assert_eq!(
        (
            results,
            rec.testing_calls.clone(),
            rec.disable_caching_calls.clone(),
            rec.end_exit_code,
        ),
        (
            vec![("cached".to_owned(), TestStatus::Fail as i32)],
            vec![vec!["cached".to_owned()]],
            vec![false],
            Some(32),
        )
    );
}

#[tokio::test]
async fn empty_unknown_response_never_reports_pass() {
    let events = vec![("unknown".to_string(), "ok")];
    let (orch, recorded, _server) = mock_orchestrator_with_injected_testing_response(
        events,
        InjectedTestingResponse::EmptyUnknownPass,
    )
    .await;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(42))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, test_config(), test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    let results: Vec<(String, i32)> = rec
        .results
        .iter()
        .map(|result| (result.name.clone(), result.status))
        .collect();
    assert_eq!(
        (
            results,
            rec.testing_calls.clone(),
            rec.disable_caching_calls.clone(),
            rec.end_exit_code,
        ),
        (
            vec![("unknown".to_owned(), TestStatus::Fatal as i32)],
            vec![vec!["unknown".to_owned()]],
            vec![false],
            Some(32),
        )
    );
}

#[tokio::test]
async fn resource_failures_retry_quarantine_and_retain_every_attempt() {
    struct ResourceCase {
        name: &'static str,
        marker: &'static str,
        status: TestStatus,
        failure_class: quokka::result::FailureClass,
    }

    for case in [
        ResourceCase {
            name: "oom",
            marker: "quokka logical test cgroup OOM: index=0 memory.max=1572864000 oom=0->1 oom_kill=0->1",
            status: TestStatus::Fail,
            failure_class: quokka::result::FailureClass::Fail,
        },
        ResourceCase {
            name: "timeout",
            marker: "quokka logical test timeout: index=0 seconds=300",
            status: TestStatus::Timeout,
            failure_class: quokka::result::FailureClass::Timeout,
        },
    ] {
        let events = vec![(case.name.to_owned(), "ok")];
        let authenticated_marker =
            case.marker
                .replacen("quokka ", "quokka resource {resource_marker_token} ", 1);
        let (orch, recorded, _server) =
            mock_orchestrator_stderr_crash(events, format!("{authenticated_marker}\n")).await;
        let mut config = test_config();
        config.duration_db = quokka::cli::DurationDbConfig::Disabled;
        config.cgroup_granularity = quokka::execution::CgroupGranularity::LogicalTest;

        let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
        intake_tx
            .send(SpecEnvelope::Spec(Box::new(libtest_spec_labeled(
                41,
                vec!["rust:flaky".to_owned(), "rust:quarantined".to_owned()],
            ))))
            .unwrap();
        intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

        drive_to_completion(orch, intake_rx, config, test_context()).await;
        let rec = recorded.lock().expect("recorded mutex poisoned");
        let expected_result = TestResult {
            name: case.name.to_owned(),
            status: case.status as i32,
            msg: None,
            target: Some(ConfiguredTargetHandle { id: 41 }),
            duration: Some(prost_types::Duration {
                seconds: 0,
                nanos: 1_000_000,
            }),
            details: format!(
                "{}\nquokka attempt history:\nattempt 1: {:?} duration_ms=1 details={}\nattempt 2: {:?} duration_ms=1 details={}\nattempt 3: {:?} duration_ms=1 details={}\n[brtr: target=root//rust/foo:foo | target_platform=cfg]",
                case.marker,
                case.failure_class,
                case.marker,
                case.failure_class,
                case.marker,
                case.failure_class,
                case.marker,
            )
            .replace("FailureClass::", ""),
            max_memory_used_bytes: Some(1024),
        };
        assert_eq!(
            (
                rec.report_attempts.clone(),
                rec.results.clone(),
                rec.testing_calls.clone(),
                rec.disable_caching_calls.clone(),
                rec.end_exit_code,
            ),
            (
                vec![expected_result.clone()],
                vec![expected_result],
                vec![
                    vec![case.name.to_owned()],
                    vec![case.name.to_owned()],
                    vec![case.name.to_owned()],
                ],
                vec![false, true, true],
                Some(0),
            )
        );
    }
}

#[tokio::test]
async fn logical_marker_with_bad_index_fails_closed_as_infra_failure() {
    let events = vec![("bad-index".to_owned(), "ok")];
    let marker =
        "quokka resource {resource_marker_token} logical test timeout: index=1 seconds=300\n";
    let (orch, recorded, _server) = mock_orchestrator_stderr_crash(events, marker.to_owned()).await;
    let mut config = test_config();
    config.duration_db = quokka::cli::DurationDbConfig::Disabled;
    config.cgroup_granularity = quokka::execution::CgroupGranularity::LogicalTest;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(44))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert_eq!(
        (
            rec.results
                .iter()
                .map(|result| (result.name.clone(), result.status))
                .collect::<Vec<_>>(),
            rec.testing_calls.clone(),
            rec.end_exit_code,
        ),
        (
            vec![("bad-index".to_owned(), TestStatus::InfraFailure as i32)],
            vec![vec!["bad-index".to_owned()]],
            Some(32),
        )
    );
}

#[tokio::test]
async fn libtest_filter_limits_discovery_and_scheduling() {
    let events = vec![
        ("alpha_one".to_string(), "ok"),
        ("beta_two".to_string(), "ok"),
        ("gamma_three".to_string(), "ok"),
    ];
    let (orch, recorded, _server) = mock_orchestrator(events).await;
    let mut config = test_config();
    config.extra_test_args = vec!["alpha".to_string()];

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(1))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert_eq!(
        rec.discovered,
        vec![(
            "root//rust/foo:foo".to_string(),
            vec!["alpha_one".to_string()]
        )]
    );
    assert_eq!(rec.testing_calls, vec![vec!["alpha_one".to_string()]]);
    assert_eq!(rec.results.len(), 1);
    assert_eq!(rec.results[0].name, "alpha_one");
    assert_eq!(rec.results[0].status, TestStatus::Pass as i32);
    assert_eq!(rec.end_exit_code, Some(0));
}

#[tokio::test]
async fn libtest_help_prints_without_listing_or_running() {
    let events = vec![("alpha_one".to_string(), "ok")];
    let (orch, recorded, _server) = mock_orchestrator(events).await;
    let mut config = test_config();
    config.extra_test_args = vec!["--help".to_string()];
    config.libtest_help_only = true;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(1))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert!(rec.listing_calls.is_empty());
    assert!(rec.testing_calls.is_empty());
    assert!(rec.results.is_empty());
    assert_eq!(rec.end_exit_code, Some(0));
}

#[tokio::test]
async fn libtest_usage_error_fails_without_listing_or_running() {
    let events = vec![("alpha_one".to_string(), "ok")];
    let (orch, recorded, _server) = mock_orchestrator(events).await;
    let mut config = test_config();
    config.extra_test_args = vec!["--format=json".to_string()];
    config.libtest_usage_error = Some(
        "The \"json\" format is only accepted on the nightly compiler with -Z unstable-options"
            .to_string(),
    );

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(1))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert!(rec.listing_calls.is_empty());
    assert!(rec.testing_calls.is_empty());
    assert!(rec.results.is_empty());
    assert_eq!(rec.end_exit_code, Some(32));
}

#[tokio::test]
async fn target_batching_runs_natively_and_reports_every_discovered_test() {
    let names = (0..65)
        .map(|index| format!("target_test_{index:03}_{}", "x".repeat(256)))
        .collect::<Vec<_>>();
    let events = names.iter().cloned().map(|name| (name, "ok")).collect();
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
    let expected_results = names
        .iter()
        .map(|name| TestResult {
            name: name.clone(),
            status: TestStatus::Pass as i32,
            msg: None,
            target: Some(ConfiguredTargetHandle { id: 3 }),
            duration: Some(prost_types::Duration {
                seconds: 0,
                nanos: 1_000_000,
            }),
            details: String::new(),
            max_memory_used_bytes: Some(1024),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        (
            rec.report_attempts.clone(),
            rec.results.clone(),
            rec.testing_calls.clone(),
            rec.testing_commands.clone(),
            rec.disable_caching_calls.clone(),
            rec.active_testing_calls,
            rec.end_exit_code,
        ),
        (
            expected_results.clone(),
            expected_results,
            vec![names],
            vec![vec![vec![
                "--exact".to_owned(),
                "--test-threads=1".to_owned(),
                "--color=never".to_owned(),
                "-Z".to_owned(),
                "unstable-options".to_owned(),
                "--format".to_owned(),
                "json".to_owned(),
            ]]],
            vec![false],
            0,
            Some(0),
        )
    );
}

#[tokio::test]
async fn target_batching_preserves_disjoint_shard_commands() {
    #[derive(Debug, PartialEq)]
    struct ShardEvidence {
        discovered: Vec<String>,
        result_names: Vec<String>,
        testing_calls: Vec<Vec<String>>,
        testing_commands: Vec<Vec<Vec<String>>>,
        end_exit_code: Option<i32>,
    }

    async fn run_shard(index: u16, names: &[String]) -> ShardEvidence {
        let events = names.iter().cloned().map(|name| (name, "ok")).collect();
        let (orch, recorded, _server) = mock_orchestrator(events).await;
        let mut config = test_config();
        config.batch_mode = quokka::batching::BatchMode::Target;
        config.shard = cli::ShardSpec { index, count: 2 };
        let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
        intake_tx
            .send(SpecEnvelope::Spec(Box::new(libtest_spec(i64::from(index)))))
            .unwrap();
        intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();
        drive_to_completion(orch, intake_rx, config, test_context()).await;
        let rec = recorded.lock().expect("recorded mutex poisoned");
        ShardEvidence {
            discovered: rec.discovered[0].1.clone(),
            result_names: rec
                .results
                .iter()
                .map(|result| result.name.clone())
                .collect(),
            testing_calls: rec.testing_calls.clone(),
            testing_commands: rec.testing_commands.clone(),
            end_exit_code: rec.end_exit_code,
        }
    }

    fn expected(evidence: &ShardEvidence) -> ShardEvidence {
        let mut command = evidence.discovered.clone();
        command.extend([
            "--exact".to_owned(),
            "--test-threads=1".to_owned(),
            "--color=never".to_owned(),
            "-Z".to_owned(),
            "unstable-options".to_owned(),
            "--format".to_owned(),
            "json".to_owned(),
        ]);
        ShardEvidence {
            discovered: evidence.discovered.clone(),
            result_names: evidence.discovered.clone(),
            testing_calls: vec![evidence.discovered.clone()],
            testing_commands: vec![vec![command]],
            end_exit_code: Some(0),
        }
    }

    let names = (0..65)
        .map(|index| format!("sharded_target_test_{index:03}"))
        .collect::<Vec<_>>();
    let shard_zero = run_shard(0, &names).await;
    let shard_one = run_shard(1, &names).await;
    let mut union = shard_zero.discovered.clone();
    union.extend(shard_one.discovered.clone());
    union.sort();
    let mut expected_union = names.clone();
    expected_union.sort();
    assert_eq!(
        (&shard_zero, &shard_one, union),
        (
            &expected(&shard_zero),
            &expected(&shard_one),
            expected_union
        )
    );
}

#[tokio::test]
async fn ci_report_retains_action_timeout_then_automatic_fresh_passes() {
    let events = vec![("x".to_owned(), "ok"), ("y".to_owned(), "ok")];
    let (orch, recorded, _server) = mock_orchestrator_full_with_stderr(
        events,
        None,
        None,
        None,
        None,
        Duration::ZERO,
        InjectedTestingResponse::FirstActionTimeout,
    )
    .await;
    let report = ci_report_path("action-timeout-isolation");
    let _ = std::fs::remove_file(&report);
    let mut config = test_config();
    config.batch_mode = quokka::batching::BatchMode::Target;
    config.ci_test_report_json = Some(report.clone());
    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(21))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();
    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let actual =
        semantic_ci_report(serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap());
    std::fs::remove_file(report).unwrap();
    let expected_test = |name: &str| {
        serde_json::json!({
            "target": "root//rust/foo:foo",
            "name": name,
            "variant": "default",
            "run_name": name,
            "repeat_index": 0,
            "status": "pass",
            "integrity": "complete",
            "quarantined": false,
            "labels": [],
            "action_or_batch_duration_seconds": 0.001,
            "max_memory_used_bytes": 1024,
            "attempts": [
                {
                    "ordinal": 0,
                    "execution_disposition": "physical",
                    "outcome": "timeout",
                    "action_or_batch_duration_seconds": 0.001,
                    "executor_environment": "local",
                },
                {
                    "ordinal": 1,
                    "execution_disposition": "physical",
                    "outcome": "pass",
                    "action_or_batch_duration_seconds": 0.001,
                    "executor_environment": "local",
                },
            ],
        })
    };
    assert_eq!(
        actual,
        semantic_ci_report(serde_json::json!({
            "schema": "quokka.ci-test-report.v1",
            "host_platform": null,
            "trace_id": null,
            "tests": [expected_test("x"), expected_test("y")],
        }))
    );
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert_eq!(
        (
            rec.testing_calls.clone(),
            rec.disable_caching_calls.clone(),
            rec.end_exit_code,
        ),
        (
            vec![
                vec!["x".to_owned(), "y".to_owned()],
                vec!["x".to_owned(), "y".to_owned()],
            ],
            vec![false, true],
            Some(0),
        )
    );
}

#[tokio::test]
async fn ordinary_singleton_action_timeout_gets_a_fresh_retry_without_a_flaky_label() {
    let events = vec![("ordinary".to_owned(), "ok")];
    let (orch, recorded, _server) = mock_orchestrator_full_with_stderr(
        events,
        None,
        None,
        None,
        None,
        Duration::ZERO,
        InjectedTestingResponse::FirstActionTimeout,
    )
    .await;
    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(42))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, test_config(), test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert_eq!(
        (
            rec.results
                .iter()
                .map(|result| (result.name.clone(), result.status))
                .collect::<Vec<_>>(),
            rec.testing_calls.clone(),
            rec.disable_caching_calls.clone(),
            rec.end_exit_code,
        ),
        (
            vec![("ordinary".to_owned(), TestStatus::Pass as i32)],
            vec![vec!["ordinary".to_owned()], vec!["ordinary".to_owned()]],
            vec![false, true],
            Some(0),
        )
    );
}

#[tokio::test]
async fn action_timeout_does_not_consume_flaky_failure_retry() {
    let events = vec![("flaky".to_owned(), "ok")];
    let (orch, recorded, _server) = mock_orchestrator_full_with_stderr(
        events,
        None,
        Some("flaky".to_owned()),
        None,
        None,
        Duration::ZERO,
        InjectedTestingResponse::FirstActionTimeout,
    )
    .await;
    let mut config = test_config();
    config.flaky_attempts = 2;
    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec_labeled(
            43,
            vec!["rust:flaky".to_owned()],
        ))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert_eq!(
        (
            rec.results
                .iter()
                .map(|result| (result.name.clone(), result.status))
                .collect::<Vec<_>>(),
            rec.testing_calls.clone(),
            rec.disable_caching_calls.clone(),
            rec.end_exit_code,
        ),
        (
            vec![("flaky".to_owned(), TestStatus::Pass as i32)],
            vec![
                vec!["flaky".to_owned()],
                vec!["flaky".to_owned()],
                vec!["flaky".to_owned()],
            ],
            vec![false, true, true],
            Some(0),
        )
    );
}

#[tokio::test]
async fn ci_report_identifies_cache_served_attempts() {
    let events = vec![("cached".to_owned(), "ok")];
    let (orch, _recorded, _server) = mock_orchestrator_full_with_stderr(
        events,
        None,
        None,
        None,
        None,
        Duration::ZERO,
        InjectedTestingResponse::NamedCachedPass,
    )
    .await;
    let report = ci_report_path("cache-served");
    let _ = std::fs::remove_file(&report);
    let mut config = test_config();
    config.batch_mode = quokka::batching::BatchMode::Target;
    config.ci_test_report_json = Some(report.clone());
    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(22))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();
    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let actual =
        semantic_ci_report(serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap());
    std::fs::remove_file(report).unwrap();
    assert_eq!(
        actual,
        semantic_ci_report(serde_json::json!({
            "schema": "quokka.ci-test-report.v1",
            "host_platform": null,
            "trace_id": null,
            "tests": [{
                "target": "root//rust/foo:foo",
                "name": "cached",
                "variant": "default",
                "run_name": "cached",
                "repeat_index": 0,
                "status": "pass",
                "integrity": "complete",
                "quarantined": false,
                "labels": [],
                "action_or_batch_duration_seconds": 0.001,
                "max_memory_used_bytes": 1024,
                "attempts": [{
                    "ordinal": 0,
                    "execution_disposition": "cache_served",
                    "outcome": "pass",
                    "action_or_batch_duration_seconds": 0.001,
                    "executor_environment": "remote",
                }],
            }],
        }))
    );
}

#[tokio::test]
async fn ci_report_omits_unmeasured_cancelled_attempt_duration() {
    let events = vec![("cancelled".to_owned(), "ok")];
    let (orch, _recorded, _server) = mock_orchestrator_full_with_stderr(
        events,
        None,
        None,
        None,
        None,
        Duration::ZERO,
        InjectedTestingResponse::CancelledUnspecified,
    )
    .await;
    let report = ci_report_path("unmeasured-cancelled");
    let _ = std::fs::remove_file(&report);
    let mut config = test_config();
    config.batch_mode = quokka::batching::BatchMode::Target;
    config.ci_test_report_json = Some(report.clone());
    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(23))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();
    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let actual =
        semantic_ci_report(serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap());
    std::fs::remove_file(report).unwrap();
    assert_eq!(
        actual,
        semantic_ci_report(serde_json::json!({
            "schema": "quokka.ci-test-report.v1",
            "host_platform": null,
            "trace_id": null,
            "tests": [{
                "target": "root//rust/foo:foo",
                "name": "cancelled",
                "variant": "default",
                "run_name": "cancelled",
                "repeat_index": 0,
                "status": "omitted",
                "integrity": "missing_terminal_result",
                "quarantined": false,
                "labels": [],
                "action_or_batch_duration_seconds": null,
                "max_memory_used_bytes": null,
                "attempts": [{
                    "ordinal": 0,
                    "execution_disposition": "unknown",
                    "outcome": "omitted",
                    "executor_environment": "unknown",
                }],
            }],
        }))
    );
}

#[tokio::test]
async fn ci_report_retains_stable_stress_repeat_identity() {
    let events = vec![("repeated".to_owned(), "ok")];
    let (orch, _recorded, _server) = mock_orchestrator_full_with_stderr(
        events,
        None,
        None,
        None,
        None,
        Duration::ZERO,
        InjectedTestingResponse::Normal,
    )
    .await;
    let report = ci_report_path("stress-repeat-identity");
    let _ = std::fs::remove_file(&report);
    let mut config = test_config();
    config.batch_mode = quokka::batching::BatchMode::Target;
    config.stress = RepeatKind::Stress(NonZeroU32::new(2).expect("nonzero"));
    config.ci_test_report_json = Some(report.clone());
    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(24))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();
    drive_to_completion(orch, intake_rx, config, test_context()).await;

    let actual =
        semantic_ci_report(serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap());
    std::fs::remove_file(report).unwrap();
    let records = actual["tests"].as_array().expect("test records");
    let identities = records
        .iter()
        .map(|record| {
            (
                record["run_name"].as_str().unwrap().to_owned(),
                record["repeat_index"].as_u64().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        identities,
        vec![
            ("repeated#rep0".to_owned(), 0),
            ("repeated#rep1".to_owned(), 1)
        ]
    );
}

#[tokio::test]
async fn batch_isolation_preserves_missing_result_after_singleton_pass() {
    let events = vec![("x".to_string(), "ok"), ("y".to_string(), "ok")];
    let (orch, recorded, _server) =
        mock_orchestrator_full(events, Some("y".to_string()), None, None).await;
    let mut config = test_config();
    config.batch_mode = quokka::batching::BatchMode::Target;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(11))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    let results = rec
        .results
        .iter()
        .map(|result| (result.name.clone(), result.status))
        .collect::<Vec<_>>();
    assert_eq!(
        (results, rec.testing_calls.clone(), rec.end_exit_code),
        (
            vec![
                ("x".to_owned(), TestStatus::Pass as i32),
                ("y".to_owned(), TestStatus::Fatal as i32),
            ],
            vec![vec!["x".to_owned(), "y".to_owned()], vec!["y".to_owned()]],
            Some(32),
        )
    );
}

#[tokio::test]
async fn action_timeout_budget_exhaustion_does_not_enter_flaky_retry() {
    let events = vec![("x".to_owned(), "ok"), ("y".to_owned(), "ok")];
    let marker =
        "quokka resource {resource_marker_token} test action timeout: seconds=300\n".to_owned();
    let (orch, recorded, _server) = mock_orchestrator_stderr_crash(events, marker).await;
    let mut config = test_config();
    config.batch_mode = quokka::batching::BatchMode::Target;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec_labeled(
            19,
            vec!["rust:flaky".to_owned()],
        ))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    let results = rec
        .results
        .iter()
        .map(|result| (result.name.clone(), result.status))
        .collect::<Vec<_>>();
    assert_eq!(
        (
            results,
            rec.testing_calls.clone(),
            rec.disable_caching_calls.clone(),
            rec.end_exit_code,
        ),
        (
            vec![
                ("x".to_owned(), TestStatus::Timeout as i32),
                ("y".to_owned(), TestStatus::Timeout as i32),
            ],
            vec![
                vec!["x".to_owned(), "y".to_owned()],
                vec!["x".to_owned(), "y".to_owned()],
                vec!["x".to_owned(), "y".to_owned()],
            ],
            vec![false, true, true],
            Some(32),
        )
    );
}

#[tokio::test]
async fn batch_isolation_overlaps_singletons_with_ordered_complete_results() {
    let events = vec![
        ("a".to_string(), "failed"),
        ("b".to_string(), "failed"),
        ("c".to_string(), "failed"),
    ];
    let (orch, recorded, _server) =
        mock_orchestrator_with_testing_delay(events, Duration::from_millis(50)).await;
    let mut config = test_config();
    config.batch_mode = quokka::batching::BatchMode::Target;
    config.limits.max_inflight_per_target = 2;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(17))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    let mut singleton_calls = rec
        .testing_calls
        .iter()
        .filter(|call| call.len() == 1)
        .cloned()
        .collect::<Vec<_>>();
    singleton_calls.sort();

    #[derive(Debug, PartialEq)]
    struct IsolationEvidence {
        results: Vec<TestResult>,
        singleton_calls: Vec<Vec<String>>,
        max_active_testing_calls: usize,
        active_testing_calls: usize,
        end_exit_code: Option<i32>,
    }

    let expected_result = |name: &str| {
        TestResult {
        name: name.to_owned(),
        status: TestStatus::Fail as i32,
        msg: None,
        target: Some(ConfiguredTargetHandle { id: 17 }),
        duration: Some(prost_types::Duration {
            seconds: 0,
            nanos: 1_000_000,
        }),
        details: "boom\nquokka attempt history:\nattempt 1: Fail duration_ms=1 details=boom\nattempt 2: Fail duration_ms=1 details=boom\n[brtr: target=root//rust/foo:foo | target_platform=cfg]".to_owned(),
        max_memory_used_bytes: Some(1024),
    }
    };
    assert_eq!(
        IsolationEvidence {
            results: rec.results.clone(),
            singleton_calls,
            max_active_testing_calls: rec.max_active_testing_calls,
            active_testing_calls: rec.active_testing_calls,
            end_exit_code: rec.end_exit_code,
        },
        IsolationEvidence {
            results: vec![
                expected_result("a"),
                expected_result("b"),
                expected_result("c"),
            ],
            singleton_calls: vec![
                vec!["a".to_owned()],
                vec!["b".to_owned()],
                vec!["c".to_owned()],
            ],
            max_active_testing_calls: 2,
            active_testing_calls: 0,
            end_exit_code: Some(32),
        }
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
        mock_orchestrator_full(events, None, Some("f".to_string()), None).await;
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

    #[derive(Debug, PartialEq)]
    struct FlakyRetryEvidence {
        report_attempts: Vec<TestResult>,
        accepted_results: Vec<TestResult>,
        testing_calls: Vec<Vec<String>>,
        disable_caching_calls: Vec<bool>,
        end_exit_code: Option<i32>,
    }

    let expected_result = |name: &str, details: &str| TestResult {
        name: name.to_owned(),
        status: TestStatus::Pass as i32,
        msg: None,
        target: Some(ConfiguredTargetHandle { id: 13 }),
        duration: Some(prost_types::Duration {
            seconds: 0,
            nanos: 1_000_000,
        }),
        details: details.to_owned(),
        max_memory_used_bytes: Some(1024),
    };
    assert_eq!(
        FlakyRetryEvidence {
            report_attempts: rec.report_attempts.clone(),
            accepted_results: rec.results.clone(),
            testing_calls: rec.testing_calls.clone(),
            disable_caching_calls: rec.disable_caching_calls.clone(),
            end_exit_code: rec.end_exit_code,
        },
        FlakyRetryEvidence {
            report_attempts: vec![
                expected_result("p", ""),
                expected_result(
                    "f",
                    "quokka attempt history:\nattempt 1: Fail duration_ms=1 details=boom\nattempt 2: Pass duration_ms=1",
                ),
            ],
            accepted_results: vec![
                expected_result("p", ""),
                expected_result(
                    "f",
                    "quokka attempt history:\nattempt 1: Fail duration_ms=1 details=boom\nattempt 2: Pass duration_ms=1",
                ),
            ],
            testing_calls: vec![vec!["p".to_owned(), "f".to_owned()], vec!["f".to_owned()],],
            disable_caching_calls: vec![false, true],
            end_exit_code: Some(0),
        }
    );
}

#[tokio::test]
async fn flaky_retries_and_failure_isolation_never_reenable_caching() {
    let events = vec![("p".to_string(), "ok"), ("f".to_string(), "failed")];
    let (orch, recorded, _server) = mock_orchestrator(events).await;
    let mut config = test_config();
    config.batch_mode = quokka::batching::BatchMode::Target;
    config.flaky_attempts = 3;

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec_labeled(
            31,
            vec!["rust:flaky".to_string()],
        ))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");

    #[derive(Debug, PartialEq)]
    struct PostNonpassCacheEvidence {
        report_attempts: Vec<TestResult>,
        accepted_results: Vec<TestResult>,
        testing_calls: Vec<Vec<String>>,
        disable_caching_calls: Vec<bool>,
        end_exit_code: Option<i32>,
    }

    let expected_result = |name: &str, status: TestStatus, details: &str| TestResult {
        name: name.to_owned(),
        status: status as i32,
        msg: None,
        target: Some(ConfiguredTargetHandle { id: 31 }),
        duration: Some(prost_types::Duration {
            seconds: 0,
            nanos: 1_000_000,
        }),
        details: details.to_owned(),
        max_memory_used_bytes: Some(1024),
    };
    let expected_results = vec![
        expected_result("p", TestStatus::Pass, ""),
        expected_result(
            "f",
            TestStatus::Fail,
            "boom\nquokka attempt history:\nattempt 1: Fail duration_ms=1 details=boom\nattempt 2: Fail duration_ms=1 details=boom\nattempt 3: Fail duration_ms=1 details=boom\nattempt 4: Fail duration_ms=1 details=boom\nattempt 5: Fail duration_ms=1 details=boom\nattempt 6: Fail duration_ms=1 details=boom\n[brtr: target=root//rust/foo:foo | target_platform=cfg]",
        ),
    ];
    assert_eq!(
        PostNonpassCacheEvidence {
            report_attempts: rec.report_attempts.clone(),
            accepted_results: rec.results.clone(),
            testing_calls: rec.testing_calls.clone(),
            disable_caching_calls: rec.disable_caching_calls.clone(),
            end_exit_code: rec.end_exit_code,
        },
        PostNonpassCacheEvidence {
            report_attempts: expected_results.clone(),
            accepted_results: expected_results,
            testing_calls: vec![
                vec!["p".to_owned(), "f".to_owned()],
                vec!["f".to_owned()],
                vec!["f".to_owned()],
                vec!["f".to_owned()],
                vec!["f".to_owned()],
                vec!["f".to_owned()],
            ],
            disable_caching_calls: vec![false, true, true, true, true, true],
            end_exit_code: Some(32),
        }
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
        mock_orchestrator_full(events, None, Some("flaky".to_string()), None).await;
    let mut config = test_config();
    config.duration_db = quokka::cli::DurationDbConfig::Persistent(dir.clone());

    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec_labeled(
            21,
            vec!["rust:flaky".to_string()],
        ))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

    drive_to_completion(orch, intake_rx, config, test_context()).await;

    let db = quokka::duration_db::DurationDb::load(dir.clone());
    let flaky_id = quokka::result::TestIdentity {
        target: "root//rust/foo:foo".into(),
        name: "flaky".into(),
        variant: quokka::variant::Variant::Default,
    };
    let steady_id = quokka::result::TestIdentity {
        target: "root//rust/foo:foo".into(),
        name: "steady".into(),
        variant: quokka::variant::Variant::Default,
    };

    let flaky_flake = db.flake(None, &flaky_id).unwrap();
    assert_eq!(flaky_flake.runs, 2, "flaky test should record 2 runs");
    assert_eq!(
        flaky_flake.failures, 1,
        "flaky test should record 1 failure"
    );

    let steady_flake = db.flake(None, &steady_id).unwrap();
    assert_eq!(steady_flake.runs, 1, "steady test should record 1 run");
    assert_eq!(
        steady_flake.failures, 0,
        "steady test should record 0 failures"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn disabled_duration_db_path_does_not_cache_bust_unseen_tests() {
    let events = vec![("a".to_string(), "ok")];
    let (orch, recorded, _server) = mock_orchestrator(events).await;
    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
    intake_tx
        .send(SpecEnvelope::Spec(Box::new(libtest_spec(1))))
        .unwrap();
    intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();
    let temp_db = std::env::temp_dir().join(format!(
        "quokka-disabled-db-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let mut config = test_config();
    config.duration_db = quokka::cli::DurationDbConfig::Disabled;

    drive_to_completion(orch, intake_rx, config, test_context()).await;
    let rec = recorded.lock().expect("recorded mutex poisoned");
    assert_eq!(rec.disable_caching_calls, vec![false]);
}

#[tokio::test]
async fn unseen_then_seen_test_caching() {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let temp_dir = std::env::temp_dir().join(format!("quokka-test-db-{}", ts));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let events = vec![("a".to_string(), "ok")];

    // First run: cold/empty database (unseen test)
    {
        let (orch, recorded, _server) = mock_orchestrator(events.clone()).await;
        let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
        intake_tx
            .send(SpecEnvelope::Spec(Box::new(libtest_spec(1))))
            .unwrap();
        intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

        let mut config = test_config();
        config.duration_db = quokka::cli::DurationDbConfig::Persistent(temp_dir.clone());

        drive_to_completion(orch, intake_rx, config, test_context()).await;
        let rec = recorded.lock().expect("recorded mutex poisoned");
        assert!(!rec.disable_caching_calls.is_empty());
        assert!(
            rec.disable_caching_calls[0],
            "expected caching disabled for unseen test"
        );
    }

    // Second run: database now contains the test (seen test)
    {
        let (orch, recorded, _server) = mock_orchestrator(events).await;
        let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
        intake_tx
            .send(SpecEnvelope::Spec(Box::new(libtest_spec(1))))
            .unwrap();
        intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

        let mut config = test_config();
        config.duration_db = quokka::cli::DurationDbConfig::Persistent(temp_dir.clone());

        drive_to_completion(orch, intake_rx, config, test_context()).await;
        let rec = recorded.lock().expect("recorded mutex poisoned");
        assert!(!rec.disable_caching_calls.is_empty());
        assert!(
            !rec.disable_caching_calls[0],
            "expected caching enabled for seen test"
        );
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn flaky_retry_via_toml_config() {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let temp_home = std::env::temp_dir().join(format!("quokka-home-{}", ts));
    let config_dir = temp_home.join(".quokka");
    std::fs::create_dir_all(&config_dir).unwrap();

    // Write config toml
    let config_toml = r#"
[flaky_retry]
attempts = 3
"#;
    std::fs::write(config_dir.join("config.toml"), config_toml).unwrap();

    let temp_db = std::env::temp_dir().join(format!("quokka-db-{}", ts));
    std::fs::create_dir_all(&temp_db).unwrap();

    // 1. First, populate the database with a failure so it is "known to flake"
    let events = vec![("a".to_string(), "failed")];
    {
        let (orch, _recorded, _server) = mock_orchestrator(events.clone()).await;
        let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
        intake_tx
            .send(SpecEnvelope::Spec(Box::new(libtest_spec(1))))
            .unwrap();
        intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

        let mut config = test_config();
        config.duration_db = quokka::cli::DurationDbConfig::Persistent(temp_db.clone());
        config.quokka_config = quokka::config::load_config_from_home(Some(temp_home.clone()));

        drive_to_completion(orch, intake_rx, config, test_context()).await;
    }

    // 2. Now run the test again, but configure the mock orchestrator so "a" fails the first time
    // and passes thereafter (flaky_once). The DB already has recorded 1 failure for it,
    // so it is known to flake. Thus, it should retry up to 3 times (from TOML config) and PASS.
    let events = vec![("a".to_string(), "ok")];
    {
        let (orch, recorded, _server) =
            mock_orchestrator_full(events, None, Some("a".to_string()), None).await;
        let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();
        intake_tx
            .send(SpecEnvelope::Spec(Box::new(libtest_spec(1))))
            .unwrap();
        intake_tx.send(SpecEnvelope::EndOfRequests).unwrap();

        let mut config = test_config();
        config.duration_db = quokka::cli::DurationDbConfig::Persistent(temp_db.clone());
        config.quokka_config = quokka::config::load_config_from_home(Some(temp_home.clone()));

        drive_to_completion(orch, intake_rx, config, test_context()).await;
        let rec = recorded.lock().expect("recorded mutex poisoned");
        assert_eq!(rec.results.len(), 1);
        assert_eq!(
            rec.results[0].status,
            TestStatus::Pass as i32,
            "should pass on retry"
        );
        assert_eq!(rec.testing_seen.get("a"), Some(&2), "should have run twice");
        assert_eq!(rec.end_exit_code, Some(0), "verdict should be PASS");
    }

    // Clean up
    let _ = std::fs::remove_dir_all(&temp_home);
    let _ = std::fs::remove_dir_all(&temp_db);
}
