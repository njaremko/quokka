//! Typed client for Buck2's `TestOrchestrator` (and `DownwardApi`) services.
//!
//! The runner is a *client* of these services: it asks buck2 to execute listing
//! and test actions ([`Self::execute2`]), and reports discovery/results back.
//! buck2 owns execution, caching, artifact expansion, and stdout/stderr capture
//! — the runner never runs a test binary itself.
//!
//! Both services are multiplexed over the single orchestrator connection, so we
//! build both generated clients from one cloned [`Channel`]. tonic clients are
//! cheap to clone and clone-per-call is the idiomatic way to issue concurrent
//! requests, so this wrapper is `Clone` and shared across all scheduler tasks.

use anyhow::Context as _;
use tonic::transport::Channel;
use tracing::Level;

use crate::proto::downward_api::downward_api_client::DownwardApiClient;
use crate::proto::downward_api::{self, ConsoleRequest};
use crate::proto::test::test_orchestrator_client::TestOrchestratorClient;
use crate::proto::test::{
    AttachInfoMessageRequest, CasDigest, ConfiguredTargetHandle, EndOfTestResultsRequest,
    ExecuteRequest2, ExecuteResponse2, ReportTestResultRequest, ReportTestSessionRequest,
    ReportTestsDiscoveredRequest, TestResult, Testing, TtlConfig, UploadFileToCasRequest,
};

/// Handle to buck2's orchestrator, shared across all scheduler tasks.
#[derive(Clone)]
pub struct Orchestrator {
    orchestrator: TestOrchestratorClient<Channel>,
    downward: DownwardApiClient<Channel>,
}

impl Orchestrator {
    /// Wrap a channel already connected to buck2's orchestrator server.
    ///
    /// Message-size caps are removed (`usize::MAX`) to match buck2's own client,
    /// since a chatty test binary's captured stdout/stderr can exceed the
    /// default 4 MiB decode ceiling.
    pub fn new(channel: Channel) -> Self {
        let orchestrator = TestOrchestratorClient::new(channel.clone())
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        let downward = DownwardApiClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        Self {
            orchestrator,
            downward,
        }
    }

    /// Ask buck2 to execute a single listing or test action. buck2 expands the
    /// command's opaque arg/env handles, resolves the executor (RE/local), reads
    /// or writes the action cache, runs it, and returns captured output.
    pub async fn execute2(&self, request: ExecuteRequest2) -> anyhow::Result<ExecuteResponse2> {
        Ok(self
            .orchestrator
            .clone()
            .execute2(request)
            .await
            .context("Execute2 RPC failed")?
            .into_inner())
    }

    /// Report the testcases discovered for a target's suite (after listing).
    pub async fn report_tests_discovered(
        &self,
        target: ConfiguredTargetHandle,
        suite: String,
        tests: Vec<String>,
    ) -> anyhow::Result<()> {
        self.orchestrator
            .clone()
            .report_tests_discovered(ReportTestsDiscoveredRequest {
                target: Some(target),
                testing: Some(Testing {
                    suite,
                    testcases: tests,
                    variant: None,
                    repeat_count: None,
                }),
            })
            .await
            .context("ReportTestsDiscovered RPC failed")?;
        Ok(())
    }

    /// Report the outcome of a single test.
    pub async fn report_test_result(&self, result: TestResult) -> anyhow::Result<()> {
        self.orchestrator
            .clone()
            .report_test_result(ReportTestResultRequest {
                result: Some(result),
            })
            .await
            .context("ReportTestResult RPC failed")?;
        Ok(())
    }

    /// Report a free-form summary about this test session.
    pub async fn report_test_session(
        &self,
        session_info: String,
        test_session_id: Option<String>,
    ) -> anyhow::Result<()> {
        self.orchestrator
            .clone()
            .report_test_session(ReportTestSessionRequest {
                session_info,
                test_session_id,
            })
            .await
            .context("ReportTestSession RPC failed")?;
        Ok(())
    }

    /// Signal that all results have been reported and supply the exit code the
    /// `buck2 test` invocation should return.
    pub async fn end_of_test_results(&self, exit_code: i32) -> anyhow::Result<()> {
        self.orchestrator
            .clone()
            .end_of_test_results(EndOfTestResultsRequest { exit_code })
            .await
            .context("EndOfTestResults RPC failed")?;
        Ok(())
    }

    /// Attach an info message to be surfaced to the user by buck2.
    pub async fn attach_info_message(&self, message: String) -> anyhow::Result<()> {
        self.orchestrator
            .clone()
            .attach_info_message(AttachInfoMessageRequest { message })
            .await
            .context("AttachInfoMessage RPC failed")?;
        Ok(())
    }

    /// Upload a local file (e.g. an oversized test log) to CAS via buck2's RE
    /// client and return its digest.
    pub async fn upload_file_to_cas(
        &self,
        local_path: String,
        ttl_seconds: i64,
        use_case: String,
    ) -> anyhow::Result<CasDigest> {
        let response = self
            .orchestrator
            .clone()
            .upload_file_to_cas(UploadFileToCasRequest {
                local_path,
                ttl_config: Some(TtlConfig {
                    ttl_seconds,
                    use_case,
                }),
            })
            .await
            .context("UploadFileToCas RPC failed")?
            .into_inner();
        response
            .digest
            .context("UploadFileToCas response missing digest")
    }

    /// Print a line to the live buck2 console (DownwardApi).
    ///
    /// `console` is the ONLY DownwardApi method buck2's external-runner host
    /// implements (`BuckTestDownwardApi` in app/buck2_test/src/downward_api.rs):
    /// it forwards to `eprintln!`. The sibling `Log` and `External` methods are
    /// `unimplemented!()` and PANIC the buck2 daemon if called, so the runner
    /// must never invoke them — there is deliberately no `log`/`external` wrapper.
    pub async fn console(&self, level: Level, message: String) -> anyhow::Result<()> {
        self.downward
            .clone()
            .console(ConsoleRequest {
                level: Some(to_downward_level(level)),
                message,
            })
            .await
            .context("DownwardApi.Console RPC failed")?;
        Ok(())
    }
}

fn to_downward_level(level: Level) -> downward_api::LogLevel {
    use downward_api::log_level::Value;
    let value = match level {
        Level::TRACE => Value::Trace,
        Level::DEBUG => Value::Debug,
        Level::INFO => Value::Info,
        Level::WARN => Value::Warn,
        Level::ERROR => Value::Error,
    };
    downward_api::LogLevel {
        value: value as i32,
    }
}
