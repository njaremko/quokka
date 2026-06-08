//! The `TestExecutor` gRPC service the runner *serves* to buck2.
//!
//! buck2 drives this side: it calls [`external_runner_spec`] once per test
//! target (delivering the target's [`ExternalRunnerSpec`]) and then
//! [`end_of_test_requests`] once, after every spec RPC has returned, to signal
//! that no more targets are coming. We forward each spec into the scheduler's
//! intake channel and forward the end-of-requests signal as an explicit
//! envelope variant so the scheduler can drain and finish.
//!
//! [`external_runner_spec`]: ExecutorService::external_runner_spec
//! [`end_of_test_requests`]: ExecutorService::end_of_test_requests

use tokio::sync::mpsc::UnboundedSender;
use tonic::{Request, Response, Status};

use crate::proto::test::test_executor_server::TestExecutor;
use crate::proto::test::{
    Empty, ExternalRunnerSpec, ExternalRunnerSpecRequest, UnstableHeapDumpRequest,
    UnstableHeapDumpResponse,
};

/// One item delivered from buck2's `TestExecutor` calls into the scheduler.
///
/// Modelled as an enum (rather than closing the channel) so end-of-requests is
/// an explicit, observable event in the scheduler's intake loop.
pub enum SpecEnvelope {
    /// A target's external runner spec to list and run.
    Spec(Box<ExternalRunnerSpec>),
    /// buck2 has sent every target it intends to; no more `Spec`s will arrive.
    EndOfRequests,
}

pub type SpecSender = UnboundedSender<SpecEnvelope>;

/// Service implementation handed to the tonic server on the executor channel.
pub struct ExecutorService {
    intake: SpecSender,
}

impl ExecutorService {
    pub fn new(intake: SpecSender) -> Self {
        Self { intake }
    }
}

#[tonic::async_trait]
impl TestExecutor for ExecutorService {
    async fn external_runner_spec(
        &self,
        request: Request<ExternalRunnerSpecRequest>,
    ) -> Result<Response<Empty>, Status> {
        let spec = request.into_inner().test_spec.ok_or_else(|| {
            Status::invalid_argument("ExternalRunnerSpecRequest missing test_spec")
        })?;
        // The only way this send fails is if the scheduler's intake receiver was
        // already dropped, which means the run is tearing down; report it so
        // buck2 surfaces a clear error rather than silently losing a target.
        self.intake
            .send(SpecEnvelope::Spec(Box::new(spec)))
            .map_err(|_| {
                Status::unavailable("test runner scheduler is no longer accepting specs")
            })?;
        Ok(Response::new(Empty {}))
    }

    async fn end_of_test_requests(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Empty>, Status> {
        self.intake.send(SpecEnvelope::EndOfRequests).map_err(|_| {
            Status::unavailable("test runner scheduler is no longer accepting specs")
        })?;
        Ok(Response::new(Empty {}))
    }

    async fn unstable_heap_dump(
        &self,
        _request: Request<UnstableHeapDumpRequest>,
    ) -> Result<Response<UnstableHeapDumpResponse>, Status> {
        // The runner uses the system allocator with no heap-profiling hook, so
        // there is nothing to dump. Report it honestly rather than writing an
        // empty file that looks like a successful dump.
        Err(Status::unimplemented("quokka does not support heap dumps"))
    }
}
