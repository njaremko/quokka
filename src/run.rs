//! The run lifecycle: wire the two channels, serve the executor, drive the
//! scheduler, and — on EVERY exit path, including a scheduler panic — report
//! `end_of_test_results` before shutting the executor server down.
//!
//! buck2 enforces this sequence: it calls `end_of_test_requests` (closing our
//! intake), drains results, and waits for `end_of_test_results`. If the runner
//! process exits without sending it, buck2 fails the whole `buck2 test` with
//! "Executor exited without reporting end-of-tests". So the call is structurally
//! guaranteed here, not left to Drop ordering.

use futures::FutureExt;
use tokio::sync::oneshot;

use crate::cli::{RunnerConfig, Transport};
use crate::executor_server::ExecutorService;
use crate::orchestrator::Orchestrator;
use crate::proto::test::test_executor_server::TestExecutorServer;
use crate::transport::{DuplexChannel, connect_client, serve_connection};

/// Exit code reported when the scheduler itself panics (a runner bug). Still
/// reported via `end_of_test_results` so buck2 fails cleanly rather than hanging.
const SCHEDULER_PANIC_EXIT: i32 = 70;

/// Run the whole test session over the given transport.
pub async fn run(
    transport: Transport,
    config: RunnerConfig,
    context: crate::cli::SessionContext,
) -> anyhow::Result<()> {
    let (intake_tx, intake_rx) = tokio::sync::mpsc::unbounded_channel();

    // Serve the TestExecutor service (buck2 -> runner) on the executor channel.
    let executor_router = tonic::transport::Server::builder().add_service(
        TestExecutorServer::new(ExecutorService::new(intake_tx))
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let (executor_io, orchestrator_channel) = match transport {
        Transport::UnixFds {
            executor_fd,
            orchestrator_fd,
        } => connect_unix(executor_fd, orchestrator_fd).await?,
        Transport::Tcp {
            executor_addr,
            orchestrator_addr,
        } => connect_tcp(&executor_addr, &orchestrator_addr).await?,
    };

    let server = tokio::spawn(async move {
        serve_connection(executor_io, executor_router, async move {
            let _ = shutdown_rx.await;
        })
        .await
    });

    let orch = Orchestrator::new(orchestrator_channel);

    // Run the scheduler and unconditionally report end-of-tests.
    drive_to_completion(orch, intake_rx, config, context).await;

    // Stop serving and wait for the server task.
    let _ = shutdown_tx.send(());
    match server.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("quokka: executor server error: {e}"),
        Err(e) => eprintln!("quokka: executor server task panicked: {e}"),
    }
    Ok(())
}

/// Run the scheduler to completion and then ALWAYS report `end_of_test_results`,
/// including when the scheduler panics (a runner bug is reported as a failing
/// run, never a hang). This is the single guarantee buck2 depends on.
pub async fn drive_to_completion(
    orch: Orchestrator,
    intake_rx: tokio::sync::mpsc::UnboundedReceiver<crate::executor_server::SpecEnvelope>,
    config: RunnerConfig,
    context: crate::cli::SessionContext,
) {
    let exit_code = match std::panic::AssertUnwindSafe(crate::scheduler::run(
        orch.clone(),
        intake_rx,
        config,
        context,
    ))
    .catch_unwind()
    .await
    {
        Ok(code) => code,
        Err(_) => {
            eprintln!("quokka: scheduler panicked; reporting failure");
            SCHEDULER_PANIC_EXIT
        }
    };

    if let Err(e) = orch.end_of_test_results(exit_code).await {
        eprintln!("quokka: failed to send end_of_test_results: {e:#}");
    }
}

type ExecutorIo =
    DuplexChannel<tokio::io::ReadHalf<DuplexStream>, tokio::io::WriteHalf<DuplexStream>>;

/// A stream that is either a Unix socket or a TCP socket, so the executor server
/// and orchestrator client are transport-agnostic.
pub enum DuplexStream {
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    Tcp(tokio::net::TcpStream),
}

impl tokio::io::AsyncRead for DuplexStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            DuplexStream::Unix(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            DuplexStream::Tcp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for DuplexStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            #[cfg(unix)]
            DuplexStream::Unix(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            DuplexStream::Tcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            DuplexStream::Unix(s) => std::pin::Pin::new(s).poll_flush(cx),
            DuplexStream::Tcp(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            DuplexStream::Unix(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            DuplexStream::Tcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

fn split_executor(stream: DuplexStream) -> ExecutorIo {
    let (read, write) = tokio::io::split(stream);
    DuplexChannel::new(read, write)
}

#[cfg(unix)]
async fn connect_unix(
    executor_fd: i32,
    orchestrator_fd: i32,
) -> anyhow::Result<(ExecutorIo, tonic::transport::Channel)> {
    let executor = DuplexStream::Unix(unix_stream_from_fd(executor_fd)?);
    let orchestrator = unix_stream_from_fd(orchestrator_fd)?;
    let channel = connect_client(orchestrator, "orchestrator").await?;
    Ok((split_executor(executor), channel))
}

#[cfg(not(unix))]
async fn connect_unix(
    _executor_fd: i32,
    _orchestrator_fd: i32,
) -> anyhow::Result<(ExecutorIo, tonic::transport::Channel)> {
    anyhow::bail!("--executor-fd/--orchestrator-fd are only supported on unix; use TCP")
}

#[cfg(unix)]
fn unix_stream_from_fd(fd: i32) -> anyhow::Result<tokio::net::UnixStream> {
    use std::os::unix::io::FromRawFd as _;
    // SAFETY: buck2 hands us a valid, open socketpair endpoint with CLOEXEC
    // cleared (app/buck2_test/src/unix/executor.rs). We take sole ownership of
    // the fd here; it is not used elsewhere in this process.
    #[allow(unsafe_code)]
    let std_stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
    std_stream.set_nonblocking(true)?;
    Ok(tokio::net::UnixStream::from_std(std_stream)?)
}

async fn connect_tcp(
    executor_addr: &str,
    orchestrator_addr: &str,
) -> anyhow::Result<(ExecutorIo, tonic::transport::Channel)> {
    let executor = DuplexStream::Tcp(tokio::net::TcpStream::connect(executor_addr).await?);
    let orchestrator = tokio::net::TcpStream::connect(orchestrator_addr).await?;
    let channel = connect_client(orchestrator, "orchestrator").await?;
    Ok((split_executor(executor), channel))
}
