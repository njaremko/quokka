//! gRPC transport over a pre-connected duplex stream.
//!
//! Buck2 does not give the runner a TCP address to dial against a fresh
//! connection; it hands us two already-connected socket endpoints (one per
//! service direction) over inherited file descriptors (or, in TCP mode, two
//! sockets it dialed). tonic normally owns connection establishment, so we wrap
//! the existing IO into a one-shot channel (client) and a one-shot incoming
//! stream (server). This mirrors buck2's own `buck2_grpc` crate, which is the
//! contract the installed buck2 binary speaks.

use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::future;
use futures::stream;
use futures::stream::StreamExt as _;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::transport::Uri;
use tonic::transport::server::Connected;
use tonic::transport::server::Router;
use tower::service_fn;

/// A duplex stream assembled from a separate read half and write half.
///
/// Splitting a socket and recombining the halves lets a `tokio::net` stream be
/// used as a tonic server connection (which requires [`Connected`]) without the
/// stream type itself having to implement `Connected`.
pub struct DuplexChannel<R, W> {
    read: R,
    write: W,
}

impl<R, W> DuplexChannel<R, W> {
    pub fn new(read: R, write: W) -> Self {
        Self { read, write }
    }
}

impl<R, W> AsyncRead for DuplexChannel<R, W>
where
    R: AsyncRead + Unpin,
    W: Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.read).poll_read(cx, buf)
    }
}

impl<R, W> AsyncWrite for DuplexChannel<R, W>
where
    R: Unpin,
    W: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.write).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.write).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.write).poll_shutdown(cx)
    }
}

impl<R, W> Connected for DuplexChannel<R, W> {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

/// Build a tonic [`Channel`] over an already-connected duplex stream.
///
/// The URI is only used to populate request `:authority`; no DNS or dialing
/// happens. The connector yields the provided IO exactly once, so the channel
/// cannot reconnect — which is correct, because there is nothing to reconnect
/// to. `name` is woven into the authority for readability in error messages.
pub async fn connect_client<T>(io: T, name: &str) -> anyhow::Result<Channel>
where
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let io = hyper_util::rt::TokioIo::new(io);
    let mut io = Some(io);
    let channel = Endpoint::try_from(format!("http://{name}.invalid"))?
        .connect_with_connector(service_fn(move |_: Uri| {
            let io = io
                .take()
                .ok_or_else(|| io::Error::other("test-runner gRPC channel cannot reconnect"));
            future::ready(io)
        }))
        .await?;
    Ok(channel)
}

/// Serve a tonic [`Router`] over a single pre-connected duplex connection until
/// `shutdown` resolves.
///
/// The incoming stream yields the one connection we hold, then pends forever so
/// the server keeps running its open connection instead of exiting the instant
/// it sees no further incoming connections.
pub async fn serve_connection<I, F>(
    io: I,
    router: Router,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    I: AsyncRead + AsyncWrite + Connected + Send + Unpin + 'static,
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let incoming = stream::once(future::ready(io))
        .chain(stream::once(future::pending()))
        .map(Ok::<_, io::Error>);
    router
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await
}
