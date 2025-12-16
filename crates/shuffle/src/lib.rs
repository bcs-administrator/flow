use futures::{StreamExt, TryStreamExt};
use proto_flow::shuffle::{
    QueueRequest, QueueResponse, SessionRequest, SessionResponse, SliceRequest, SliceResponse,
};

mod dialer;
mod queue;
mod session;
mod slice;

pub use dialer::Dialer;
pub use queue::{DefaultQueueHandler, QueueHandler};
pub use session::{DefaultSessionHandler, SessionHandler};
pub use slice::{DefaultSliceHandler, SliceHandler};

/// ShuffleService implements the Shuffle gRPC service, delegating to
/// pluggable handler implementations for Session, Slice, and Queue RPCs.
///
/// The generic parameters allow mocking individual handlers for testing.
/// For example, to test Session logic with a mock Slice handler:
/// ```ignore
/// let service = ShuffleService::new(
///     DefaultSessionHandler,
///     MockSliceHandler::new(),
///     DefaultQueueHandler::new(),
/// );
/// ```
#[derive(Clone)]
pub struct ShuffleService<Sess, Sli, Que> {
    session_handler: Sess,
    slice_handler: Sli,
    queue_handler: Que,
}

impl<Sess, Sli, Que> ShuffleService<Sess, Sli, Que> {
    pub fn new(session_handler: Sess, slice_handler: Sli, queue_handler: Que) -> Self {
        Self {
            session_handler,
            slice_handler,
            queue_handler,
        }
    }
}

impl ShuffleService<DefaultSessionHandler, DefaultSliceHandler, DefaultQueueHandler> {
    /// Create a ShuffleService with default handler implementations.
    pub fn with_defaults() -> Self {
        let dialer = Dialer::new();
        Self::new(
            DefaultSessionHandler::new(dialer.clone()),
            DefaultSliceHandler::new(dialer),
            DefaultQueueHandler::new(),
        )
    }
}

impl<Sess, Sli, Que> ShuffleService<Sess, Sli, Que>
where
    Sess: SessionHandler + Clone,
    Sli: SliceHandler + Clone,
    Que: QueueHandler + Clone,
{
    /// Build a tonic Router containing the Shuffle service.
    pub fn build_tonic_server(self) -> tonic::transport::server::Router {
        tonic::transport::Server::builder().add_service(
            proto_grpc::shuffle::shuffle_server::ShuffleServer::new(self)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
        )
    }
}

#[tonic::async_trait]
impl<Sess, Sli, Que> proto_grpc::shuffle::shuffle_server::Shuffle for ShuffleService<Sess, Sli, Que>
where
    Sess: SessionHandler + Clone,
    Sli: SliceHandler + Clone,
    Que: QueueHandler + Clone,
{
    type SessionStream = futures::stream::BoxStream<'static, tonic::Result<SessionResponse>>;
    type SliceStream = futures::stream::BoxStream<'static, tonic::Result<SliceResponse>>;
    type QueueStream = futures::stream::BoxStream<'static, tonic::Result<QueueResponse>>;

    async fn session(
        &self,
        request: tonic::Request<tonic::Streaming<SessionRequest>>,
    ) -> tonic::Result<tonic::Response<Self::SessionStream>> {
        tracing::debug!(?request, "started session request");

        let request_rx = stream_status_to_error(request.into_inner());
        let response_rx =
            stream_error_to_status(self.session_handler.clone().serve_session(request_rx));

        Ok(tonic::Response::new(response_rx.boxed()))
    }

    async fn slice(
        &self,
        request: tonic::Request<tonic::Streaming<SliceRequest>>,
    ) -> tonic::Result<tonic::Response<Self::SliceStream>> {
        tracing::debug!(?request, "started slice request");

        let request_rx = stream_status_to_error(request.into_inner());
        let response_rx =
            stream_error_to_status(self.slice_handler.clone().serve_slice(request_rx));

        Ok(tonic::Response::new(response_rx.boxed()))
    }

    async fn queue(
        &self,
        request: tonic::Request<tonic::Streaming<QueueRequest>>,
    ) -> tonic::Result<tonic::Response<Self::QueueStream>> {
        tracing::debug!(?request, "started queue request");

        let request_rx = stream_status_to_error(request.into_inner());
        let response_rx =
            stream_error_to_status(self.queue_handler.clone().serve_queue(request_rx));

        Ok(tonic::Response::new(response_rx.boxed()))
    }
}

// Map an anyhow::Error into a tonic::Status.
fn anyhow_to_status(err: anyhow::Error) -> tonic::Status {
    match err.downcast::<tonic::Status>() {
        Ok(status) => status,
        Err(err) => tonic::Status::internal(format!("{err:?}")),
    }
}

// Map a tonic::Status into an anyhow::Error.
fn status_to_anyhow(status: tonic::Status) -> anyhow::Error {
    match status.code() {
        tonic::Code::Internal => anyhow::anyhow!(status.message().to_owned()),
        _ => anyhow::Error::new(status),
    }
}

fn stream_error_to_status<T, S: futures::Stream<Item = anyhow::Result<T>>>(
    s: S,
) -> impl futures::Stream<Item = tonic::Result<T>> {
    s.map_err(anyhow_to_status)
}

fn stream_status_to_error<T, S: futures::Stream<Item = tonic::Result<T>>>(
    s: S,
) -> impl futures::Stream<Item = anyhow::Result<T>> {
    s.map_err(status_to_anyhow)
}
