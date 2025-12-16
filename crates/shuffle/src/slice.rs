use crate::Dialer;
use anyhow::Context;
use futures::TryStreamExt;
use proto_flow::shuffle::{
    queue_request, slice_request, slice_response, Member, QueueRequest, QueueResponse,
    SliceRequest, SliceResponse,
};

/// Trait for handling Slice RPC operations.
pub trait SliceHandler: Send + Sync + 'static {
    /// Handle the Slice RPC stream.
    fn serve_slice(
        self,
        request_rx: impl futures::Stream<Item = anyhow::Result<SliceRequest>>
            + Send
            + Unpin
            + 'static,
    ) -> impl futures::Stream<Item = anyhow::Result<SliceResponse>> + Send + 'static;
}

/// Default implementation of SliceHandler.
#[derive(Clone)]
pub struct DefaultSliceHandler {
    dialer: Dialer,
}

impl DefaultSliceHandler {
    pub fn new(dialer: Dialer) -> Self {
        Self { dialer }
    }
}

impl SliceHandler for DefaultSliceHandler {
    fn serve_slice(
        self,
        request_rx: impl futures::Stream<Item = anyhow::Result<SliceRequest>>
            + Send
            + Unpin
            + 'static,
    ) -> impl futures::Stream<Item = anyhow::Result<SliceResponse>> + Send + 'static {
        coroutines::try_coroutine(move |co| async move {
            serve_slice(self.dialer, request_rx, co).await
        })
    }
}

async fn serve_slice(
    dialer: Dialer,
    mut request_rx: impl futures::Stream<Item = anyhow::Result<SliceRequest>>
        + Send
        + Unpin
        + 'static,
    mut co: coroutines::Suspend<SliceResponse, ()>,
) -> anyhow::Result<()> {
    // Read the Open request.
    let open = request_rx
        .try_next()
        .await?
        .context("expected Open request")?;

    let slice_request::Open {
        session_id,
        task: _task,
        members,
        member_index,
    } = open.open.context("first message must be Open")?;

    tracing::info!(
        session_id,
        member_index,
        member_count = members.len(),
        "slice received Open"
    );

    // Open Queue RPCs to all members.
    let queue_streams = open_queue_rpcs(&dialer, session_id, member_index, &members).await?;

    tracing::info!(
        session_id,
        member_index,
        queue_count = queue_streams.len(),
        "slice opened all Queue RPCs"
    );

    // Send Opened response to Session.
    () = co
        .yield_(SliceResponse {
            opened: Some(slice_response::Opened {}),
            progress_delta: None,
        })
        .await;

    tracing::info!(session_id, member_index, "slice sent Opened to Session");

    // Main loop: handle JournalTags, StartRead, StopRead from Session.
    while let Some(request) = request_rx.try_next().await? {
        if let Some(journal_tags) = request.journal_tags {
            // TODO: Store journal tag mappings.
            tracing::debug!(
                tag_count = journal_tags.tags.len(),
                "slice received JournalTags"
            );
        }

        if let Some(start_read) = request.start_read {
            // TODO: Start reading from the specified journal.
            tracing::debug!(
                journal_tag = start_read.journal_tag,
                binding = start_read.binding,
                "slice received StartRead"
            );
        }

        if let Some(stop_read) = request.stop_read {
            // TODO: Stop reading from the specified journal.
            tracing::debug!(
                journal_tag = stop_read.journal_tag,
                "slice received StopRead"
            );
        }
    }

    tracing::info!(session_id, member_index, "slice stream ended");
    Ok(())
}

/// Open Queue RPCs to all members and wait for Opened responses.
async fn open_queue_rpcs(
    dialer: &Dialer,
    session_id: u64,
    slice_member_index: u32,
    members: &[Member],
) -> anyhow::Result<Vec<QueueStream>> {
    let member_count = members.len() as u32;

    let futures: Vec<_> = members
        .iter()
        .enumerate()
        .map(|(queue_member_index, member)| {
            let dialer = dialer.clone();
            let address = member.address.clone();
            let queue_member_index = queue_member_index as u32;

            async move {
                tracing::debug!(
                    session_id,
                    slice_member_index,
                    queue_member_index,
                    %address,
                    "opening Queue RPC"
                );

                // Connect to the member's gRPC endpoint.
                let mut client = dialer.dial(&address).await?;

                // Create request stream with Open message.
                let (request_tx, request_rx) = futures::channel::mpsc::channel::<QueueRequest>(16);

                // Send Open request.
                let mut request_tx_clone = request_tx.clone();
                futures::SinkExt::send(
                    &mut request_tx_clone,
                    QueueRequest {
                        open: Some(queue_request::Open {
                            session_id,
                            member_count,
                            slice_member_index,
                            queue_member_index,
                        }),
                        enqueue: None,
                        flush: None,
                    },
                )
                .await
                .context("sending Queue Open")?;

                // Start the Queue RPC.
                let response_stream = client
                    .queue(request_rx)
                    .await
                    .context("starting Queue RPC")?
                    .into_inner();

                // Wait for Opened response.
                let mut response_stream = response_stream;
                let opened = response_stream
                    .try_next()
                    .await
                    .context("waiting for Queue Opened")?
                    .context("Queue closed without Opened")?;

                anyhow::ensure!(
                    opened.opened.is_some(),
                    "expected Opened response from Queue"
                );

                tracing::debug!(
                    session_id,
                    slice_member_index,
                    queue_member_index,
                    "received Opened from Queue"
                );

                Ok(QueueStream {
                    request_tx,
                    response_rx: response_stream,
                })
            }
        })
        .collect();

    futures::future::try_join_all(futures).await
}

/// A connected Queue RPC stream.
#[allow(dead_code)]
pub struct QueueStream {
    pub request_tx: futures::channel::mpsc::Sender<QueueRequest>,
    pub response_rx: tonic::Streaming<QueueResponse>,
}
