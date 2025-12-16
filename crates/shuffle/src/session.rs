use crate::Dialer;
use anyhow::Context;
use futures::TryStreamExt;
use proto_flow::shuffle::{
    session_request, session_response, slice_request, Member, SessionRequest, SessionResponse,
    SliceRequest, SliceResponse, Task,
};

/// Trait for handling Session RPC operations.
pub trait SessionHandler: Send + Sync + 'static {
    /// Handle the Session RPC stream.
    fn serve_session(
        self,
        request_rx: impl futures::Stream<Item = anyhow::Result<SessionRequest>>
            + Send
            + Unpin
            + 'static,
    ) -> impl futures::Stream<Item = anyhow::Result<SessionResponse>> + Send + 'static;
}

/// Default implementation of SessionHandler.
#[derive(Clone)]
pub struct DefaultSessionHandler {
    dialer: Dialer,
}

impl DefaultSessionHandler {
    pub fn new(dialer: Dialer) -> Self {
        Self { dialer }
    }
}

impl SessionHandler for DefaultSessionHandler {
    fn serve_session(
        self,
        request_rx: impl futures::Stream<Item = anyhow::Result<SessionRequest>>
            + Send
            + Unpin
            + 'static,
    ) -> impl futures::Stream<Item = anyhow::Result<SessionResponse>> + Send + 'static {
        coroutines::try_coroutine(move |co| async move {
            serve_session(self.dialer, request_rx, co).await
        })
    }
}

async fn serve_session(
    dialer: Dialer,
    mut request_rx: impl futures::Stream<Item = anyhow::Result<SessionRequest>>
        + Send
        + Unpin
        + 'static,
    mut co: coroutines::Suspend<SessionResponse, ()>,
) -> anyhow::Result<()> {
    // Read the Open request.
    let open = request_rx
        .try_next()
        .await?
        .context("expected Open request")?;

    let session_request::Open {
        session_id,
        task,
        members,
        resume_tags,
        last_commit,
        read_through,
    } = open.open.context("first message must be Open")?;

    let task = task.context("Open must include task")?;

    tracing::info!(
        session_id,
        member_count = members.len(),
        resume_tag_count = resume_tags.len(),
        last_commit_count = last_commit.len(),
        read_through_count = read_through.len(),
        "session received Open"
    );

    // Open Slice RPCs to all members.
    let slice_streams = open_slice_rpcs(&dialer, session_id, &task, &members).await?;

    tracing::info!(
        session_id,
        slice_count = slice_streams.len(),
        "session opened all Slice RPCs, sending Opened"
    );

    // Send Opened response to client.
    () = co
        .yield_(SessionResponse {
            opened: Some(session_response::Opened {}),
            next_checkpoint: None,
        })
        .await;

    tracing::info!(session_id, "session sent Opened to client");

    // TODO: Start journal watch, broadcast JournalTags, send StartRead/StopRead.

    // Main loop: handle NextCheckpoint requests from client.
    while let Some(request) = request_rx.try_next().await? {
        if request.next_checkpoint.is_some() {
            // TODO: Aggregate progress deltas and return checkpoint.
            tracing::debug!("session received NextCheckpoint request");

            // For now, return an empty checkpoint delta.
            () = co
                .yield_(SessionResponse {
                    opened: None,
                    next_checkpoint: Some(session_response::NextCheckpoint {
                        delta_checkpoint: Vec::new(),
                    }),
                })
                .await;
        }
    }

    tracing::info!(session_id, "session stream ended");
    Ok(())
}

/// Open Slice RPCs to all members and wait for Opened responses.
async fn open_slice_rpcs(
    dialer: &Dialer,
    session_id: u64,
    task: &Task,
    members: &[Member],
) -> anyhow::Result<Vec<SliceStream>> {
    let futures: Vec<_> = members
        .iter()
        .enumerate()
        .map(|(member_index, member)| {
            let dialer = dialer.clone();
            let task = task.clone();
            let members = members.to_vec();
            let address = member.address.clone();

            async move {
                tracing::debug!(session_id, member_index, %address, "opening Slice RPC");

                // Connect to the member's gRPC endpoint.
                let mut client = dialer.dial(&address).await?;

                // Create request stream with Open message.
                let (request_tx, request_rx) = futures::channel::mpsc::channel::<SliceRequest>(16);

                // Send Open request.
                let mut request_tx_clone = request_tx.clone();
                futures::SinkExt::send(
                    &mut request_tx_clone,
                    SliceRequest {
                        open: Some(slice_request::Open {
                            session_id,
                            task: Some(task),
                            members,
                            member_index: member_index as u32,
                        }),
                        journal_tags: None,
                        start_read: None,
                        stop_read: None,
                    },
                )
                .await
                .context("sending Slice Open")?;

                // Start the Slice RPC.
                let response_stream = client
                    .slice(request_rx)
                    .await
                    .context("starting Slice RPC")?
                    .into_inner();

                // Wait for Opened response.
                let mut response_stream = response_stream;
                let opened = response_stream
                    .try_next()
                    .await
                    .context("waiting for Slice Opened")?
                    .context("Slice closed without Opened")?;

                anyhow::ensure!(
                    opened.opened.is_some(),
                    "expected Opened response from Slice"
                );

                tracing::debug!(session_id, member_index, "received Opened from Slice");

                Ok(SliceStream {
                    request_tx,
                    response_rx: response_stream,
                })
            }
        })
        .collect();

    futures::future::try_join_all(futures).await
}

/// A connected Slice RPC stream.
#[allow(dead_code)]
pub struct SliceStream {
    pub request_tx: futures::channel::mpsc::Sender<SliceRequest>,
    pub response_rx: tonic::Streaming<SliceResponse>,
}
