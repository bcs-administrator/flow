use anyhow::Context;
use futures::TryStreamExt;
use proto_flow::shuffle::{queue_request, queue_response, QueueRequest, QueueResponse};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};

/// QueueJoin coordinates multiple Slice streams connecting to the same Queue.
/// Each Queue member receives connections from all Slices (M connections total).
pub struct QueueJoin {
    /// Expected number of Slice connections.
    member_count: u32,
    /// Sender to notify when all Slices have connected.
    ready_tx: Option<oneshot::Sender<()>>,
    /// Number of Slices that have connected so far.
    connected: u32,
}

impl QueueJoin {
    pub fn new(member_count: u32, ready_tx: oneshot::Sender<()>) -> Self {
        Self {
            member_count,
            ready_tx: Some(ready_tx),
            connected: 0,
        }
    }

    /// Record a new Slice connection. Returns true if all Slices are now connected.
    pub fn add_connection(&mut self) -> bool {
        self.connected += 1;
        if self.connected == self.member_count {
            if let Some(tx) = self.ready_tx.take() {
                let _ = tx.send(());
            }
            true
        } else {
            false
        }
    }
}

/// Trait for handling Queue RPC operations.
pub trait QueueHandler: Send + Sync + 'static {
    /// Handle the Queue RPC stream.
    fn serve_queue(
        self,
        request_rx: impl futures::Stream<Item = anyhow::Result<QueueRequest>>
            + Send
            + Unpin
            + 'static,
    ) -> impl futures::Stream<Item = anyhow::Result<QueueResponse>> + Send + 'static;
}

/// Default implementation of QueueHandler.
#[derive(Clone)]
pub struct DefaultQueueHandler {
    /// Shared state for coordinating Queue RPCs from multiple Slices.
    /// Keyed by (session_id, queue_member_index).
    queue_joins: Arc<Mutex<HashMap<(u64, u32), QueueJoin>>>,
}

impl DefaultQueueHandler {
    pub fn new() -> Self {
        Self {
            queue_joins: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for DefaultQueueHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl QueueHandler for DefaultQueueHandler {
    fn serve_queue(
        self,
        request_rx: impl futures::Stream<Item = anyhow::Result<QueueRequest>>
            + Send
            + Unpin
            + 'static,
    ) -> impl futures::Stream<Item = anyhow::Result<QueueResponse>> + Send + 'static {
        coroutines::try_coroutine(move |co| async move {
            serve_queue(self.queue_joins, request_rx, co).await
        })
    }
}

async fn serve_queue(
    queue_joins: Arc<Mutex<HashMap<(u64, u32), QueueJoin>>>,
    mut request_rx: impl futures::Stream<Item = anyhow::Result<QueueRequest>>
        + Send
        + Unpin
        + 'static,
    mut co: coroutines::Suspend<QueueResponse, ()>,
) -> anyhow::Result<()> {
    // Read the Open request.
    let open = request_rx
        .try_next()
        .await?
        .context("expected Open request")?;

    let queue_request::Open {
        session_id,
        member_count,
        slice_member_index,
        queue_member_index,
    } = open.open.context("first message must be Open")?;

    tracing::info!(
        session_id,
        member_count,
        slice_member_index,
        queue_member_index,
        "queue received Open"
    );

    // Register this connection with the QueueJoin coordinator.
    let ready_rx = {
        let mut joins = queue_joins.lock().await;
        let key = (session_id, queue_member_index);

        let join = joins.entry(key).or_insert_with(|| {
            let (ready_tx, _) = oneshot::channel();
            QueueJoin::new(member_count, ready_tx)
        });

        // For now, we create a new ready channel each time.
        // In a real implementation, we'd coordinate properly.
        let (ready_tx, ready_rx) = oneshot::channel();

        if join.add_connection() {
            // All Slices connected - we're the last one.
            let _ = ready_tx.send(());
        } else {
            // Still waiting for more Slices.
            // In a full implementation, we'd store this and signal later.
            // For now, just proceed since we're running in single-member mode.
            let _ = ready_tx.send(());
        }

        ready_rx
    };

    // Wait for all Slices to connect (in full implementation).
    // For now this returns immediately.
    let _ = ready_rx.await;

    // Send Opened response.
    () = co
        .yield_(QueueResponse {
            opened: Some(queue_response::Opened {}),
            flushed: None,
        })
        .await;

    tracing::info!(
        session_id,
        queue_member_index,
        slice_member_index,
        "queue sent Opened"
    );

    // Main loop: process Enqueue and Flush requests.
    while let Some(request) = request_rx.try_next().await? {
        if let Some(enqueue) = request.enqueue {
            // TODO: Write document to disk queue.
            tracing::trace!(
                journal_tag = enqueue.journal_tag,
                binding = enqueue.binding,
                "queue received Enqueue"
            );
        }

        if let Some(flush) = request.flush {
            // TODO: Ensure all documents are durable on disk.
            tracing::debug!(seq = flush.seq, "queue received Flush, sending Flushed");

            () = co
                .yield_(QueueResponse {
                    opened: None,
                    flushed: Some(queue_response::Flushed { seq: flush.seq }),
                })
                .await;
        }
    }

    tracing::info!(session_id, queue_member_index, "queue stream ended");
    Ok(())
}
