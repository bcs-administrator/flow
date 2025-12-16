use anyhow::Context;
use proto_grpc::shuffle::shuffle_client::ShuffleClient;
use std::sync::Arc;
use tonic::transport::Channel;

/// Dialer manages gRPC channel connections to shuffle service endpoints.
///
/// This provides a central place for:
/// - Common dialing configuration (timeouts, TLS, etc.)
/// - Channel reuse for already-established connections
///
/// Currently a trivial implementation that creates new connections each time,
/// but structured to allow future optimization.
#[derive(Clone)]
pub struct Dialer {
    inner: Arc<DialerInner>,
}

struct DialerInner {
    // Future: connection cache, configuration, etc.
}

impl Dialer {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DialerInner {}),
        }
    }

    /// Dial a shuffle service endpoint and return a client.
    pub async fn dial(&self, address: &str) -> anyhow::Result<ShuffleClient<Channel>> {
        // Future: check cache, apply configuration, etc.
        let _ = &self.inner; // Silence unused warning for now.

        ShuffleClient::connect(address.to_string())
            .await
            .with_context(|| format!("connecting to {address}"))
    }
}

impl Default for Dialer {
    fn default() -> Self {
        Self::new()
    }
}
