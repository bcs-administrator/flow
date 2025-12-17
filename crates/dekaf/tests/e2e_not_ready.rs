//! Tests for CollectionStatus::NotReady behavior.
//!
//! These tests verify that Dekaf correctly handles the case where a collection
//! binding exists but journals are not yet available. This happens when:
//! - A collection was recently reset and the writer hasn't created journals yet
//! - The collection exists in the control plane but no data has been written
//!
//! In these cases, Dekaf should return `LeaderNotAvailable` to signal clients
//! to retry, rather than returning confusing errors or fake partition data.

mod e2e;

use e2e::{
    DekafTestEnv,
    raw_kafka::{
        TestKafkaClient, list_offsets_partition_error, metadata_leader_epoch,
    },
};
use kafka_protocol::ResponseError;
use serde_json::json;
use std::time::Duration;

const FIXTURE: &str = include_str!("e2e/fixtures/basic.flow.yaml");

/// Timeout for waiting for Dekaf to see the reset (spec refresh).
const SPEC_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);

/// Test that Metadata returns LeaderNotAvailable for a collection with no journals.
///
/// After a collection reset, there's a window where:
/// 1. The new partition template exists (spec updated)
/// 2. But the journal hasn't been created yet (created lazily on first write)
///
/// During this window, Dekaf should return LeaderNotAvailable for metadata
/// requests, signaling clients to retry.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_metadata_returns_leader_not_available_when_no_journals() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("not_ready_metadata", FIXTURE).await?;
    let info = env.connection_info();

    // Inject initial document so the collection has data and journals exist
    env.inject_documents("data", vec![json!({"id": "1", "value": "initial"})])
        .await?;

    tracing::info!("Connecting raw Kafka client");
    let mut client =
        TestKafkaClient::connect(&info.broker, &info.username, "test-token-12345").await?;

    // Verify metadata works initially
    let metadata = client.metadata(&["test_topic"]).await?;
    let initial_epoch = metadata_leader_epoch(&metadata, "test_topic", 0);
    assert!(
        initial_epoch.is_some(),
        "should have epoch before reset"
    );
    tracing::info!(initial_epoch = ?initial_epoch, "Initial metadata OK");

    // Reset collection: disable capture → reset → re-enable
    // But DON'T inject any documents after reset - this leaves journals uncreated
    tracing::info!("Starting collection reset sequence (without post-reset document injection)");
    env.disable_capture().await?;
    env.reset_collection(None).await?;
    env.enable_capture().await?;

    // Wait for capture to be ready
    let capture = env.capture.as_ref().unwrap();
    env.wait_for_primary(capture).await?;

    // Now we need to wait for Dekaf to pick up the new spec (with new partition template)
    // but NOT have journals yet. This is tricky because:
    // 1. Dekaf refreshes specs periodically (SPEC_TTL)
    // 2. After refresh, it will see new partition template but 0 journals
    //
    // We poll metadata until we see LeaderNotAvailable OR a new epoch with partitions.
    // If journals get created before we can test, that's OK - we'll skip the test.
    tracing::info!("Polling Dekaf for NotReady state (LeaderNotAvailable)");

    let deadline = std::time::Instant::now() + SPEC_REFRESH_TIMEOUT;
    let mut saw_leader_not_available = false;

    while std::time::Instant::now() < deadline {
        let metadata = client.metadata(&["test_topic"]).await?;

        // Check for LeaderNotAvailable error on the topic
        let topic = metadata
            .topics
            .iter()
            .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some("test_topic"));

        if let Some(topic) = topic {
            if topic.error_code == ResponseError::LeaderNotAvailable.code() {
                tracing::info!("Got LeaderNotAvailable from metadata - NotReady state confirmed!");
                saw_leader_not_available = true;
                break;
            }

            // Check if we got a new epoch with partitions (journals were created)
            if let Some(new_epoch) = metadata_leader_epoch(&metadata, "test_topic", 0) {
                if initial_epoch.map_or(true, |e| new_epoch > e) && !topic.partitions.is_empty() {
                    tracing::info!(
                        new_epoch,
                        partitions = topic.partitions.len(),
                        "Journals were created before we could test NotReady state - skipping"
                    );
                    // This is not a failure - just means journals were created quickly
                    return Ok(());
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(
        saw_leader_not_available,
        "Expected to see LeaderNotAvailable during NotReady window"
    );

    // Verify that after injecting a document (creating journals), metadata works again
    tracing::info!("Injecting document to create journals");
    env.inject_documents("data", vec![json!({"id": "2", "value": "post-reset"})])
        .await?;

    // Poll until metadata returns successfully with partitions
    let deadline = std::time::Instant::now() + SPEC_REFRESH_TIMEOUT;
    loop {
        let metadata = client.metadata(&["test_topic"]).await?;
        let topic = metadata
            .topics
            .iter()
            .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some("test_topic"));

        if let Some(topic) = topic {
            if topic.error_code == 0 && !topic.partitions.is_empty() {
                tracing::info!(
                    partitions = topic.partitions.len(),
                    "Metadata returned successfully after journal creation"
                );
                break;
            }
        }

        if std::time::Instant::now() > deadline {
            anyhow::bail!("Timeout waiting for metadata to succeed after journal creation");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Ok(())
}

/// Test that ListOffsets returns LeaderNotAvailable for a collection with no journals.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_list_offsets_returns_leader_not_available_when_no_journals() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("not_ready_list_offsets", FIXTURE).await?;
    let info = env.connection_info();

    // Inject initial document
    env.inject_documents("data", vec![json!({"id": "1", "value": "initial"})])
        .await?;

    tracing::info!("Connecting raw Kafka client");
    let mut client =
        TestKafkaClient::connect(&info.broker, &info.username, "test-token-12345").await?;

    // Verify ListOffsets works initially
    let initial_epoch = {
        let metadata = client.metadata(&["test_topic"]).await?;
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("should have initial epoch")
    };

    let list_resp = client
        .list_offsets_with_epoch("test_topic", 0, -1, initial_epoch)
        .await?;
    let error = list_offsets_partition_error(&list_resp, "test_topic", 0);
    assert!(
        error.map_or(false, |e| e == 0),
        "ListOffsets should succeed before reset"
    );
    tracing::info!("Initial ListOffsets OK");

    // Reset without injecting documents
    tracing::info!("Starting collection reset sequence");
    env.disable_capture().await?;
    env.reset_collection(None).await?;
    env.enable_capture().await?;

    let capture = env.capture.as_ref().unwrap();
    env.wait_for_primary(capture).await?;

    // Poll for LeaderNotAvailable from ListOffsets
    tracing::info!("Polling for NotReady state via ListOffsets");
    let deadline = std::time::Instant::now() + SPEC_REFRESH_TIMEOUT;
    let mut saw_leader_not_available = false;

    while std::time::Instant::now() < deadline {
        // First check metadata to see if we're in NotReady state
        let metadata = client.metadata(&["test_topic"]).await?;
        let topic = metadata
            .topics
            .iter()
            .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some("test_topic"));

        if let Some(topic) = topic {
            // If metadata returns LeaderNotAvailable, ListOffsets should too
            if topic.error_code == ResponseError::LeaderNotAvailable.code() {
                // Try ListOffsets - it should also return LeaderNotAvailable
                // Use epoch -1 (no epoch validation) to isolate NotReady behavior
                let list_resp = client
                    .list_offsets_with_epoch("test_topic", 0, -1, -1)
                    .await?;
                let error = list_offsets_partition_error(&list_resp, "test_topic", 0);

                if error == Some(ResponseError::LeaderNotAvailable.code()) {
                    tracing::info!("Got LeaderNotAvailable from ListOffsets - NotReady confirmed!");
                    saw_leader_not_available = true;
                    break;
                }
            }

            // Check if journals were created (new epoch with partitions)
            if let Some(new_epoch) = metadata_leader_epoch(&metadata, "test_topic", 0) {
                if new_epoch > initial_epoch && !topic.partitions.is_empty() {
                    tracing::info!(
                        new_epoch,
                        "Journals created before NotReady test - skipping"
                    );
                    return Ok(());
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(
        saw_leader_not_available,
        "Expected to see LeaderNotAvailable from ListOffsets during NotReady window"
    );

    Ok(())
}

/// Test that Fetch returns LeaderNotAvailable for a collection with no journals.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_fetch_returns_leader_not_available_when_no_journals() -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("not_ready_fetch", FIXTURE).await?;
    let info = env.connection_info();

    // Inject initial document
    env.inject_documents("data", vec![json!({"id": "1", "value": "initial"})])
        .await?;

    tracing::info!("Connecting raw Kafka client");
    let mut client =
        TestKafkaClient::connect(&info.broker, &info.username, "test-token-12345").await?;

    // Get initial epoch
    let initial_epoch = {
        let metadata = client.metadata(&["test_topic"]).await?;
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("should have initial epoch")
    };
    tracing::info!(initial_epoch, "Got initial epoch");

    // Verify fetch works initially
    let fetch_resp = client
        .fetch_with_epoch("test_topic", 0, 0, initial_epoch)
        .await?;
    let error = e2e::raw_kafka::fetch_partition_error(&fetch_resp, "test_topic", 0);
    assert!(
        error.map_or(false, |e| e == 0),
        "Fetch should succeed before reset"
    );
    tracing::info!("Initial Fetch OK");

    // Reset without injecting documents
    tracing::info!("Starting collection reset sequence");
    env.disable_capture().await?;
    env.reset_collection(None).await?;
    env.enable_capture().await?;

    let capture = env.capture.as_ref().unwrap();
    env.wait_for_primary(capture).await?;

    // Poll for LeaderNotAvailable from Fetch
    tracing::info!("Polling for NotReady state via Fetch");
    let deadline = std::time::Instant::now() + SPEC_REFRESH_TIMEOUT;
    let mut saw_leader_not_available = false;

    while std::time::Instant::now() < deadline {
        // Check metadata first
        let metadata = client.metadata(&["test_topic"]).await?;
        let topic = metadata
            .topics
            .iter()
            .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some("test_topic"));

        if let Some(topic) = topic {
            if topic.error_code == ResponseError::LeaderNotAvailable.code() {
                // Metadata shows NotReady - try Fetch with epoch -1 to isolate NotReady behavior
                let fetch_resp = client
                    .fetch_with_epoch("test_topic", 0, 0, -1)
                    .await?;
                let error = e2e::raw_kafka::fetch_partition_error(&fetch_resp, "test_topic", 0);

                if error == Some(ResponseError::LeaderNotAvailable.code()) {
                    tracing::info!("Got LeaderNotAvailable from Fetch - NotReady confirmed!");
                    saw_leader_not_available = true;
                    break;
                }
            }

            // Check if journals were created
            if let Some(new_epoch) = metadata_leader_epoch(&metadata, "test_topic", 0) {
                if new_epoch > initial_epoch && !topic.partitions.is_empty() {
                    tracing::info!(
                        new_epoch,
                        "Journals created before NotReady test - skipping"
                    );
                    return Ok(());
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(
        saw_leader_not_available,
        "Expected to see LeaderNotAvailable from Fetch during NotReady window"
    );

    Ok(())
}

/// Test that OffsetForLeaderEpoch returns LeaderNotAvailable for a collection with no journals.
#[ignore] // Requires local stack
#[tokio::test]
async fn test_offset_for_leader_epoch_returns_leader_not_available_when_no_journals(
) -> anyhow::Result<()> {
    e2e::init_tracing();

    let env = DekafTestEnv::setup("not_ready_offset_epoch", FIXTURE).await?;
    let info = env.connection_info();

    // Inject initial document
    env.inject_documents("data", vec![json!({"id": "1", "value": "initial"})])
        .await?;

    tracing::info!("Connecting raw Kafka client");
    let mut client =
        TestKafkaClient::connect(&info.broker, &info.username, "test-token-12345").await?;

    // Get initial epoch
    let initial_epoch = {
        let metadata = client.metadata(&["test_topic"]).await?;
        metadata_leader_epoch(&metadata, "test_topic", 0).expect("should have initial epoch")
    };
    tracing::info!(initial_epoch, "Got initial epoch");

    // Verify OffsetForLeaderEpoch works initially
    let resp = client
        .offset_for_leader_epoch("test_topic", 0, initial_epoch)
        .await?;
    let result = e2e::raw_kafka::offset_for_epoch_result(&resp, "test_topic", 0);
    assert!(
        result.map_or(false, |r| r.error_code == 0),
        "OffsetForLeaderEpoch should succeed before reset"
    );
    tracing::info!("Initial OffsetForLeaderEpoch OK");

    // Reset without injecting documents
    tracing::info!("Starting collection reset sequence");
    env.disable_capture().await?;
    env.reset_collection(None).await?;
    env.enable_capture().await?;

    let capture = env.capture.as_ref().unwrap();
    env.wait_for_primary(capture).await?;

    // Poll for LeaderNotAvailable
    tracing::info!("Polling for NotReady state via OffsetForLeaderEpoch");
    let deadline = std::time::Instant::now() + SPEC_REFRESH_TIMEOUT;
    let mut saw_leader_not_available = false;

    while std::time::Instant::now() < deadline {
        // Check metadata first
        let metadata = client.metadata(&["test_topic"]).await?;
        let topic = metadata
            .topics
            .iter()
            .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some("test_topic"));

        if let Some(topic) = topic {
            if topic.error_code == ResponseError::LeaderNotAvailable.code() {
                // Try OffsetForLeaderEpoch - use epoch 1 (a valid epoch)
                let resp = client
                    .offset_for_leader_epoch("test_topic", 0, 1)
                    .await?;
                let result = e2e::raw_kafka::offset_for_epoch_result(&resp, "test_topic", 0);

                if let Some(r) = result {
                    if r.error_code == ResponseError::LeaderNotAvailable.code() {
                        tracing::info!(
                            "Got LeaderNotAvailable from OffsetForLeaderEpoch - NotReady confirmed!"
                        );
                        saw_leader_not_available = true;
                        break;
                    }
                }
            }

            // Check if journals were created
            if let Some(new_epoch) = metadata_leader_epoch(&metadata, "test_topic", 0) {
                if new_epoch > initial_epoch && !topic.partitions.is_empty() {
                    tracing::info!(
                        new_epoch,
                        "Journals created before NotReady test - skipping"
                    );
                    return Ok(());
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(
        saw_leader_not_available,
        "Expected to see LeaderNotAvailable from OffsetForLeaderEpoch during NotReady window"
    );

    Ok(())
}
