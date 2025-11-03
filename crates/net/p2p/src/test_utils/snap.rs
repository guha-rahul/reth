//! Testing support for snap sync related interfaces.

use crate::{
    download::DownloadClient,
    error::PeerRequestResult,
    priority::Priority,
    snap::client::{SnapClient, SnapResponse},
};
use reth_eth_wire_types::snap::{
    AccountRangeMessage, ByteCodesMessage, GetAccountRangeMessage, GetByteCodesMessage,
    GetStorageRangesMessage, GetTrieNodesMessage, StorageRangesMessage, TrieNodesMessage,
};
use reth_network_peers::{PeerId, WithPeerId};
use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::Mutex;

type SnapFut = std::pin::Pin<
    Box<dyn std::future::Future<Output = PeerRequestResult<SnapResponse>> + Send + Sync>,
>;

/// A test client for snap sync requests
#[derive(Debug, Default, Clone)]
pub struct TestSnapClient {
    /// Mock account range responses
    account_responses: Arc<Mutex<Vec<AccountRangeMessage>>>,
    /// Mock storage range responses
    storage_responses: Arc<Mutex<Vec<StorageRangesMessage>>>,
    /// Mock bytecode responses
    bytecode_responses: Arc<Mutex<Vec<ByteCodesMessage>>>,
    /// Mock trie node responses
    trie_node_responses: Arc<Mutex<Vec<TrieNodesMessage>>>,
    /// Request counter
    request_attempts: Arc<AtomicU64>,
}

impl TestSnapClient {
    /// Return the number of times client was polled
    pub fn request_attempts(&self) -> u64 {
        self.request_attempts.load(Ordering::SeqCst)
    }

    /// Adds account range responses to the set
    pub async fn extend_account_ranges(
        &self,
        responses: impl IntoIterator<Item = AccountRangeMessage>,
    ) {
        let mut lock = self.account_responses.lock().await;
        lock.extend(responses);
    }

    /// Adds storage range responses to the set
    pub async fn extend_storage_ranges(
        &self,
        responses: impl IntoIterator<Item = StorageRangesMessage>,
    ) {
        let mut lock = self.storage_responses.lock().await;
        lock.extend(responses);
    }

    /// Adds bytecode responses to the set
    pub async fn extend_bytecodes(&self, responses: impl IntoIterator<Item = ByteCodesMessage>) {
        let mut lock = self.bytecode_responses.lock().await;
        lock.extend(responses);
    }

    /// Adds trie node responses to the set
    pub async fn extend_trie_nodes(&self, responses: impl IntoIterator<Item = TrieNodesMessage>) {
        let mut lock = self.trie_node_responses.lock().await;
        lock.extend(responses);
    }

    /// Clears all responses
    pub async fn clear(&self) {
        self.account_responses.lock().await.clear();
        self.storage_responses.lock().await.clear();
        self.bytecode_responses.lock().await.clear();
        self.trie_node_responses.lock().await.clear();
    }
}

impl DownloadClient for TestSnapClient {
    fn report_bad_message(&self, _peer_id: PeerId) {
        // noop
    }

    fn num_connected_peers(&self) -> usize {
        10 // Pretend we have connected peers
    }
}

impl SnapClient for TestSnapClient {
    type Output = SnapFut;

    fn get_storage_ranges(&self, request: GetStorageRangesMessage) -> Self::Output {
        self.get_storage_ranges_with_priority(request, Priority::Normal)
    }

    fn get_byte_codes(&self, request: GetByteCodesMessage) -> Self::Output {
        self.get_byte_codes_with_priority(request, Priority::Normal)
    }

    fn get_trie_nodes(&self, request: GetTrieNodesMessage) -> Self::Output {
        self.get_trie_nodes_with_priority(request, Priority::Normal)
    }

    fn get_account_range_with_priority(
        &self,
        request: GetAccountRangeMessage,
        _priority: Priority,
    ) -> Self::Output {
        let responses = Arc::clone(&self.account_responses);
        self.request_attempts.fetch_add(1, Ordering::SeqCst);

        Box::pin(async move {
            let mut lock = responses.lock().await;
            let response = if !lock.is_empty() {
                lock.remove(0) // Take first response (FIFO order)
            } else {
                // Return empty response if no mock data
                AccountRangeMessage {
                    request_id: request.request_id,
                    accounts: vec![],
                    proof: vec![],
                }
            };

            Ok(WithPeerId::from((PeerId::default(), SnapResponse::AccountRange(response))))
        })
    }

    fn get_storage_ranges_with_priority(
        &self,
        request: GetStorageRangesMessage,
        _priority: Priority,
    ) -> Self::Output {
        let responses = Arc::clone(&self.storage_responses);
        self.request_attempts.fetch_add(1, Ordering::SeqCst);

        Box::pin(async move {
            let mut lock = responses.lock().await;
            let response = if !lock.is_empty() {
                lock.remove(0)
            } else {
                StorageRangesMessage {
                    request_id: request.request_id,
                    slots: vec![],
                    proof: vec![],
                }
            };

            Ok(WithPeerId::from((PeerId::default(), SnapResponse::StorageRanges(response))))
        })
    }

    fn get_byte_codes_with_priority(
        &self,
        request: GetByteCodesMessage,
        _priority: Priority,
    ) -> Self::Output {
        let responses = Arc::clone(&self.bytecode_responses);
        self.request_attempts.fetch_add(1, Ordering::SeqCst);

        Box::pin(async move {
            let mut lock = responses.lock().await;
            let response = if !lock.is_empty() {
                lock.remove(0)
            } else {
                ByteCodesMessage { request_id: request.request_id, codes: vec![] }
            };

            Ok(WithPeerId::from((PeerId::default(), SnapResponse::ByteCodes(response))))
        })
    }

    fn get_trie_nodes_with_priority(
        &self,
        request: GetTrieNodesMessage,
        _priority: Priority,
    ) -> Self::Output {
        let responses = Arc::clone(&self.trie_node_responses);
        self.request_attempts.fetch_add(1, Ordering::SeqCst);

        Box::pin(async move {
            let mut lock = responses.lock().await;
            let response = if !lock.is_empty() {
                lock.remove(0)
            } else {
                TrieNodesMessage { request_id: request.request_id, nodes: vec![] }
            };

            Ok(WithPeerId::from((PeerId::default(), SnapResponse::TrieNodes(response))))
        })
    }
}
