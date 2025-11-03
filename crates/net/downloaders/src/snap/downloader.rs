//! Snap sync downloader that implements Stream for parallel state downloading.

use alloy_primitives::{Bytes, B256, U256};
use futures::stream::{FuturesUnordered, Stream};
use futures::StreamExt;
use reth_eth_wire_types::snap::{
    AccountRangeMessage, ByteCodesMessage, GetAccountRangeMessage, GetByteCodesMessage,
    GetStorageRangesMessage, StorageRangesMessage,
};
use reth_network_p2p::{
    error::{DownloadError, DownloadResult, RequestError},
    snap::client::{SnapClient, SnapResponse},
};
use reth_primitives_traits::Account;
use reth_trie_common::{verify_account_range_proof, verify_storage_range_proof};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tracing::*;

/// Maximum response size for snap requests (512KB)
const MAX_RESPONSE_BYTES: u64 = 512 * 1024;

/// Maximum accounts to request storage for in one batch
const STORAGE_ACCOUNTS_PER_BATCH: usize = 128;

/// Maximum bytecode hashes per request
const BYTECODE_CHUNK_SIZE: usize = 50_000;

/// Default maximum concurrent requests
const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 10;

/// Helper to create a bad response error
fn bad_response_error(msg: impl Into<String>) -> DownloadError {
    DownloadError::RequestError(RequestError::BadResponse)
}

/// Sync phase for the snap downloader
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapPhase {
    /// Downloading account ranges
    AccountSync,
    /// Downloading storage for accounts with storage
    StorageSync,
    /// Downloading bytecode for accounts with code
    BytecodeSync,
    /// All phases complete
    Complete,
}

/// A batch of snap sync data ready to be written to the database
#[derive(Debug)]
pub enum SnapBatch {
    /// Batch of accounts with their hashes and storage roots
    Accounts {
        /// Account data: (account_hash, account, storage_root)
        accounts: Vec<(B256, Account, B256)>,
        /// Whether there are more accounts to download in this range
        has_more: bool,
    },
    /// Batch of storage slots for accounts
    Storage {
        /// Storage data: (account_hash, storage_root, [(slot_hash, slot_value)])
        storage: Vec<(B256, B256, Vec<(B256, U256)>)>,
        /// Accounts with complete storage downloaded
        accounts_complete: HashSet<B256>,
        /// Accounts with partial storage (account_hash → last_slot_hash)
        accounts_partial: HashMap<B256, B256>,
    },
    /// Batch of bytecode
    Bytecode {
        /// Bytecode data: (code_hash, bytecode)
        codes: Vec<(B256, Bytes)>,
    },
}

/// Snap sync downloader that downloads account state in parallel.
///
/// This downloader implements [`Stream`] and manages:
/// - Multiple concurrent requests (default 10)
/// - Proof verification
/// - Multi-phase sync (Account → Storage → Bytecode)
/// - Batch formation and yielding
#[must_use = "Stream does nothing unless polled"]
pub struct SnapDownloader<Client: SnapClient> {
    /// The snap sync client
    client: Arc<Client>,

    /// Maximum number of concurrent requests
    max_concurrent_requests: usize,

    /// Current sync phase
    phase: SnapPhase,

    /// State root for the sync
    state_root: B256,

    // ===== Account Sync State =====
    /// Next account hash to download from
    next_account_hash: B256,

    /// Target account hash (usually 0xff..ff)
    target_account_hash: B256,

    /// In-flight account range requests
    in_flight_account_requests: FuturesUnordered<
        Pin<Box<dyn std::future::Future<Output = DownloadResult<(u64, SnapResponse)>> + Send>>,
    >,

    /// Buffered verified account ranges ready to yield
    buffered_account_ranges: VecDeque<AccountRangeData>,

    /// Mapping of account hash to storage root (collected during account sync)
    account_storage_roots: HashMap<B256, B256>,

    /// Code hashes that need downloading (collected during account sync)
    code_hashes_needed: HashSet<B256>,

    // ===== Storage Sync State =====
    /// Accounts pending storage download
    accounts_pending_storage: VecDeque<B256>,

    /// In-flight storage range requests
    in_flight_storage_requests: FuturesUnordered<
        Pin<Box<dyn std::future::Future<Output = DownloadResult<(Vec<B256>, SnapResponse)>> + Send>>,
    >,

    /// Buffered verified storage ranges ready to yield
    buffered_storage_ranges: VecDeque<StorageRangeData>,

    /// Accounts with partial storage (account_hash → last_slot_hash)
    accounts_partial_storage: HashMap<B256, B256>,

    // ===== Bytecode Sync State =====
    /// Code hashes pending download
    code_hashes_pending: VecDeque<B256>,

    /// In-flight bytecode requests
    in_flight_bytecode_requests: FuturesUnordered<
        Pin<Box<dyn std::future::Future<Output = DownloadResult<(Vec<B256>, SnapResponse)>> + Send>>,
    >,

    /// Buffered verified bytecodes ready to yield
    buffered_bytecodes: VecDeque<ByteCodeData>,

    /// Request ID counter
    request_id: u64,

    /// Whether the downloader has been terminated
    terminated: bool,
}

/// Verified account range data
#[derive(Debug)]
struct AccountRangeData {
    /// Account data: (hash, account, storage_root)
    accounts: Vec<(B256, Account, B256)>,
    /// Whether there are more accounts after this range
    has_more: bool,
}

/// Verified storage range data
#[derive(Debug)]
struct StorageRangeData {
    /// Account hash this storage belongs to
    account_hash: B256,
    /// Storage root for verification
    storage_root: B256,
    /// Storage slots: (slot_hash, slot_value)
    slots: Vec<(B256, U256)>,
    /// Whether this account's storage is complete
    is_complete: bool,
}

/// Verified bytecode data
#[derive(Debug)]
struct ByteCodeData {
    /// Code hash
    hash: B256,
    /// Bytecode
    code: Bytes,
}

impl<Client: SnapClient + 'static> SnapDownloader<Client> {
    /// Create a new snap downloader builder
    pub fn builder(client: Client, state_root: B256) -> SnapDownloaderBuilder<Client> {
        SnapDownloaderBuilder {
            client,
            state_root,
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
            start_hash: B256::ZERO,
            target_hash: B256::from([0xff; 32]),
        }
    }

    /// Get the next request ID
    fn next_request_id(&mut self) -> u64 {
        let id = self.request_id;
        self.request_id = self.request_id.wrapping_add(1);
        id
    }

    /// Check if we can submit a new request based on concurrency limit
    fn can_submit_new_request(&self) -> bool {
        match self.phase {
            SnapPhase::AccountSync => {
                self.in_flight_account_requests.len() < self.max_concurrent_requests
            }
            SnapPhase::StorageSync => {
                self.in_flight_storage_requests.len() < self.max_concurrent_requests
            }
            SnapPhase::BytecodeSync => {
                self.in_flight_bytecode_requests.len() < self.max_concurrent_requests
            }
            SnapPhase::Complete => false,
        }
    }

    /// Check if the downloader is terminated
    fn is_terminated(&self) -> bool {
        self.terminated ||
            (self.phase == SnapPhase::Complete &&
                self.in_flight_account_requests.is_empty() &&
                self.in_flight_storage_requests.is_empty() &&
                self.in_flight_bytecode_requests.is_empty() &&
                self.buffered_account_ranges.is_empty() &&
                self.buffered_storage_ranges.is_empty() &&
                self.buffered_bytecodes.is_empty())
    }

    /// Submit a new account range request
    fn submit_account_range_request(&mut self) {
        let request_id = self.next_request_id();
        let request = GetAccountRangeMessage {
            request_id,
            root_hash: self.state_root,
            starting_hash: self.next_account_hash,
            limit_hash: self.target_account_hash,
            response_bytes: MAX_RESPONSE_BYTES,
        };

        debug!(target: "downloaders::snap",
            request_id,
            ?self.next_account_hash,
            "Submitting account range request"
        );

        let client = Arc::clone(&self.client);
        let fut = async move {
            let response = client.get_account_range(request).await?;
            Ok((request_id, response.into_data()))
        };

        self.in_flight_account_requests.push(Box::pin(fut));
    }

    /// Submit a new storage ranges request
    fn submit_storage_ranges_request(&mut self) {
        if self.accounts_pending_storage.is_empty() {
            return;
        }

        let request_id = self.next_request_id();
        let batch_size =
            std::cmp::min(STORAGE_ACCOUNTS_PER_BATCH, self.accounts_pending_storage.len());

        let account_batch: Vec<B256> =
            self.accounts_pending_storage.drain(..batch_size).collect();

        let request = GetStorageRangesMessage {
            request_id,
            root_hash: self.state_root,
            account_hashes: account_batch.clone(),
            starting_hash: B256::ZERO,
            limit_hash: B256::from([0xff; 32]),
            response_bytes: MAX_RESPONSE_BYTES,
        };

        debug!(target: "downloaders::snap",
            request_id,
            accounts = account_batch.len(),
            "Submitting storage ranges request"
        );

        let client = Arc::clone(&self.client);
        let fut = async move {
            let response = client.get_storage_ranges(request).await?;
            Ok((account_batch, response.into_data()))
        };

        self.in_flight_storage_requests.push(Box::pin(fut));
    }

    /// Submit a new bytecode request
    fn submit_bytecode_request(&mut self) {
        if self.code_hashes_pending.is_empty() {
            return;
        }

        let request_id = self.next_request_id();
        let batch_size = std::cmp::min(BYTECODE_CHUNK_SIZE, self.code_hashes_pending.len());

        let code_batch: Vec<B256> = self.code_hashes_pending.drain(..batch_size).collect();

        let request = GetByteCodesMessage {
            request_id,
            hashes: code_batch.clone(),
            response_bytes: MAX_RESPONSE_BYTES * 10, // Larger for bytecode
        };

        debug!(target: "downloaders::snap",
            request_id,
            code_hashes = code_batch.len(),
            "Submitting bytecode request"
        );

        let client = Arc::clone(&self.client);
        let fut = async move {
            let response = client.get_byte_codes(request).await?;
            Ok((code_batch, response.into_data()))
        };

        self.in_flight_bytecode_requests.push(Box::pin(fut));
    }

    /// Process an account range response
    fn process_account_range_response(
        &mut self,
        _request_id: u64,
        response: SnapResponse,
    ) -> DownloadResult<()> {
        let account_range = match response {
            SnapResponse::AccountRange(msg) => msg,
            _ => {
                return Err(bad_response_error("Expected AccountRange response"))
            }
        };

        if account_range.accounts.is_empty() {
            warn!(target: "downloaders::snap", "Received empty account range");
            // Empty response means no more data available in this range
            // Update next_account_hash to target to signal completion
            self.next_account_hash = self.target_account_hash;
            return Ok(());
        }

        // Decode accounts and collect metadata
        let mut accounts = Vec::new();
        let mut account_hashes = Vec::new();
        let mut account_values = Vec::new();

        for acc in &account_range.accounts {
            let account_hash = acc.hash;
            let slim_account = self.decode_slim_account(&acc.body)?;

            // Track storage roots
            if slim_account.storage_root != alloy_consensus::EMPTY_ROOT_HASH {
                self.account_storage_roots.insert(account_hash, slim_account.storage_root);
            }

            // Track code hashes
            if let Some(code_hash) = slim_account.bytecode_hash {
                self.code_hashes_needed.insert(code_hash);
            }

            let account = Account {
                nonce: slim_account.nonce,
                balance: slim_account.balance,
                bytecode_hash: slim_account.bytecode_hash,
            };

            account_hashes.push(account_hash);
            account_values.push(account.clone());
            accounts.push((account_hash, account, slim_account.storage_root));
        }

        // Verify range proof
        let proof_result = verify_account_range_proof(
            self.state_root,
            self.next_account_hash,
            &account_hashes,
            &account_values,
            &account_range.proof,
        )
        .map_err(|e| bad_response_error("Account proof verification failed"))?;

        if !proof_result.valid {
            return Err(bad_response_error("Invalid account range proof"));
        }

        // Update next hash
        if let Some((last_hash, _, _)) = accounts.last() {
            self.next_account_hash = *last_hash;
        }

        // Buffer the verified data
        self.buffered_account_ranges.push_back(AccountRangeData {
            accounts,
            has_more: proof_result.has_more,
        });

        debug!(target: "downloaders::snap",
            buffered = self.buffered_account_ranges.len(),
            has_more = proof_result.has_more,
            "Buffered account range"
        );

        Ok(())
    }

    /// Process a storage ranges response
    fn process_storage_ranges_response(
        &mut self,
        account_batch: Vec<B256>,
        response: SnapResponse,
    ) -> DownloadResult<()> {
        let storage_ranges = match response {
            SnapResponse::StorageRanges(msg) => msg,
            _ => {
                return Err(bad_response_error("Expected StorageRanges response"))
            }
        };

        for (account_idx, account_slots) in storage_ranges.slots.iter().enumerate() {
            if account_idx >= account_batch.len() {
                break;
            }

            let account_hash = account_batch[account_idx];
            let storage_root = match self.account_storage_roots.get(&account_hash) {
                Some(root) => *root,
                None => continue,
            };

            // Decode storage values
            let slot_hashes: Vec<B256> = account_slots.iter().map(|s| s.hash).collect();
            let slot_values: Vec<U256> = account_slots
                .iter()
                .map(|s| self.decode_storage_value(&s.data))
                .collect::<Result<Vec<_>, _>>()?;

            // Verify proof for last account if proof present
            let is_last_account = account_idx == storage_ranges.slots.len() - 1;
            let mut is_complete = true;

            if is_last_account && !storage_ranges.proof.is_empty() && !slot_hashes.is_empty() {
                let proof_result = verify_storage_range_proof(
                    storage_root,
                    B256::ZERO,
                    &slot_hashes,
                    &slot_values,
                    &storage_ranges.proof,
                )
                .map_err(|e| bad_response_error("Storage proof verification failed"))?;

                if !proof_result.valid {
                    return Err(bad_response_error("Invalid storage proof"));
                }

                is_complete = !proof_result.has_more;
            }

            // Buffer the verified storage data
            self.buffered_storage_ranges.push_back(StorageRangeData {
                account_hash,
                storage_root,
                slots: slot_hashes.into_iter().zip(slot_values).collect(),
                is_complete,
            });
        }

        debug!(target: "downloaders::snap",
            buffered = self.buffered_storage_ranges.len(),
            "Buffered storage ranges"
        );

        Ok(())
    }

    /// Process a bytecode response
    fn process_bytecode_response(
        &mut self,
        code_batch: Vec<B256>,
        response: SnapResponse,
    ) -> DownloadResult<()> {
        let bytecodes = match response {
            SnapResponse::ByteCodes(msg) => msg,
            _ => {
                return Err(bad_response_error("Expected ByteCodes response"))
            }
        };

        for (hash, code) in code_batch.iter().zip(&bytecodes.codes) {
            // Verify hash
            use alloy_primitives::keccak256;
            if keccak256(code) != *hash {
                return Err(bad_response_error("Code hash mismatch"));
            }

            self.buffered_bytecodes.push_back(ByteCodeData { hash: *hash, code: code.clone() });
        }

        debug!(target: "downloaders::snap",
            buffered = self.buffered_bytecodes.len(),
            "Buffered bytecodes"
        );

        Ok(())
    }

    /// Try to form a batch from buffered data
    fn try_form_batch(&mut self) -> Option<SnapBatch> {
        match self.phase {
            SnapPhase::AccountSync => {
                if let Some(range_data) = self.buffered_account_ranges.pop_front() {
                    let has_more = range_data.has_more;
                    return Some(SnapBatch::Accounts { accounts: range_data.accounts, has_more });
                }
            }
            SnapPhase::StorageSync => {
                if !self.buffered_storage_ranges.is_empty() {
                    // Collect storage data for multiple accounts
                    let mut storage = Vec::new();
                    let mut accounts_complete = HashSet::new();
                    let mut accounts_partial = HashMap::new();

                    while let Some(storage_data) = self.buffered_storage_ranges.pop_front() {
                        if storage_data.is_complete {
                            accounts_complete.insert(storage_data.account_hash);
                        } else if let Some((last_slot, _)) = storage_data.slots.last() {
                            accounts_partial.insert(storage_data.account_hash, *last_slot);
                        }

                        storage.push((
                            storage_data.account_hash,
                            storage_data.storage_root,
                            storage_data.slots,
                        ));

                        // Limit batch size
                        if storage.len() >= 100 {
                            break;
                        }
                    }

                    if !storage.is_empty() {
                        return Some(SnapBatch::Storage {
                            storage,
                            accounts_complete,
                            accounts_partial,
                        });
                    }
                }
            }
            SnapPhase::BytecodeSync => {
                if !self.buffered_bytecodes.is_empty() {
                    let mut codes = Vec::new();

                    while let Some(code_data) = self.buffered_bytecodes.pop_front() {
                        codes.push((code_data.hash, code_data.code));

                        // Limit batch size
                        if codes.len() >= 1000 {
                            break;
                        }
                    }

                    if !codes.is_empty() {
                        return Some(SnapBatch::Bytecode { codes });
                    }
                }
            }
            SnapPhase::Complete => {}
        }

        None
    }

    /// Decode a slim account from snap protocol wire format
    fn decode_slim_account(&self, slim_rlp: &Bytes) -> DownloadResult<SlimAccount> {
        use alloy_rlp::Decodable;

        let mut decoder = &slim_rlp[..];
        let list_header = alloy_rlp::Header::decode(&mut decoder)
            .map_err(|e| bad_response_error("Failed to decode slim account header"))?;

        if !list_header.list {
            return Err(bad_response_error("Slim account is not an RLP list"));
        }

        let nonce = u64::decode(&mut decoder)
            .map_err(|e| bad_response_error("Failed to decode nonce"))?;

        let balance = U256::decode(&mut decoder)
            .map_err(|e| bad_response_error("Failed to decode balance"))?;

        let storage_root_bytes = Bytes::decode(&mut decoder)
            .map_err(|e| bad_response_error("Failed to decode storage_root"))?;

        let storage_root = if storage_root_bytes.is_empty() {
            alloy_consensus::EMPTY_ROOT_HASH
        } else if storage_root_bytes.len() == 32 {
            B256::from_slice(&storage_root_bytes)
        } else {
            return Err(bad_response_error("Invalid storage_root length"));
        };

        let code_hash_bytes = Bytes::decode(&mut decoder)
            .map_err(|e| bad_response_error("Failed to decode code_hash"))?;

        let bytecode_hash = if code_hash_bytes.is_empty() {
            None
        } else if code_hash_bytes.len() == 32 {
            Some(B256::from_slice(&code_hash_bytes))
        } else {
            return Err(bad_response_error("Invalid code_hash length"));
        };

        Ok(SlimAccount { nonce, balance, storage_root, bytecode_hash })
    }

    /// Decode a storage value from snap protocol wire format
    fn decode_storage_value(&self, data: &Bytes) -> DownloadResult<U256> {
        use alloy_rlp::Decodable;
        U256::decode(&mut &data[..])
            .map_err(|e| bad_response_error("Failed to decode storage value"))
    }

    /// Transition to the next phase
    fn transition_phase(&mut self) {
        match self.phase {
            SnapPhase::AccountSync => {
                // Transition to storage sync if there are accounts with storage
                if !self.account_storage_roots.is_empty() {
                    self.accounts_pending_storage =
                        self.account_storage_roots.keys().copied().collect();
                    self.phase = SnapPhase::StorageSync;
                    info!(target: "downloaders::snap",
                        accounts_with_storage = self.accounts_pending_storage.len(),
                        "Transitioning to StorageSync phase"
                    );
                } else if !self.code_hashes_needed.is_empty() {
                    self.code_hashes_pending = self.code_hashes_needed.iter().copied().collect();
                    self.phase = SnapPhase::BytecodeSync;
                    info!(target: "downloaders::snap",
                        code_hashes = self.code_hashes_pending.len(),
                        "Transitioning to BytecodeSync phase"
                    );
                } else {
                    self.phase = SnapPhase::Complete;
                    info!(target: "downloaders::snap", "Snap sync complete");
                }
            }
            SnapPhase::StorageSync => {
                if !self.code_hashes_needed.is_empty() {
                    self.code_hashes_pending = self.code_hashes_needed.iter().copied().collect();
                    self.phase = SnapPhase::BytecodeSync;
                    info!(target: "downloaders::snap",
                        code_hashes = self.code_hashes_pending.len(),
                        "Transitioning to BytecodeSync phase"
                    );
                } else {
                    self.phase = SnapPhase::Complete;
                    info!(target: "downloaders::snap", "Snap sync complete");
                }
            }
            SnapPhase::BytecodeSync => {
                self.phase = SnapPhase::Complete;
                info!(target: "downloaders::snap", "Snap sync complete");
            }
            SnapPhase::Complete => {}
        }
    }
}

/// Slim account from snap protocol
#[derive(Debug, Clone)]
struct SlimAccount {
    nonce: u64,
    balance: U256,
    storage_root: B256,
    bytecode_hash: Option<B256>,
}

impl<Client: SnapClient + 'static> Stream for SnapDownloader<Client> {
    type Item = DownloadResult<SnapBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.is_terminated() {
            return Poll::Ready(None);
        }

        loop {
            // Poll in-flight requests based on current phase
            match this.phase {
                SnapPhase::AccountSync => {
                    while let Poll::Ready(Some(response)) =
                        this.in_flight_account_requests.poll_next_unpin(cx)
                    {
                        match response {
                            Ok((request_id, snap_response)) => {
                                if let Err(e) =
                                    this.process_account_range_response(request_id, snap_response)
                                {
                                    error!(target: "downloaders::snap", ?e, "Account range processing failed");
                                    this.terminated = true;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            }
                            Err(e) => {
                                error!(target: "downloaders::snap", ?e, "Account range request failed");
                                this.terminated = true;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }

                    // Submit new account requests
                    while this.can_submit_new_request() &&
                        this.next_account_hash < this.target_account_hash
                    {
                        this.submit_account_range_request();
                    }

                    // Check if account sync phase is complete
                    if this.next_account_hash >= this.target_account_hash &&
                        this.in_flight_account_requests.is_empty() &&
                        this.buffered_account_ranges.is_empty()
                    {
                        this.transition_phase();
                        continue;
                    }
                }
                SnapPhase::StorageSync => {
                    while let Poll::Ready(Some(response)) =
                        this.in_flight_storage_requests.poll_next_unpin(cx)
                    {
                        match response {
                            Ok((account_batch, snap_response)) => {
                                if let Err(e) = this.process_storage_ranges_response(
                                    account_batch,
                                    snap_response,
                                ) {
                                    error!(target: "downloaders::snap", ?e, "Storage range processing failed");
                                    this.terminated = true;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            }
                            Err(e) => {
                                error!(target: "downloaders::snap", ?e, "Storage range request failed");
                                this.terminated = true;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }

                    // Submit new storage requests
                    while this.can_submit_new_request() && !this.accounts_pending_storage.is_empty()
                    {
                        this.submit_storage_ranges_request();
                    }

                    // Check if storage sync phase is complete
                    if this.accounts_pending_storage.is_empty() &&
                        this.in_flight_storage_requests.is_empty() &&
                        this.buffered_storage_ranges.is_empty()
                    {
                        this.transition_phase();
                        continue;
                    }
                }
                SnapPhase::BytecodeSync => {
                    while let Poll::Ready(Some(response)) =
                        this.in_flight_bytecode_requests.poll_next_unpin(cx)
                    {
                        match response {
                            Ok((code_batch, snap_response)) => {
                                if let Err(e) =
                                    this.process_bytecode_response(code_batch, snap_response)
                                {
                                    error!(target: "downloaders::snap", ?e, "Bytecode processing failed");
                                    this.terminated = true;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            }
                            Err(e) => {
                                error!(target: "downloaders::snap", ?e, "Bytecode request failed");
                                this.terminated = true;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }

                    // Submit new bytecode requests
                    while this.can_submit_new_request() && !this.code_hashes_pending.is_empty() {
                        this.submit_bytecode_request();
                    }

                    // Check if bytecode sync phase is complete
                    if this.code_hashes_pending.is_empty() &&
                        this.in_flight_bytecode_requests.is_empty() &&
                        this.buffered_bytecodes.is_empty()
                    {
                        this.transition_phase();
                        continue;
                    }
                }
                SnapPhase::Complete => {
                    return Poll::Ready(None);
                }
            }

            // Try to yield a batch
            if let Some(batch) = this.try_form_batch() {
                return Poll::Ready(Some(Ok(batch)));
            }

            // No batch ready yet, need to wait for more responses
            if this.in_flight_account_requests.is_empty() &&
                this.in_flight_storage_requests.is_empty() &&
                this.in_flight_bytecode_requests.is_empty()
            {
                // No in-flight requests and no buffered data - shouldn't happen
                warn!(target: "downloaders::snap", "No in-flight requests but no batch ready");
                return Poll::Pending;
            }

            return Poll::Pending;
        }
    }
}

/// Builder for [`SnapDownloader`]
pub struct SnapDownloaderBuilder<Client: SnapClient> {
    client: Client,
    state_root: B256,
    max_concurrent_requests: usize,
    start_hash: B256,
    target_hash: B256,
}

impl<Client: SnapClient + 'static> SnapDownloaderBuilder<Client> {
    /// Set the maximum number of concurrent requests
    pub const fn with_max_concurrent_requests(mut self, max: usize) -> Self {
        self.max_concurrent_requests = max;
        self
    }

    /// Set the starting hash for account sync
    pub const fn with_start_hash(mut self, hash: B256) -> Self {
        self.start_hash = hash;
        self
    }

    /// Set the target hash for account sync
    pub const fn with_target_hash(mut self, hash: B256) -> Self {
        self.target_hash = hash;
        self
    }

    /// Build the downloader
    pub fn build(self) -> SnapDownloader<Client> {
        SnapDownloader {
            client: Arc::new(self.client),
            max_concurrent_requests: self.max_concurrent_requests,
            phase: SnapPhase::AccountSync,
            state_root: self.state_root,
            next_account_hash: self.start_hash,
            target_account_hash: self.target_hash,
            in_flight_account_requests: FuturesUnordered::new(),
            buffered_account_ranges: VecDeque::new(),
            account_storage_roots: HashMap::new(),
            code_hashes_needed: HashSet::new(),
            accounts_pending_storage: VecDeque::new(),
            in_flight_storage_requests: FuturesUnordered::new(),
            buffered_storage_ranges: VecDeque::new(),
            accounts_partial_storage: HashMap::new(),
            code_hashes_pending: VecDeque::new(),
            in_flight_bytecode_requests: FuturesUnordered::new(),
            buffered_bytecodes: VecDeque::new(),
            request_id: 0,
            terminated: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_network_p2p::test_utils::TestSnapClient;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_downloader_initialization() {
        let client = TestSnapClient::default();
        let state_root = B256::random();

        let downloader = SnapDownloader::builder(client, state_root)
            .with_max_concurrent_requests(10)
            .build();

        assert_eq!(downloader.max_concurrent_requests, 10);
        assert_eq!(downloader.phase, SnapPhase::AccountSync);
        assert_eq!(downloader.state_root, state_root);
        assert!(!downloader.terminated);
    }

    #[tokio::test]
    async fn test_request_submission() {
        let client = Arc::new(TestSnapClient::default());
        let state_root = B256::random();

        let downloader =
            SnapDownloader::builder((*client).clone(), state_root).with_max_concurrent_requests(1).build();

        // Before polling, no requests should be made
        assert_eq!(client.request_attempts(), 0);

        // Drop downloader to avoid hanging
        drop(downloader);
    }

    #[tokio::test]
    async fn test_concurrent_limit() {
        let client = Arc::new(TestSnapClient::default());
        let state_root = B256::random();

        let max_concurrent = 5;
        let mut downloader = SnapDownloader::builder((*client).clone(), state_root)
            .with_max_concurrent_requests(max_concurrent)
            .build();

        // Manually trigger request submission
        for _ in 0..max_concurrent {
            if downloader.can_submit_new_request() {
                downloader.submit_account_range_request();
            }
        }

        // Should have submitted max_concurrent requests
        assert_eq!(downloader.in_flight_account_requests.len(), max_concurrent);

        // Can't submit more
        assert!(downloader.can_submit_new_request() == false ||
                downloader.in_flight_account_requests.len() == max_concurrent);
    }

    #[tokio::test]
    async fn test_builder_configuration() {
        let client = TestSnapClient::default();
        let state_root = B256::random();
        let start_hash = B256::random();
        let target_hash = B256::random();

        let downloader = SnapDownloader::builder(client, state_root)
            .with_max_concurrent_requests(15)
            .with_start_hash(start_hash)
            .with_target_hash(target_hash)
            .build();

        assert_eq!(downloader.max_concurrent_requests, 15);
        assert_eq!(downloader.next_account_hash, start_hash);
        assert_eq!(downloader.target_account_hash, target_hash);
        assert_eq!(downloader.state_root, state_root);
    }

    #[tokio::test]
    async fn test_empty_response_no_infinite_loop() {
        use reth_eth_wire_types::snap::AccountRangeMessage;

        let client = Arc::new(TestSnapClient::default());
        let state_root = B256::random();

        // Add empty account range response
        client.extend_account_ranges(vec![AccountRangeMessage {
            request_id: 0,
            accounts: vec![],
            proof: vec![],
        }]).await;

        let mut downloader = SnapDownloader::builder((*client).clone(), state_root)
            .with_max_concurrent_requests(1)
            .build();

        // Should terminate without hanging
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            downloader.next()
        ).await;

        // Should complete (not timeout)
        assert!(result.is_ok(), "Downloader should not hang on empty response");

        // After processing empty response, next_account_hash should equal target
        assert_eq!(downloader.next_account_hash, downloader.target_account_hash);

        // Phase should transition to Complete (no storage, no bytecode needed)
        assert_eq!(downloader.phase, SnapPhase::Complete);

        // Verify only 1 request was made (not infinite)
        assert_eq!(client.request_attempts(), 1);
    }

}
