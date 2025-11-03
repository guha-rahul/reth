use reth_downloaders::snap::{SnapBatch, SnapDownloader};
use reth_network_p2p::snap::client::SnapClient;
use reth_provider::{BlockReader, DBProvider, HeaderProvider};
use reth_stages_api::{
    EntitiesCheckpoint, ExecInput, ExecOutput, Stage, StageCheckpoint, StageError, StageId,
    UnwindInput, UnwindOutput,
};
use std::task::{ready, Context, Poll};
use tracing::*;

/// The stage simply:
/// - Polls the downloader Stream
/// - Writes batches to the database while Downloader does the heavy llifting
#[derive(Debug)]
pub struct SnapSyncStage<D> {
    /// The snap downloader (Stream implementation)
    downloader: Option<D>,
    /// Buffered batch ready to write
    buffer: Option<SnapBatch>,
    /// Total entities processed for progress tracking
    total_accounts_processed: u64,
    total_storage_slots_processed: u64,
    total_bytecodes_processed: u64,
}

impl<D> SnapSyncStage<D> {
    /// Create a new snap sync stage with the given downloader
    pub const fn new(downloader: D) -> Self {
        Self {
            downloader: Some(downloader),
            buffer: None,
            total_accounts_processed: 0,
            total_storage_slots_processed: 0,
            total_bytecodes_processed: 0,
        }
    }
}

impl<Provider, D> Stage<Provider> for SnapSyncStage<D>
where
    Provider: DBProvider + BlockReader + HeaderProvider,
    D: futures_util::Stream<Item = reth_network_p2p::error::DownloadResult<SnapBatch>>
        + Send
        + Sync
        + Unpin
        + 'static,
{
    fn id(&self) -> StageId {
        StageId::SnapSync
    }

    fn poll_execute_ready(
        &mut self,
        cx: &mut Context<'_>,
        input: ExecInput,
    ) -> Poll<Result<(), StageError>> {
        // Case 1: Already have buffered batch
        if self.buffer.is_some() {
            trace!(target: "sync::stages::snap_sync", "Buffer ready for execution");
            return Poll::Ready(Ok(()));
        }

        // Case 2: Target reached
        if input.target_reached() {
            debug!(target: "sync::stages::snap_sync", "Target reached");
            return Poll::Ready(Ok(()));
        }

        // Case 3: Poll the downloader Stream
        let downloader = match self.downloader.as_mut() {
            Some(d) => d,
            None => {
                // Downloader was taken (shouldn't happen in normal operation)
                return Poll::Ready(Err(StageError::Fatal(
                    "Downloader was taken".into(),
                )));
            }
        };

        use futures_util::StreamExt;
        match ready!(downloader.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                debug!(target: "sync::stages::snap_sync",
                    batch_type = ?std::mem::discriminant(&batch),
                    "Received batch from downloader"
                );
                self.buffer = Some(batch);
                Poll::Ready(Ok(()))
            }
            Some(Err(e)) => {
                error!(target: "sync::stages::snap_sync", ?e, "Downloader error");
                Poll::Ready(Err(StageError::Fatal(
                    format!("Downloader error: {:?}", e).into(),
                )))
            }
            None => {
                // Stream ended - snap sync complete
                info!(target: "sync::stages::snap_sync", "Downloader stream ended - snap sync complete");
                self.downloader = None;
                Poll::Ready(Ok(()))
            }
        }
    }

    fn execute(&mut self, _provider: &Provider, input: ExecInput) -> Result<ExecOutput, StageError> {
        // Check if already at target
        if input.target_reached() {
            return Ok(ExecOutput::done(input.checkpoint()));
        }

        // Check if downloader stream ended and no buffered data (complete)
        if self.downloader.is_none() && self.buffer.is_none() {
            info!(target: "sync::stages::snap_sync",
                accounts = self.total_accounts_processed,
                storage_slots = self.total_storage_slots_processed,
                bytecodes = self.total_bytecodes_processed,
                "Snap sync complete"
            );

            let latest_block = input.target();
            return Ok(ExecOutput::done(
                StageCheckpoint::new(latest_block).with_entities_stage_checkpoint(
                    EntitiesCheckpoint {
                        processed: self.total_accounts_processed,
                        total: self.total_accounts_processed,
                    },
                ),
            ));
        }

        // Take the buffered batch
        let batch = self.buffer.take().ok_or_else(|| {
            StageError::Fatal("execute() called before poll_execute_ready finished".into())
        })?;

        let latest_block = input.target();

        // Write batch to database based on type
        match batch {
            SnapBatch::Accounts { accounts, has_more } => {
                debug!(target: "sync::stages::snap_sync",
                    accounts = accounts.len(),
                    has_more,
                    "Writing account batch to database"
                );

                // TODO: Implement database writes
        

                self.total_accounts_processed += accounts.len() as u64;

                Ok(ExecOutput {
                    checkpoint: StageCheckpoint::new(latest_block)
                        .with_entities_stage_checkpoint(EntitiesCheckpoint {
                            processed: self.total_accounts_processed,
                            total: self.total_accounts_processed,
                        }),
                    done: !has_more && self.downloader.is_none(),
                })
            }
            SnapBatch::Storage { storage, accounts_complete, accounts_partial } => {
                let mut total_slots = 0u64;

                debug!(target: "sync::stages::snap_sync",
                    accounts = storage.len(),
                    complete = accounts_complete.len(),
                    partial = accounts_partial.len(),
                    "Writing storage batch to database"
                );

                // TODO: Implement database writes
   
                for (_account_hash, _storage_root, slots) in &storage {
                    total_slots += slots.len() as u64;
                }

                self.total_storage_slots_processed += total_slots;

                Ok(ExecOutput {
                    checkpoint: StageCheckpoint::new(latest_block)
                        .with_entities_stage_checkpoint(EntitiesCheckpoint {
                            processed: self.total_storage_slots_processed,
                            total: self.total_storage_slots_processed,
                        }),
                    done: false,
                })
            }
            SnapBatch::Bytecode { codes } => {
                debug!(target: "sync::stages::snap_sync",
                    bytecodes = codes.len(),
                    "Writing bytecode batch to database"
                );

                // TODO: Implement database writes
             

                self.total_bytecodes_processed += codes.len() as u64;

                Ok(ExecOutput {
                    checkpoint: StageCheckpoint::new(latest_block)
                        .with_entities_stage_checkpoint(EntitiesCheckpoint {
                            processed: self.total_bytecodes_processed,
                            total: self.total_bytecodes_processed,
                        }),
                    done: false,
                })
            }
        }
    }

    fn unwind(
        &mut self,
        _provider: &Provider,
        input: UnwindInput,
    ) -> Result<UnwindOutput, StageError> {
        // Simple unwind implementation (This doesnt delte any data from db)
        // TODO: Clear HashedAccounts, HashedStorages, AccountBytecodes entries if needed

        info!(target: "sync::stages::snap_sync", unwind_to = input.unwind_to, "Unwinding snap sync stage");

        // Reset progress counters
        self.total_accounts_processed = 0;
        self.total_storage_slots_processed = 0;
        self.total_bytecodes_processed = 0;

        Ok(UnwindOutput { checkpoint: StageCheckpoint::new(input.unwind_to) })
    }
}

impl<D> Default for SnapSyncStage<D>
where
    D: Default,
{
    fn default() -> Self {
        Self::new(D::default())
    }
}

/// Helper to create a SnapSyncStage with a SnapDownloader
pub fn create_snap_sync_stage<Client: SnapClient + 'static>(
    client: Client,
    state_root: alloy_primitives::B256,
) -> SnapSyncStage<SnapDownloader<Client>> {
    let downloader = SnapDownloader::builder(client, state_root)
        .with_max_concurrent_requests(10) // 10 parallel requests
        .build();

    SnapSyncStage::new(downloader)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stage_id() {
        use reth_network_p2p::test_utils::TestSnapClient;

        let client = TestSnapClient::default();
        let state_root = alloy_primitives::B256::ZERO;
        let stage = create_snap_sync_stage(client, state_root);

        assert_eq!(stage.id(), StageId::SnapSync);
    }
}
