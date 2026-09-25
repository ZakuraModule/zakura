//! Adapters from Zakura state and mempool notifications to Geyser plugin events.

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use color_eyre::eyre::{eyre, Report};
use tokio::{sync::broadcast, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use tracing::{error, info, warn};
use zakura_chain::parameters::Network;
use zakura_geyser_plugin_interface::{
    BestChainChange, BlockEvent, CanonicalBlock, EventKind, MempoolEvent, MempoolEventKind,
    PluginEvent,
};
use zakura_geyser_plugin_manager::{Config, GeyserPluginManager, PluginPublisher, PluginRegistry};
use zakura_node_services::mempool::{MempoolChange, MempoolChangeKind, MempoolTxSubscriber};
use zakura_state::{
    ChainTipChange, FinalizedBlockNotification, NonFinalizedBlock, ReadRequest, ReadResponse,
    ReadStateService, TipAction, MAX_BLOCK_REORG_HEIGHT,
};

/// Running manager and source-adapter tasks owned by `zakurad`.
pub struct GeyserRuntime {
    manager: Option<GeyserPluginManager>,
    adapters: Vec<JoinHandle<()>>,
}

impl GeyserRuntime {
    /// Stops event producers before draining and unloading plugin workers.
    pub async fn shutdown(mut self) {
        for adapter in self.adapters.drain(..) {
            adapter.abort();
            let _ = adapter.await;
        }

        if let Some(manager) = self.manager.take() {
            manager.shutdown().await;
        }
    }
}

impl Drop for GeyserRuntime {
    fn drop(&mut self) {
        for adapter in &self.adapters {
            adapter.abort();
        }
    }
}

/// Starts the configured built-in plugins and only the source adapters they subscribe to.
pub async fn init(
    config: &Config,
    network: Network,
    read_state: ReadStateService,
    chain_tip_change: ChainTipChange,
    mempool_subscriber: MempoolTxSubscriber,
    shutdown: CancellationToken,
) -> Result<GeyserRuntime, Report> {
    let mut registry = PluginRegistry::with_builtin_plugins();
    zakura_grpc_geyser::register(&mut registry);
    let manager = GeyserPluginManager::start(config.clone(), registry)
        .map_err(|error| eyre!("Geyser plugin manager startup failed: {error}"))?;
    let publisher = manager.publisher();
    let mut adapters = Vec::new();
    let publishes_accepted_blocks = manager.has_subscribers(EventKind::BlockAccepted);
    let publishes_finalized_blocks = manager.has_subscribers(EventKind::BlockFinalized);

    if publishes_accepted_blocks {
        let listener = read_state
            .clone()
            .oneshot(ReadRequest::NonFinalizedBlocksListener {
                known_chain_tips: HashSet::new(),
            })
            .await
            .map_err(|error| {
                eyre!("failed to subscribe Geyser to non-finalized blocks: {error}")
            })?;
        let ReadResponse::NonFinalizedBlocksListener(listener) = listener else {
            return Err(eyre!(
                "state returned an unexpected response to the Geyser non-finalized block subscription"
            ));
        };

        adapters.push(tokio::spawn(forward_non_finalized_blocks(
            listener.unwrap(),
            publisher.clone(),
            network.clone(),
            shutdown.clone(),
        )));
    }

    if publishes_finalized_blocks {
        let listener = read_state
            .clone()
            .oneshot(ReadRequest::FinalizedBlocksListener)
            .await
            .map_err(|error| eyre!("failed to subscribe Geyser to finalized blocks: {error}"))?;
        let ReadResponse::FinalizedBlocksListener(listener) = listener else {
            return Err(eyre!(
                "state returned an unexpected response to the Geyser finalized block subscription"
            ));
        };
        adapters.push(tokio::spawn(forward_finalized_blocks(
            listener.into_receiver(),
            publisher.clone(),
            network.clone(),
            shutdown.clone(),
        )));
    }

    if manager.has_subscribers(EventKind::BestChainChanged) {
        adapters.push(tokio::spawn(forward_best_chain_changes(
            chain_tip_change,
            read_state.clone(),
            publisher.clone(),
            shutdown.clone(),
        )));
    }

    if manager.has_subscribers(EventKind::MempoolChanged) {
        adapters.push(tokio::spawn(forward_mempool_changes(
            mempool_subscriber.subscribe(),
            publisher,
            network,
            shutdown,
        )));
    }

    info!(
        adapters = adapters.len(),
        "initialized Geyser plugin event adapters"
    );

    Ok(GeyserRuntime {
        manager: Some(manager),
        adapters,
    })
}

async fn forward_non_finalized_blocks(
    mut blocks: tokio::sync::mpsc::Receiver<NonFinalizedBlock>,
    publisher: PluginPublisher,
    network: Network,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            block = blocks.recv() => {
                let Some(NonFinalizedBlock {
                    hash,
                    block,
                    receipt_order,
                    spent_outputs,
                }) = block else {
                    warn!("Geyser non-finalized block source closed");
                    break;
                };

                let Some(height) = block.coinbase_height() else {
                    error!(?hash, "validated non-finalized block has no coinbase height; skipping Geyser event");
                    metrics::counter!(
                        "plugin.events.rejected.total",
                        "plugin" => "source",
                        "reason" => "missing_block_height"
                    )
                    .increment(1);
                    continue;
                };

                publisher.try_publish(PluginEvent::BlockAccepted(BlockEvent::new(
                    network.clone(),
                    height,
                    hash,
                    block,
                    receipt_order,
                    spent_outputs,
                )));
            }
        }
    }
}

async fn forward_finalized_blocks(
    mut blocks: broadcast::Receiver<FinalizedBlockNotification>,
    publisher: PluginPublisher,
    network: Network,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            result = blocks.recv() => match result {
                Ok(FinalizedBlockNotification { hash, height, block, spent_outputs }) => {
                    publisher.try_publish(PluginEvent::BlockFinalized(BlockEvent::new(
                        network.clone(),
                        height,
                        hash,
                        block,
                        None,
                        spent_outputs,
                    )));
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "Geyser finalized-block listener lagged; events were dropped without delaying state commits");
                    metrics::counter!(
                        "plugin.source.gaps.total",
                        "source" => "finalized_blocks",
                        "reason" => "listener_lagged"
                    )
                    .increment(skipped);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    warn!("Geyser finalized-block source closed");
                    break;
                }
            }
        }
    }
}

async fn forward_best_chain_changes(
    mut changes: ChainTipChange,
    read_state: ReadStateService,
    publisher: PluginPublisher,
    shutdown: CancellationToken,
) {
    let mut canonical_chain = match CanonicalChainWindow::load(read_state.clone()).await {
        Ok(canonical_chain) => canonical_chain,
        Err(error) => {
            warn!(?error, "failed to initialize Geyser canonical-chain window");
            CanonicalChainWindow::default()
        }
    };

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            change = changes.wait_for_tip_change() => {
                let change = match change {
                    Ok(TipAction::Grow { block }) => {
                        canonical_chain.push(CanonicalBlock {
                            height: block.height,
                            hash: block.hash,
                            previous_block_hash: block.previous_block_hash,
                        });
                        BestChainChange::Grow {
                            height: block.height,
                            hash: block.hash,
                            previous_block_hash: block.previous_block_hash,
                            transaction_ids: block.transaction_hashes,
                        }
                    }
                    Ok(TipAction::Reset { height, hash }) => {
                        let transition = canonical_chain
                            .reset(read_state.clone(), height, hash)
                            .await;
                        match transition {
                            Ok(transition) => BestChainChange::Reset {
                                height,
                                hash,
                                disconnected_blocks: transition.disconnected_blocks.into(),
                                connected_blocks: transition.connected_blocks.into(),
                                diff_complete: transition.diff_complete,
                            },
                            Err(error) => {
                                warn!(?error, ?height, ?hash, "failed to resolve canonical-chain reset");
                                metrics::counter!(
                                    "plugin.source.gaps.total",
                                    "source" => "best_chain",
                                    "reason" => "canonical_diff_unavailable"
                                )
                                .increment(1);
                                BestChainChange::Reset {
                                    height,
                                    hash,
                                    disconnected_blocks: Arc::new([]),
                                    connected_blocks: Arc::new([]),
                                    diff_complete: false,
                                }
                            }
                        }
                    }
                    Err(error) => {
                        warn!(?error, "Geyser best-chain source closed");
                        break;
                    }
                };

                publisher.try_publish(PluginEvent::BestChainChanged(change));
            }
        }
    }
}

#[derive(Debug, Default)]
struct CanonicalChainWindow {
    blocks: BTreeMap<zakura_chain::block::Height, CanonicalBlock>,
}

#[derive(Debug, Eq, PartialEq)]
struct CanonicalChainTransition {
    disconnected_blocks: Vec<CanonicalBlock>,
    connected_blocks: Vec<CanonicalBlock>,
    diff_complete: bool,
}

impl CanonicalChainWindow {
    async fn load(read_state: ReadStateService) -> Result<Self, Report> {
        let tip = read_state
            .clone()
            .oneshot(ReadRequest::Tip)
            .await
            .map_err(|error| eyre!("failed to read best-chain tip: {error}"))?;
        let ReadResponse::Tip(Some((tip_height, _))) = tip else {
            return Ok(Self::default());
        };

        Self::load_at(read_state, tip_height).await
    }

    async fn load_at(
        read_state: ReadStateService,
        tip_height: zakura_chain::block::Height,
    ) -> Result<Self, Report> {
        let start_height =
            zakura_chain::block::Height(tip_height.0.saturating_sub(MAX_BLOCK_REORG_HEIGHT));
        let count = tip_height
            .0
            .saturating_sub(start_height.0)
            .saturating_add(1);
        let response = read_state
            .oneshot(ReadRequest::BlocksByHeightRange {
                start: start_height,
                count,
            })
            .await
            .map_err(|error| eyre!("failed to read canonical-chain window: {error}"))?;
        let ReadResponse::Blocks(blocks) = response else {
            return Err(eyre!(
                "state returned an unexpected canonical-chain window response"
            ));
        };

        let blocks = blocks
            .into_iter()
            .map(|(height, block, _)| {
                (
                    height,
                    CanonicalBlock {
                        height,
                        hash: block.hash(),
                        previous_block_hash: block.header.previous_block_hash,
                    },
                )
            })
            .collect();
        Ok(Self { blocks })
    }

    fn push(&mut self, block: CanonicalBlock) {
        self.blocks.insert(block.height, block);
        let minimum_height = block.height.0.saturating_sub(MAX_BLOCK_REORG_HEIGHT);
        self.blocks.retain(|height, _| height.0 >= minimum_height);
    }

    async fn reset(
        &mut self,
        read_state: ReadStateService,
        tip_height: zakura_chain::block::Height,
        tip_hash: zakura_chain::block::Hash,
    ) -> Result<CanonicalChainTransition, Report> {
        let replacement = Self::load_at(read_state, tip_height).await?;
        if replacement.blocks.get(&tip_height).map(|block| block.hash) != Some(tip_hash) {
            return Err(eyre!(
                "canonical-chain window tip does not match the announced reset tip"
            ));
        }

        let transition = canonical_chain_transition(&self.blocks, &replacement.blocks);
        *self = replacement;
        Ok(transition)
    }
}

fn canonical_chain_transition(
    previous: &BTreeMap<zakura_chain::block::Height, CanonicalBlock>,
    current: &BTreeMap<zakura_chain::block::Height, CanonicalBlock>,
) -> CanonicalChainTransition {
    let common_height = previous.iter().rev().find_map(|(height, block)| {
        (current.get(height).map(|current| current.hash) == Some(block.hash)).then_some(*height)
    });

    let implicit_common_parent = common_height.is_none()
        && previous
            .first_key_value()
            .is_some_and(|(previous_height, previous_block)| {
                current
                    .first_key_value()
                    .is_some_and(|(current_height, current_block)| {
                        previous_height == current_height
                            && previous_block.previous_block_hash
                                == current_block.previous_block_hash
                    })
            });
    let diff_complete = common_height.is_some() || implicit_common_parent;
    let after_common = |height: &&zakura_chain::block::Height| {
        common_height.is_none_or(|common_height| **height > common_height)
    };

    let disconnected_blocks = previous
        .iter()
        .filter(|(height, _)| after_common(height))
        .map(|(_, block)| *block)
        .rev()
        .collect();
    let connected_blocks = current
        .iter()
        .filter(|(height, _)| after_common(height))
        .map(|(_, block)| *block)
        .collect();

    CanonicalChainTransition {
        disconnected_blocks,
        connected_blocks,
        diff_complete,
    }
}

async fn forward_mempool_changes(
    mut changes: broadcast::Receiver<MempoolChange>,
    publisher: PluginPublisher,
    network: Network,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            change = changes.recv() => {
                let change = match change {
                    Ok(change) => change,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "Geyser mempool adapter lagged; ephemeral events were skipped");
                        metrics::counter!(
                            "plugin.source.gaps.total",
                            "source" => "mempool",
                            "reason" => "broadcast_lag"
                        )
                        .increment(skipped);
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        warn!("Geyser mempool source closed");
                        break;
                    }
                };

                let (change_kind, transaction_ids, transactions) = change.into_parts();
                let kind = match change_kind {
                    MempoolChangeKind::Added => MempoolEventKind::Added,
                    MempoolChangeKind::Invalidated => MempoolEventKind::Invalidated,
                    MempoolChangeKind::Mined => MempoolEventKind::Mined,
                };
                let transaction_ids: Arc<[_]> = transaction_ids.into_iter().collect();
                publisher.try_publish(PluginEvent::MempoolChanged(MempoolEvent::new(
                    network.clone(),
                    kind,
                    transaction_ids,
                    transactions,
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::block::{Hash, Height};

    fn block(height: u32, hash: u8, previous: u8) -> CanonicalBlock {
        CanonicalBlock {
            height: Height(height),
            hash: Hash([hash; 32]),
            previous_block_hash: Hash([previous; 32]),
        }
    }

    fn chain(blocks: &[CanonicalBlock]) -> BTreeMap<Height, CanonicalBlock> {
        blocks.iter().map(|block| (block.height, *block)).collect()
    }

    #[test]
    fn canonical_reorg_disconnects_tip_first_and_connects_ancestor_first() {
        let common = block(10, 10, 9);
        let old_11 = block(11, 11, 10);
        let old_12 = block(12, 12, 11);
        let new_11 = block(11, 21, 10);
        let new_12 = block(12, 22, 21);

        let transition = canonical_chain_transition(
            &chain(&[common, old_11, old_12]),
            &chain(&[common, new_11, new_12]),
        );

        assert_eq!(transition.disconnected_blocks, vec![old_12, old_11]);
        assert_eq!(transition.connected_blocks, vec![new_11, new_12]);
        assert!(transition.diff_complete);
    }

    #[test]
    fn canonical_reset_recovers_skipped_grows() {
        let common = block(10, 10, 9);
        let new_11 = block(11, 11, 10);
        let new_12 = block(12, 12, 11);

        let transition =
            canonical_chain_transition(&chain(&[common]), &chain(&[common, new_11, new_12]));

        assert!(transition.disconnected_blocks.is_empty());
        assert_eq!(transition.connected_blocks, vec![new_11, new_12]);
        assert!(transition.diff_complete);
    }

    #[test]
    fn canonical_reset_marks_a_missing_common_ancestor_incomplete() {
        let transition =
            canonical_chain_transition(&chain(&[block(20, 20, 19)]), &chain(&[block(21, 31, 30)]));

        assert!(!transition.diff_complete);
    }
}
