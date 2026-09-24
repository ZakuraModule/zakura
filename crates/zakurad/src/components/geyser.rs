//! Adapters from Zakura state and mempool notifications to Geyser plugin events.

use std::{collections::HashSet, sync::Arc};

use color_eyre::eyre::{eyre, Report};
use tokio::{sync::broadcast, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use tracing::{error, info, warn};
use zakura_geyser_plugin_interface::{
    BestChainChange, BlockEvent, EventKind, MempoolEvent, MempoolEventKind, PluginEvent,
};
use zakura_geyser_plugin_manager::{Config, GeyserPluginManager, PluginPublisher, PluginRegistry};
use zakura_node_services::mempool::{MempoolChange, MempoolChangeKind, MempoolTxSubscriber};
use zakura_state::{
    ChainTipChange, NonFinalizedBlock, ReadRequest, ReadResponse, ReadStateService, TipAction,
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
    read_state: ReadStateService,
    chain_tip_change: ChainTipChange,
    mempool_subscriber: MempoolTxSubscriber,
    shutdown: CancellationToken,
) -> Result<GeyserRuntime, Report> {
    let manager =
        GeyserPluginManager::start(config.clone(), PluginRegistry::with_builtin_plugins())
            .map_err(|error| eyre!("Geyser plugin manager startup failed: {error}"))?;
    let publisher = manager.publisher();
    let mut adapters = Vec::new();

    if manager.has_subscribers(EventKind::BlockAccepted) {
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
            shutdown.clone(),
        )));
    }

    if manager.has_subscribers(EventKind::BlockFinalized) {
        let finalized_tip = finalized_tip(read_state.clone()).await?;
        adapters.push(tokio::spawn(forward_finalized_blocks(
            read_state,
            chain_tip_change.clone_for_task(),
            finalized_tip,
            publisher.clone(),
            shutdown.clone(),
        )));
    }

    if manager.has_subscribers(EventKind::BestChainChanged) {
        adapters.push(tokio::spawn(forward_best_chain_changes(
            chain_tip_change,
            publisher.clone(),
            shutdown.clone(),
        )));
    }

    if manager.has_subscribers(EventKind::MempoolChanged) {
        adapters.push(tokio::spawn(forward_mempool_changes(
            mempool_subscriber.subscribe(),
            publisher,
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
                    height,
                    hash,
                    block,
                    receipt_order,
                )));
            }
        }
    }
}

async fn finalized_tip(
    read_state: ReadStateService,
) -> Result<Option<(zakura_chain::block::Height, zakura_chain::block::Hash)>, Report> {
    match read_state.oneshot(ReadRequest::FinalizedTip).await {
        Ok(ReadResponse::FinalizedTip(tip)) => Ok(tip),
        Ok(_) => Err(eyre!(
            "state returned an unexpected response to the Geyser finalized tip query"
        )),
        Err(error) => Err(eyre!("failed to query Geyser finalized tip: {error}")),
    }
}

async fn forward_finalized_blocks(
    read_state: ReadStateService,
    mut changes: ChainTipChange,
    mut last_finalized_tip: Option<(zakura_chain::block::Height, zakura_chain::block::Hash)>,
    publisher: PluginPublisher,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            change = changes.wait_for_tip_change() => {
                if let Err(error) = change {
                    warn!(?error, "Geyser finalized-block trigger source closed");
                    break;
                }
            }
        }

        let new_finalized_tip = match finalized_tip(read_state.clone()).await {
            Ok(tip) => tip,
            Err(error) => {
                error!(
                    ?error,
                    "Geyser finalized tip query failed; stopping finalized events"
                );
                metrics::counter!(
                    "plugin.source.gaps.total",
                    "source" => "finalized_blocks",
                    "reason" => "tip_query_failed"
                )
                .increment(1);
                break;
            }
        };

        let Some((new_height, _new_hash)) = new_finalized_tip else {
            continue;
        };
        let mut next_height = match last_finalized_tip {
            Some((height, _hash)) => match height.next() {
                Ok(height) => height,
                Err(_) => break,
            },
            None => zakura_chain::block::Height::MIN,
        };

        if last_finalized_tip.is_some_and(|(height, _hash)| new_height < height) {
            error!(
                ?last_finalized_tip,
                ?new_finalized_tip,
                "finalized tip moved backwards; stopping Geyser finalized events"
            );
            metrics::counter!(
                "plugin.source.gaps.total",
                "source" => "finalized_blocks",
                "reason" => "tip_moved_backwards"
            )
            .increment(1);
            break;
        }

        while next_height <= new_height {
            let response = read_state
                .clone()
                .oneshot(ReadRequest::Block(next_height.into()))
                .await;
            let block = match response {
                Ok(ReadResponse::Block(Some(block))) => block,
                Ok(ReadResponse::Block(None)) => {
                    error!(
                        ?next_height,
                        "newly finalized block is unavailable; stopping Geyser finalized events"
                    );
                    metrics::counter!(
                        "plugin.source.gaps.total",
                        "source" => "finalized_blocks",
                        "reason" => "block_unavailable"
                    )
                    .increment(1);
                    return;
                }
                Ok(_) => {
                    error!(
                        ?next_height,
                        "state returned an unexpected finalized block response"
                    );
                    return;
                }
                Err(error) => {
                    error!(?error, ?next_height, "failed to read newly finalized block");
                    metrics::counter!(
                        "plugin.source.gaps.total",
                        "source" => "finalized_blocks",
                        "reason" => "block_query_failed"
                    )
                    .increment(1);
                    return;
                }
            };
            let hash = block.hash();
            publisher.try_publish(PluginEvent::BlockFinalized(BlockEvent::new(
                next_height,
                hash,
                block,
                None,
            )));

            let following_height = match next_height.next() {
                Ok(height) => height,
                Err(_) => break,
            };
            next_height = following_height;
        }

        last_finalized_tip = new_finalized_tip;
    }
}

async fn forward_best_chain_changes(
    mut changes: ChainTipChange,
    publisher: PluginPublisher,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            change = changes.wait_for_tip_change() => {
                let change = match change {
                    Ok(TipAction::Grow { block }) => BestChainChange::Grow {
                        height: block.height,
                        hash: block.hash,
                        previous_block_hash: block.previous_block_hash,
                        transaction_ids: block.transaction_hashes,
                    },
                    Ok(TipAction::Reset { height, hash }) => {
                        BestChainChange::Reset { height, hash }
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

async fn forward_mempool_changes(
    mut changes: broadcast::Receiver<MempoolChange>,
    publisher: PluginPublisher,
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

                let kind = match change.kind() {
                    MempoolChangeKind::Added => MempoolEventKind::Added,
                    MempoolChangeKind::Invalidated => MempoolEventKind::Invalidated,
                    MempoolChangeKind::Mined => MempoolEventKind::Mined,
                };
                let transaction_ids: Arc<[_]> = change.into_tx_ids().into_iter().collect();
                publisher.try_publish(PluginEvent::MempoolChanged(MempoolEvent::new(
                    kind,
                    transaction_ids,
                )));
            }
        }
    }
}
