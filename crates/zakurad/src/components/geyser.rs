//! Adapters from Zakura state and mempool notifications to Geyser plugin events.

use std::{collections::HashSet, sync::Arc};

use color_eyre::eyre::{eyre, Report};
use tokio::{sync::broadcast, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use tracing::{error, info, warn};
use zakura_chain::parameters::Network;
use zakura_geyser_plugin_interface::{
    BestChainChange, BlockEvent, EventKind, MempoolEvent, MempoolEventKind, PluginEvent,
};
use zakura_geyser_plugin_manager::{Config, GeyserPluginManager, PluginPublisher, PluginRegistry};
use zakura_node_services::mempool::{MempoolChange, MempoolChangeKind, MempoolTxSubscriber};
use zakura_state::{
    ChainTipChange, FinalizedBlockNotification, NonFinalizedBlock, ReadRequest, ReadResponse,
    ReadStateService, TipAction,
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
            network,
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
