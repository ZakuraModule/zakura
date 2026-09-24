//! Versioned event and callback interface for Zakura Geyser plugins.
//!
//! Plugin callbacks are synchronous, but managers must invoke them outside node
//! consensus and state-write tasks. Large payloads are shared through [`Arc`]
//! so fan-out does not clone complete blocks for every plugin.

use std::{fmt::Debug, sync::Arc, time::SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zakura_chain::{
    block::{self, Block},
    transaction::{self, UnminedTxId},
};

/// The plugin callback interface version implemented by this crate.
pub const GEYSER_INTERFACE_VERSION: u32 = 1;

/// The event envelope schema version implemented by this crate.
pub const EVENT_SCHEMA_VERSION: u32 = 1;

/// A unique identifier for one node process event-producing session.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct SessionId(pub u128);

/// A versioned event delivered to a plugin.
#[derive(Clone, Debug)]
pub struct EventEnvelope {
    /// Event schema version.
    pub schema_version: u32,
    /// Node process session that produced this event.
    pub session_id: SessionId,
    /// Monotonically increasing sequence within `session_id`.
    pub sequence: u64,
    /// Local time when the manager observed the event.
    pub observed_at: SystemTime,
    /// Event payload.
    pub payload: PluginEvent,
}

impl EventEnvelope {
    /// Returns the event kind used for routing and bounded metric labels.
    pub fn kind(&self) -> EventKind {
        self.payload.kind()
    }

    /// Returns the attributed number of bytes retained while this event is queued.
    pub fn estimated_size_bytes(&self) -> usize {
        self.payload.estimated_size_bytes()
    }
}

/// Events available to plugins.
#[derive(Clone, Debug)]
pub enum PluginEvent {
    /// A validated block was accepted into non-finalized state.
    BlockAccepted(BlockEvent),
    /// The best chain grew or reset.
    BestChainChanged(BestChainChange),
    /// A block became finalized.
    BlockFinalized(BlockEvent),
    /// The local mempool changed.
    MempoolChanged(MempoolEvent),
}

impl PluginEvent {
    /// Returns the kind used for subscription routing.
    pub const fn kind(&self) -> EventKind {
        match self {
            Self::BlockAccepted(_) => EventKind::BlockAccepted,
            Self::BestChainChanged(_) => EventKind::BestChainChanged,
            Self::BlockFinalized(_) => EventKind::BlockFinalized,
            Self::MempoolChanged(_) => EventKind::MempoolChanged,
        }
    }

    /// Returns the attributed number of bytes retained while this event is queued.
    pub fn estimated_size_bytes(&self) -> usize {
        match self {
            Self::BlockAccepted(block) | Self::BlockFinalized(block) => block.estimated_size_bytes,
            Self::BestChainChanged(change) => change.estimated_size_bytes(),
            Self::MempoolChanged(change) => change.estimated_size_bytes(),
        }
    }
}

/// A validated block and its commit metadata.
#[derive(Clone, Debug)]
pub struct BlockEvent {
    /// Block height.
    pub height: block::Height,
    /// Block hash.
    pub hash: block::Hash,
    /// Complete decoded block, shared across plugin queues.
    pub block: Arc<Block>,
    /// Process-local verifier receipt order, when available.
    pub receipt_order: Option<u64>,
    estimated_size_bytes: usize,
}

impl BlockEvent {
    /// Creates a block event and attributes the retained decoded block memory once.
    pub fn new(
        height: block::Height,
        hash: block::Hash,
        block: Arc<Block>,
        receipt_order: Option<u64>,
    ) -> Self {
        let estimated_size_bytes = usize::try_from(block.attributed_memory_size_bytes())
            .unwrap_or(usize::MAX)
            .saturating_add(std::mem::size_of::<Self>());

        Self {
            height,
            hash,
            block,
            receipt_order,
            estimated_size_bytes,
        }
    }
}

/// A transition of the node's best chain.
#[derive(Clone, Debug)]
pub enum BestChainChange {
    /// The best chain grew by one block.
    Grow {
        /// New best-chain height.
        height: block::Height,
        /// New best-chain hash.
        hash: block::Hash,
        /// Previous best-chain hash.
        previous_block_hash: block::Hash,
        /// Transaction IDs mined in this block.
        transaction_ids: Arc<[transaction::Hash]>,
    },
    /// The best chain reset because of a reorg, skipped notification, or activation boundary.
    Reset {
        /// Best-chain height after the reset.
        height: block::Height,
        /// Best-chain hash after the reset.
        hash: block::Hash,
    },
}

impl BestChainChange {
    fn estimated_size_bytes(&self) -> usize {
        let transaction_bytes = match self {
            Self::Grow {
                transaction_ids, ..
            } => transaction_ids
                .len()
                .saturating_mul(std::mem::size_of::<transaction::Hash>()),
            Self::Reset { .. } => 0,
        };

        std::mem::size_of::<Self>().saturating_add(transaction_bytes)
    }
}

/// A local mempool transition.
#[derive(Clone, Debug)]
pub struct MempoolEvent {
    /// Kind of transition.
    pub kind: MempoolEventKind,
    /// Affected unmined transaction IDs.
    pub transaction_ids: Arc<[UnminedTxId]>,
}

impl MempoolEvent {
    /// Creates a mempool event.
    pub fn new(kind: MempoolEventKind, transaction_ids: Arc<[UnminedTxId]>) -> Self {
        Self {
            kind,
            transaction_ids,
        }
    }

    fn estimated_size_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(
            self.transaction_ids
                .len()
                .saturating_mul(std::mem::size_of::<UnminedTxId>()),
        )
    }
}

/// A mempool transition kind.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum MempoolEventKind {
    /// Transactions entered the verified mempool.
    Added,
    /// Transactions were rejected or invalidated.
    Invalidated,
    /// Transactions were mined and removed.
    Mined,
}

/// A bounded event class used for routing and metric labels.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum EventKind {
    /// A validated non-finalized block.
    BlockAccepted,
    /// A best-chain grow or reset.
    BestChainChanged,
    /// A finalized block.
    BlockFinalized,
    /// A local mempool transition.
    MempoolChanged,
}

impl EventKind {
    /// Returns the stable metric label for this event kind.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BlockAccepted => "block_accepted",
            Self::BestChainChanged => "best_chain_changed",
            Self::BlockFinalized => "block_finalized",
            Self::MempoolChanged => "mempool_changed",
        }
    }
}

/// Event classes requested by a plugin.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct EventSubscriptions {
    /// Receive validated non-finalized blocks.
    pub block_accepted: bool,
    /// Receive best-chain grow and reset transitions.
    pub best_chain: bool,
    /// Receive finalized blocks.
    pub finalized_blocks: bool,
    /// Receive local mempool transitions.
    pub mempool: bool,
}

impl EventSubscriptions {
    /// Subscribe to every event class.
    pub const fn all() -> Self {
        Self {
            block_accepted: true,
            best_chain: true,
            finalized_blocks: true,
            mempool: true,
        }
    }

    /// Returns true when this subscription includes `kind`.
    pub const fn contains(self, kind: EventKind) -> bool {
        match kind {
            EventKind::BlockAccepted => self.block_accepted,
            EventKind::BestChainChanged => self.best_chain,
            EventKind::BlockFinalized => self.finalized_blocks,
            EventKind::MempoolChanged => self.mempool,
        }
    }
}

impl Default for EventSubscriptions {
    fn default() -> Self {
        Self::all()
    }
}

/// An error returned by a plugin lifecycle callback.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{message}")]
pub struct PluginError {
    message: String,
}

impl PluginError {
    /// Creates a plugin error without exposing a plugin-specific error type across the interface.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Result returned by plugin lifecycle callbacks.
pub type PluginResult<T = ()> = Result<T, PluginError>;

/// A synchronous callback interface invoked by one dedicated worker per plugin.
pub trait GeyserPlugin: Debug + Send + 'static {
    /// Returns a stable implementation name for diagnostics.
    fn name(&self) -> &'static str;

    /// Returns the callback interface version implemented by this plugin.
    fn interface_version(&self) -> u32 {
        GEYSER_INTERFACE_VERSION
    }

    /// Returns the event classes requested by this plugin.
    fn subscriptions(&self) -> EventSubscriptions;

    /// Initializes plugin-owned resources before event delivery starts.
    fn on_load(&mut self) -> PluginResult {
        Ok(())
    }

    /// Handles one event on this plugin's dedicated worker.
    fn on_event(&mut self, event: Arc<EventEnvelope>) -> PluginResult;

    /// Releases plugin-owned resources after delivery stops.
    fn on_unload(&mut self) -> PluginResult {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscriptions_route_only_selected_events() {
        let subscriptions = EventSubscriptions {
            block_accepted: false,
            best_chain: true,
            finalized_blocks: false,
            mempool: true,
        };

        assert!(!subscriptions.contains(EventKind::BlockAccepted));
        assert!(subscriptions.contains(EventKind::BestChainChanged));
        assert!(!subscriptions.contains(EventKind::BlockFinalized));
        assert!(subscriptions.contains(EventKind::MempoolChanged));
    }
}
