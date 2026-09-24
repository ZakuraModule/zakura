//! Geyser plugin manager configuration.

use std::{collections::HashSet, time::Duration};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zakura_geyser_plugin_interface::EventSubscriptions;

/// Default total configured memory ceiling for enabled plugin queues.
pub const DEFAULT_TOTAL_QUEUE_BYTES: usize = 512 * 1024 * 1024;

/// Default number of events retained by one plugin queue.
pub const DEFAULT_QUEUE_CAPACITY_EVENTS: usize = 4_096;

/// Default attributed memory ceiling for one plugin queue.
pub const DEFAULT_QUEUE_CAPACITY_BYTES: usize = 64 * 1024 * 1024;

/// Geyser plugin manager configuration.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Enables the plugin manager and event adapters.
    pub enabled: bool,
    /// Maximum time to drain plugin workers during shutdown.
    #[serde(with = "humantime_serde")]
    pub shutdown_timeout: Duration,
    /// Sum ceiling for all enabled per-plugin queue byte capacities.
    pub total_queue_bytes: usize,
    /// Built-in plugin descriptors, in startup order.
    pub plugins: Vec<PluginConfig>,
}

impl Config {
    /// Validates bounded queue sizes, names, and aggregate memory configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.shutdown_timeout.is_zero() {
            return Err(ConfigError::ZeroShutdownTimeout);
        }

        if self.total_queue_bytes == 0 {
            return Err(ConfigError::ZeroTotalQueueBytes);
        }

        let mut names = HashSet::new();
        let mut enabled_queue_bytes = 0usize;

        for plugin in &self.plugins {
            plugin.validate()?;

            if !names.insert(plugin.name.clone()) {
                return Err(ConfigError::DuplicatePluginName(plugin.name.clone()));
            }

            if plugin.enabled {
                enabled_queue_bytes = enabled_queue_bytes
                    .checked_add(plugin.queue.capacity_bytes)
                    .ok_or(ConfigError::QueueCapacityOverflow)?;
            }
        }

        if enabled_queue_bytes > self.total_queue_bytes {
            return Err(ConfigError::TotalQueueBytesExceeded {
                configured: enabled_queue_bytes,
                limit: self.total_queue_bytes,
            });
        }

        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            shutdown_timeout: Duration::from_secs(30),
            total_queue_bytes: DEFAULT_TOTAL_QUEUE_BYTES,
            plugins: Vec::new(),
        }
    }
}

/// One built-in plugin instance.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct PluginConfig {
    /// Unique ASCII name used in logs and bounded metric labels.
    pub name: String,
    /// Enables this descriptor.
    pub enabled: bool,
    /// Makes initialization failure abort node startup.
    pub required_at_startup: bool,
    /// Registered built-in implementation kind.
    pub kind: String,
    /// Event classes routed to this plugin.
    pub subscriptions: EventSubscriptions,
    /// Per-plugin bounded queue configuration.
    pub queue: QueueConfig,
    /// Callback error handling policy.
    pub failure_policy: FailurePolicy,
    /// Opaque plugin-specific configuration passed to its registered factory.
    pub plugin: serde_json::Value,
}

impl PluginConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if !is_valid_identifier(&self.name) {
            return Err(ConfigError::InvalidPluginName(self.name.clone()));
        }

        if !is_valid_identifier(&self.kind) {
            return Err(ConfigError::InvalidPluginKind(self.kind.clone()));
        }

        if self.queue.capacity_events == 0 {
            return Err(ConfigError::ZeroEventCapacity(self.name.clone()));
        }

        if self.queue.capacity_bytes == 0 {
            return Err(ConfigError::ZeroByteCapacity(self.name.clone()));
        }

        Ok(())
    }
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            enabled: true,
            required_at_startup: false,
            kind: "null".to_owned(),
            subscriptions: EventSubscriptions::default(),
            queue: QueueConfig::default(),
            failure_policy: FailurePolicy::default(),
            plugin: serde_json::Value::Object(serde_json::Map::new()),
        }
    }
}

/// Per-plugin queue limits and overflow behavior.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct QueueConfig {
    /// Maximum number of queued events.
    pub capacity_events: usize,
    /// Maximum attributed bytes retained by queued events.
    pub capacity_bytes: usize,
    /// Action taken when either queue limit is reached.
    pub overflow: OverflowPolicy,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            capacity_events: DEFAULT_QUEUE_CAPACITY_EVENTS,
            capacity_bytes: DEFAULT_QUEUE_CAPACITY_BYTES,
            overflow: OverflowPolicy::DisablePlugin,
        }
    }
}

/// Policy applied when a plugin queue cannot accept an event.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OverflowPolicy {
    /// Disable the plugin so a chain-data gap cannot remain silent.
    #[default]
    DisablePlugin,
    /// Drop only the newest event and keep the plugin running.
    ///
    /// This is intended for explicitly best-effort consumers, particularly
    /// ephemeral mempool or diagnostic consumers.
    DropNewest,
}

impl OverflowPolicy {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::DisablePlugin => "disable_plugin",
            Self::DropNewest => "drop_newest",
        }
    }
}

/// Policy applied after a plugin callback error or panic.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailurePolicy {
    /// Disable only the failing plugin.
    #[default]
    DisablePlugin,
    /// Record the error and continue with the next event.
    Continue,
}

/// Invalid manager configuration.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ConfigError {
    /// Shutdown timeout must allow at least some cleanup time.
    #[error("geyser.shutdown_timeout must be greater than zero")]
    ZeroShutdownTimeout,
    /// Global queue memory limit must be non-zero.
    #[error("geyser.total_queue_bytes must be greater than zero")]
    ZeroTotalQueueBytes,
    /// Plugin names must be safe bounded identifiers.
    #[error("invalid Geyser plugin name {0:?}; use 1-64 ASCII letters, digits, '-' or '_'")]
    InvalidPluginName(String),
    /// Plugin kinds must be safe bounded identifiers.
    #[error("invalid Geyser plugin kind {0:?}; use 1-64 ASCII letters, digits, '-' or '_'")]
    InvalidPluginKind(String),
    /// Plugin names must be unique.
    #[error("duplicate Geyser plugin name {0:?}")]
    DuplicatePluginName(String),
    /// An event-count queue limit must be non-zero.
    #[error("Geyser plugin {0:?} queue capacity_events must be greater than zero")]
    ZeroEventCapacity(String),
    /// A byte queue limit must be non-zero.
    #[error("Geyser plugin {0:?} queue capacity_bytes must be greater than zero")]
    ZeroByteCapacity(String),
    /// Summing queue limits overflowed the platform integer type.
    #[error("sum of Geyser plugin queue byte capacities overflowed")]
    QueueCapacityOverflow,
    /// Enabled plugin queue limits exceed the configured global ceiling.
    #[error(
        "enabled Geyser plugin queues request {configured} bytes, exceeding total_queue_bytes {limit}"
    )]
    TotalQueueBytesExceeded {
        /// Sum of enabled per-plugin queue capacities.
        configured: usize,
        /// Configured global limit.
        limit: usize,
    },
}

fn is_valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_aggregate_queue_memory() {
        let plugin = PluginConfig {
            name: "null-a".to_owned(),
            queue: QueueConfig {
                capacity_bytes: 10,
                ..QueueConfig::default()
            },
            ..PluginConfig::default()
        };
        let config = Config {
            enabled: true,
            total_queue_bytes: 9,
            plugins: vec![plugin],
            ..Config::default()
        };

        assert_eq!(
            config.validate(),
            Err(ConfigError::TotalQueueBytesExceeded {
                configured: 10,
                limit: 9,
            })
        );
    }

    #[test]
    fn rejects_duplicate_metric_labels() {
        let plugin = PluginConfig {
            name: "duplicate".to_owned(),
            ..PluginConfig::default()
        };
        let config = Config {
            plugins: vec![plugin.clone(), plugin],
            ..Config::default()
        };

        assert_eq!(
            config.validate(),
            Err(ConfigError::DuplicatePluginName("duplicate".to_owned()))
        );
    }

    #[test]
    fn parses_node_toml_with_plugin_specific_config() {
        let config: Config = toml::from_str(
            r#"
enabled = true
shutdown_timeout = "5s"
total_queue_bytes = 2048

[[plugins]]
name = "null-main"
kind = "null"
required_at_startup = true

[plugins.subscriptions]
block_accepted = true
best_chain = true
finalized_blocks = true
mempool = false

[plugins.queue]
capacity_events = 16
capacity_bytes = 1024
overflow = "disable_plugin"

[plugins.plugin]
label = "primary"
"#,
        )
        .expect("valid Geyser TOML config");

        config.validate().expect("parsed config is valid");
        assert!(config.enabled);
        assert_eq!(config.plugins.len(), 1);
        assert_eq!(config.plugins[0].plugin["label"], "primary");
        assert!(!config.plugins[0].subscriptions.mempool);
    }
}
