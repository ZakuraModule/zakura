//! Bounded, isolated runtime for Zakura Geyser plugins.
//!
//! Each plugin gets one bounded queue and one worker. Publishing never waits
//! for a plugin callback or for queue capacity.

mod config;

pub use config::{
    Config, ConfigError, FailurePolicy, OverflowPolicy, PluginConfig, QueueConfig,
    DEFAULT_QUEUE_CAPACITY_BYTES, DEFAULT_QUEUE_CAPACITY_EVENTS, DEFAULT_TOTAL_QUEUE_BYTES,
};

use std::{
    collections::HashMap,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Instant, SystemTime},
};

use thiserror::Error;
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{self, Instant as TokioInstant},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use zakura_geyser_plugin_interface::{
    EventEnvelope, EventKind, EventSubscriptions, GeyserPlugin, PluginError, PluginEvent,
    PluginResult, SessionId, EVENT_SCHEMA_VERSION, GEYSER_INTERFACE_VERSION,
};

type Factory = dyn Fn(&PluginConfig) -> PluginResult<Box<dyn GeyserPlugin>> + Send + Sync + 'static;

/// Registry of compile-time plugin factories.
#[derive(Default)]
pub struct PluginRegistry {
    factories: HashMap<String, Arc<Factory>>,
}

impl PluginRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a registry containing manager-provided built-in plugins.
    pub fn with_builtin_plugins() -> Self {
        let mut registry = Self::new();
        registry.register("null", |config| {
            Ok(Box::new(NullPlugin::new(config.subscriptions)))
        });
        registry
    }

    /// Registers or replaces a built-in factory under `kind`.
    pub fn register<F>(&mut self, kind: impl Into<String>, factory: F)
    where
        F: Fn(&PluginConfig) -> PluginResult<Box<dyn GeyserPlugin>> + Send + Sync + 'static,
    {
        self.factories.insert(kind.into(), Arc::new(factory));
    }

    fn create(&self, config: &PluginConfig) -> Result<Box<dyn GeyserPlugin>, StartError> {
        let factory =
            self.factories
                .get(&config.kind)
                .ok_or_else(|| StartError::UnknownPluginKind {
                    plugin: config.name.clone(),
                    kind: config.kind.clone(),
                })?;

        factory(config).map_err(|source| StartError::CreatePlugin {
            plugin: config.name.clone(),
            source,
        })
    }
}

/// A plugin that intentionally discards every event.
#[derive(Debug)]
pub struct NullPlugin {
    subscriptions: EventSubscriptions,
}

impl NullPlugin {
    /// Creates a null plugin with explicit subscriptions.
    pub const fn new(subscriptions: EventSubscriptions) -> Self {
        Self { subscriptions }
    }
}

impl GeyserPlugin for NullPlugin {
    fn name(&self) -> &'static str {
        "null"
    }

    fn subscriptions(&self) -> EventSubscriptions {
        self.subscriptions
    }

    fn on_event(&mut self, _event: Arc<EventEnvelope>) -> PluginResult {
        Ok(())
    }
}

/// A running plugin manager.
pub struct GeyserPluginManager {
    publisher: PluginPublisher,
    shutdown: CancellationToken,
    workers: Vec<JoinHandle<()>>,
    shutdown_timeout: std::time::Duration,
}

struct LoadedPlugin {
    config: PluginConfig,
    plugin: Box<dyn GeyserPlugin>,
    subscriptions: EventSubscriptions,
}

#[derive(Default)]
struct LoadedPlugins(Option<Vec<LoadedPlugin>>);

impl LoadedPlugins {
    fn push(&mut self, plugin: LoadedPlugin) {
        self.0.get_or_insert_with(Vec::new).push(plugin);
    }

    fn take(&mut self) -> Vec<LoadedPlugin> {
        self.0.take().unwrap_or_default()
    }
}

impl Drop for LoadedPlugins {
    fn drop(&mut self) {
        for mut loaded in self.0.take().unwrap_or_default() {
            unload_after_startup_failure(&loaded.config.name, loaded.plugin.as_mut());
        }
    }
}

impl GeyserPluginManager {
    /// Validates configuration, loads enabled plugins, and starts one worker per plugin.
    pub fn start(config: Config, registry: PluginRegistry) -> Result<Self, StartError> {
        config.validate()?;

        let shutdown = CancellationToken::new();
        let accepting = Arc::new(AtomicBool::new(config.enabled));
        let mut targets = Vec::new();
        let mut workers = Vec::new();
        let mut loaded_plugins = LoadedPlugins::default();

        if config.enabled {
            for plugin_config in config.plugins.iter().filter(|plugin| plugin.enabled) {
                let mut plugin = registry.create(plugin_config)?;
                let interface_version = plugin.interface_version();
                if interface_version != GEYSER_INTERFACE_VERSION {
                    return Err(StartError::InterfaceVersion {
                        plugin: plugin_config.name.clone(),
                        expected: GEYSER_INTERFACE_VERSION,
                        actual: interface_version,
                    });
                }

                let load_result = catch_unwind(AssertUnwindSafe(|| plugin.on_load()));
                match load_result {
                    Ok(Ok(())) => {}
                    Ok(Err(source)) if plugin_config.required_at_startup => {
                        unload_after_startup_failure(&plugin_config.name, plugin.as_mut());
                        return Err(StartError::LoadPlugin {
                            plugin: plugin_config.name.clone(),
                            source,
                        });
                    }
                    Err(_) if plugin_config.required_at_startup => {
                        unload_after_startup_failure(&plugin_config.name, plugin.as_mut());
                        return Err(StartError::LoadPluginPanic {
                            plugin: plugin_config.name.clone(),
                        });
                    }
                    Ok(Err(source)) => {
                        error!(
                            plugin = %plugin_config.name,
                            kind = %plugin_config.kind,
                            ?source,
                            "optional Geyser plugin failed to load; plugin disabled"
                        );
                        metrics::counter!(
                            "plugin.errors.total",
                            "plugin" => plugin_config.name.clone(),
                            "operation" => "load",
                            "kind" => "error"
                        )
                        .increment(1);
                        unload_after_startup_failure(&plugin_config.name, plugin.as_mut());
                        continue;
                    }
                    Err(_) => {
                        error!(
                            plugin = %plugin_config.name,
                            kind = %plugin_config.kind,
                            "optional Geyser plugin panicked while loading; plugin disabled"
                        );
                        metrics::counter!(
                            "plugin.errors.total",
                            "plugin" => plugin_config.name.clone(),
                            "operation" => "load",
                            "kind" => "panic"
                        )
                        .increment(1);
                        unload_after_startup_failure(&plugin_config.name, plugin.as_mut());
                        continue;
                    }
                }

                let subscriptions = plugin.subscriptions();
                loaded_plugins.push(LoadedPlugin {
                    config: plugin_config.clone(),
                    plugin,
                    subscriptions,
                });
            }
        }

        for LoadedPlugin {
            config: plugin_config,
            plugin,
            subscriptions,
        } in loaded_plugins.take()
        {
            let active = Arc::new(AtomicBool::new(true));
            let stop = CancellationToken::new();
            let budget = Arc::new(ByteBudget::new(plugin_config.queue.capacity_bytes));
            let depth = Arc::new(AtomicUsize::new(0));
            let (sender, receiver) = mpsc::channel(plugin_config.queue.capacity_events);
            let plugin_name = plugin_config.name.clone();

            metrics::gauge!(
                "plugin.loaded",
                "plugin" => plugin_name.clone(),
                "mode" => "builtin"
            )
            .set(1.0);
            metrics::gauge!("plugin.ready", "plugin" => plugin_name.clone()).set(1.0);
            metrics::gauge!(
                "plugin.queue.capacity_events",
                "plugin" => plugin_name.clone()
            )
            .set(metric_count(plugin_config.queue.capacity_events));
            metrics::gauge!(
                "plugin.queue.capacity_bytes",
                "plugin" => plugin_name.clone()
            )
            .set(metric_count(plugin_config.queue.capacity_bytes));

            info!(
                plugin = %plugin_name,
                implementation = plugin.name(),
                kind = %plugin_config.kind,
                capacity_events = plugin_config.queue.capacity_events,
                capacity_bytes = plugin_config.queue.capacity_bytes,
                "loaded built-in Geyser plugin"
            );

            targets.push(PluginTarget {
                name: plugin_name.clone(),
                subscriptions,
                sender,
                budget: budget.clone(),
                depth: depth.clone(),
                active: active.clone(),
                stop: stop.clone(),
                overflow: plugin_config.queue.overflow,
            });

            workers.push(tokio::spawn(run_plugin_worker(
                plugin_name,
                plugin,
                receiver,
                active,
                stop,
                shutdown.clone(),
                plugin_config.failure_policy,
            )));
        }

        let publisher = PluginPublisher {
            inner: Arc::new(PublisherInner {
                targets,
                accepting,
                session_id: SessionId(rand::random()),
                next_sequence: AtomicU64::new(1),
            }),
        };

        if config.enabled && publisher.inner.targets.is_empty() {
            warn!("Geyser plugin manager is enabled but no plugins are running");
        }

        Ok(Self {
            publisher,
            shutdown,
            workers,
            shutdown_timeout: config.shutdown_timeout,
        })
    }

    /// Returns a cheap cloneable non-blocking publisher.
    pub fn publisher(&self) -> PluginPublisher {
        self.publisher.clone()
    }

    /// Returns true when the manager is accepting events for any active subscriber of `kind`.
    pub fn has_subscribers(&self, kind: EventKind) -> bool {
        self.publisher.has_subscribers(kind)
    }

    /// Stops publication, drains queued events within the configured timeout, and unloads plugins.
    pub async fn shutdown(mut self) {
        self.publisher.stop_accepting();
        self.shutdown.cancel();

        let deadline = TokioInstant::now() + self.shutdown_timeout;
        for mut worker in self.workers.drain(..) {
            if time::timeout_at(deadline, &mut worker).await.is_err() {
                warn!(
                    timeout = ?self.shutdown_timeout,
                    "Geyser plugin shutdown timed out; aborting remaining worker"
                );
                worker.abort();
            }
        }
    }
}

impl Drop for GeyserPluginManager {
    fn drop(&mut self) {
        self.publisher.stop_accepting();
        self.shutdown.cancel();
        // Dropping a Tokio join handle detaches its task. The cancellation lets
        // workers run `on_unload` after any synchronous callback returns. An
        // immediate abort here would skip plugin cleanup at the next await.
    }
}

/// A cloneable, non-blocking event publisher.
#[derive(Clone)]
pub struct PluginPublisher {
    inner: Arc<PublisherInner>,
}

impl PluginPublisher {
    /// Returns true when at least one active plugin subscribes to `kind`.
    pub fn has_subscribers(&self, kind: EventKind) -> bool {
        self.inner.accepting.load(Ordering::Acquire)
            && self
                .inner
                .targets
                .iter()
                .any(|target| target.is_interested(kind))
    }

    /// Routes an event without waiting for queue capacity or plugin callbacks.
    pub fn try_publish(&self, event: PluginEvent) -> PublishReport {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return PublishReport {
                not_accepting: true,
                ..PublishReport::default()
            };
        }

        let kind = event.kind();
        if !self.has_subscribers(kind) {
            return PublishReport::default();
        }

        let sequence = self.inner.next_sequence.fetch_add(1, Ordering::Relaxed);
        let envelope = Arc::new(EventEnvelope {
            schema_version: EVENT_SCHEMA_VERSION,
            session_id: self.inner.session_id,
            sequence,
            observed_at: SystemTime::now(),
            payload: event,
        });
        let event_bytes = envelope.estimated_size_bytes();
        let event_name = kind.as_str();
        let mut report = PublishReport::default();

        metrics::counter!("plugin.events.published.total", "event" => event_name).increment(1);

        for target in &self.inner.targets {
            if !target.is_interested(kind) {
                report.skipped = report.skipped.saturating_add(1);
                continue;
            }

            if !target.budget.try_reserve(event_bytes) {
                target.reject_overflow("bytes");
                report.rejected = report.rejected.saturating_add(1);
                continue;
            }

            let accounting = QueueAccounting::new(
                target.name.clone(),
                event_bytes,
                target.budget.clone(),
                target.depth.clone(),
            );
            let queued = QueuedEvent {
                envelope: envelope.clone(),
                _accounting: accounting,
            };

            match target.sender.try_send(queued) {
                Ok(()) => {
                    metrics::counter!(
                        "plugin.events.routed.total",
                        "plugin" => target.name.clone(),
                        "event" => event_name
                    )
                    .increment(1);
                    metrics::counter!(
                        "plugin.event.bytes.total",
                        "plugin" => target.name.clone(),
                        "event" => event_name
                    )
                    .increment(u64::try_from(event_bytes).unwrap_or(u64::MAX));
                    report.routed = report.routed.saturating_add(1);
                }
                Err(mpsc::error::TrySendError::Full(_queued)) => {
                    target.reject_overflow("events");
                    report.rejected = report.rejected.saturating_add(1);
                }
                Err(mpsc::error::TrySendError::Closed(_queued)) => {
                    target.active.store(false, Ordering::Release);
                    metrics::counter!(
                        "plugin.events.rejected.total",
                        "plugin" => target.name.clone(),
                        "reason" => "closed"
                    )
                    .increment(1);
                    report.rejected = report.rejected.saturating_add(1);
                }
            }
        }

        report
    }

    fn stop_accepting(&self) {
        self.inner.accepting.store(false, Ordering::Release);
    }
}

struct PublisherInner {
    targets: Vec<PluginTarget>,
    accepting: Arc<AtomicBool>,
    session_id: SessionId,
    next_sequence: AtomicU64,
}

struct PluginTarget {
    name: String,
    subscriptions: EventSubscriptions,
    sender: mpsc::Sender<QueuedEvent>,
    budget: Arc<ByteBudget>,
    depth: Arc<AtomicUsize>,
    active: Arc<AtomicBool>,
    stop: CancellationToken,
    overflow: OverflowPolicy,
}

impl PluginTarget {
    fn is_interested(&self, kind: EventKind) -> bool {
        self.active.load(Ordering::Acquire) && self.subscriptions.contains(kind)
    }

    fn reject_overflow(&self, limit: &'static str) {
        metrics::counter!(
            "plugin.queue.overflow.total",
            "plugin" => self.name.clone(),
            "policy" => self.overflow.as_str(),
            "limit" => limit
        )
        .increment(1);
        metrics::counter!(
            "plugin.events.rejected.total",
            "plugin" => self.name.clone(),
            "reason" => "queue_full"
        )
        .increment(1);

        match self.overflow {
            OverflowPolicy::DisablePlugin => {
                if self.active.swap(false, Ordering::AcqRel) {
                    warn!(
                        plugin = %self.name,
                        limit,
                        "Geyser plugin queue overflowed; disabling plugin to make the event gap explicit"
                    );
                    metrics::counter!(
                        "plugin.disabled.total",
                        "plugin" => self.name.clone(),
                        "reason" => "queue_overflow"
                    )
                    .increment(1);
                    metrics::gauge!("plugin.ready", "plugin" => self.name.clone()).set(0.0);
                    self.stop.cancel();
                }
            }
            OverflowPolicy::DropNewest => {
                warn!(
                    plugin = %self.name,
                    limit,
                    "dropping newest Geyser event because the plugin queue is full"
                );
            }
        }
    }
}

struct ByteBudget {
    used: AtomicUsize,
    limit: usize,
}

impl ByteBudget {
    const fn new(limit: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            limit,
        }
    }

    fn try_reserve(&self, bytes: usize) -> bool {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|new_used| *new_used <= self.limit)
            })
            .is_ok()
    }
}

struct QueueAccounting {
    plugin: String,
    bytes: usize,
    budget: Arc<ByteBudget>,
    depth: Arc<AtomicUsize>,
}

impl QueueAccounting {
    fn new(plugin: String, bytes: usize, budget: Arc<ByteBudget>, depth: Arc<AtomicUsize>) -> Self {
        depth.fetch_add(1, Ordering::AcqRel);
        metrics::gauge!("plugin.queue.events", "plugin" => plugin.clone()).increment(1.0);
        metrics::gauge!("plugin.queue.bytes", "plugin" => plugin.clone())
            .increment(metric_count(bytes));

        Self {
            plugin,
            bytes,
            budget,
            depth,
        }
    }
}

impl Drop for QueueAccounting {
    fn drop(&mut self) {
        let previous_bytes = self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
        debug_assert!(previous_bytes >= self.bytes);
        let previous_depth = self.depth.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous_depth > 0);
        metrics::gauge!("plugin.queue.events", "plugin" => self.plugin.clone()).decrement(1.0);
        metrics::gauge!("plugin.queue.bytes", "plugin" => self.plugin.clone())
            .decrement(metric_count(self.bytes));
    }
}

struct QueuedEvent {
    envelope: Arc<EventEnvelope>,
    _accounting: QueueAccounting,
}

fn unload_after_startup_failure(name: &str, plugin: &mut dyn GeyserPlugin) {
    match catch_unwind(AssertUnwindSafe(|| plugin.on_unload())) {
        Ok(Ok(())) => {}
        Ok(Err(source)) => {
            warn!(
                plugin = name,
                ?source,
                "Geyser plugin cleanup failed after a startup error"
            );
        }
        Err(_) => {
            warn!(
                plugin = name,
                "Geyser plugin cleanup panicked after a startup error"
            );
        }
    }
}

async fn run_plugin_worker(
    name: String,
    mut plugin: Box<dyn GeyserPlugin>,
    mut receiver: mpsc::Receiver<QueuedEvent>,
    active: Arc<AtomicBool>,
    stop: CancellationToken,
    shutdown: CancellationToken,
    failure_policy: FailurePolicy,
) {
    loop {
        tokio::select! {
            biased;

            () = stop.cancelled() => break,
            () = shutdown.cancelled() => {
                receiver.close();
                while let Some(queued) = receiver.recv().await {
                    if !handle_event(&name, plugin.as_mut(), queued.envelope, failure_policy) {
                        break;
                    }
                }
                break;
            }
            queued = receiver.recv() => {
                let Some(queued) = queued else {
                    break;
                };

                if !handle_event(&name, plugin.as_mut(), queued.envelope, failure_policy) {
                    active.store(false, Ordering::Release);
                    break;
                }
            }
        }
    }

    active.store(false, Ordering::Release);
    metrics::gauge!("plugin.ready", "plugin" => name.clone()).set(0.0);

    match catch_unwind(AssertUnwindSafe(|| plugin.on_unload())) {
        Ok(Ok(())) => {}
        Ok(Err(source)) => {
            error!(plugin = %name, ?source, "Geyser plugin failed to unload");
            metrics::counter!(
                "plugin.errors.total",
                "plugin" => name.clone(),
                "operation" => "unload",
                "kind" => "error"
            )
            .increment(1);
        }
        Err(_) => {
            error!(plugin = %name, "Geyser plugin panicked while unloading");
            metrics::counter!(
                "plugin.errors.total",
                "plugin" => name.clone(),
                "operation" => "unload",
                "kind" => "panic"
            )
            .increment(1);
        }
    }

    metrics::gauge!(
        "plugin.loaded",
        "plugin" => name.clone(),
        "mode" => "builtin"
    )
    .set(0.0);
    info!(plugin = %name, "unloaded Geyser plugin");
}

fn handle_event(
    name: &str,
    plugin: &mut dyn GeyserPlugin,
    event: Arc<EventEnvelope>,
    failure_policy: FailurePolicy,
) -> bool {
    let event_name = event.kind().as_str();
    let started = Instant::now();
    metrics::gauge!("plugin.handler.in_flight", "plugin" => name.to_owned()).increment(1.0);
    let result = catch_unwind(AssertUnwindSafe(|| plugin.on_event(event)));
    metrics::gauge!("plugin.handler.in_flight", "plugin" => name.to_owned()).decrement(1.0);

    let (result_name, should_continue) = match result {
        Ok(Ok(())) => ("ok", true),
        Ok(Err(source)) => {
            error!(
                plugin = name,
                event = event_name,
                ?source,
                "Geyser plugin callback failed"
            );
            metrics::counter!(
                "plugin.errors.total",
                "plugin" => name.to_owned(),
                "operation" => "event",
                "kind" => "error"
            )
            .increment(1);
            ("error", failure_policy == FailurePolicy::Continue)
        }
        Err(_) => {
            error!(
                plugin = name,
                event = event_name,
                "Geyser plugin callback panicked"
            );
            metrics::counter!(
                "plugin.errors.total",
                "plugin" => name.to_owned(),
                "operation" => "event",
                "kind" => "panic"
            )
            .increment(1);
            ("panic", failure_policy == FailurePolicy::Continue)
        }
    };

    metrics::histogram!(
        "plugin.handler.duration_seconds",
        "plugin" => name.to_owned(),
        "event" => event_name,
        "result" => result_name
    )
    .record(started.elapsed().as_secs_f64());

    if !should_continue {
        metrics::counter!(
            "plugin.disabled.total",
            "plugin" => name.to_owned(),
            "reason" => "callback_failure"
        )
        .increment(1);
    }

    should_continue
}

fn metric_count(value: usize) -> f64 {
    // This cast is safe because every `usize` is finite as `f64`; metrics accept
    // the possible loss of integer precision above 2^53 for impossible queue sizes.
    value as f64
}

/// Result of one non-blocking publish operation.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct PublishReport {
    /// Number of plugin queues that accepted the event.
    pub routed: usize,
    /// Number of plugins that were inactive or not subscribed.
    pub skipped: usize,
    /// Number of subscribed plugins whose queue rejected the event.
    pub rejected: usize,
    /// True when manager shutdown or global disable rejected publication.
    pub not_accepting: bool,
}

/// Failure to configure or start the plugin runtime.
#[derive(Debug, Error)]
pub enum StartError {
    /// Manager configuration is invalid.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// No factory was registered for an enabled built-in kind.
    #[error("Geyser plugin {plugin:?} uses unknown built-in kind {kind:?}")]
    UnknownPluginKind {
        /// Configured plugin instance name.
        plugin: String,
        /// Requested built-in implementation kind.
        kind: String,
    },
    /// A factory could not construct a plugin.
    #[error("failed to create Geyser plugin {plugin:?}: {source}")]
    CreatePlugin {
        /// Configured plugin instance name.
        plugin: String,
        /// Factory error.
        #[source]
        source: PluginError,
    },
    /// Plugin and node callback interface versions differ.
    #[error(
        "Geyser plugin {plugin:?} interface version mismatch: expected {expected}, got {actual}"
    )]
    InterfaceVersion {
        /// Configured plugin instance name.
        plugin: String,
        /// Node interface version.
        expected: u32,
        /// Plugin interface version.
        actual: u32,
    },
    /// A required plugin returned an initialization error.
    #[error("required Geyser plugin {plugin:?} failed to load: {source}")]
    LoadPlugin {
        /// Configured plugin instance name.
        plugin: String,
        /// Plugin initialization error.
        #[source]
        source: PluginError,
    },
    /// A required plugin panicked during initialization.
    #[error("required Geyser plugin {plugin:?} panicked while loading")]
    LoadPluginPanic {
        /// Configured plugin instance name.
        plugin: String,
    },
}

#[cfg(test)]
mod tests;
