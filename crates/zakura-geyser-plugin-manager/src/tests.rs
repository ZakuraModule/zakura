use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc as std_mpsc, Arc,
};

use tokio::time::{sleep, timeout, Duration};
use zakura_geyser_plugin_interface::{
    BestChainChange, EventEnvelope, EventSubscriptions, GeyserPlugin, PluginEvent, PluginResult,
};

use super::*;

#[derive(Debug)]
struct CountingPlugin {
    handled: Arc<AtomicUsize>,
}

impl GeyserPlugin for CountingPlugin {
    fn name(&self) -> &'static str {
        "counting"
    }

    fn subscriptions(&self) -> EventSubscriptions {
        EventSubscriptions::all()
    }

    fn on_event(&mut self, _event: Arc<EventEnvelope>) -> PluginResult {
        self.handled.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Debug)]
struct BlockingPlugin {
    entered: std_mpsc::Sender<()>,
    release: std_mpsc::Receiver<()>,
}

#[derive(Debug)]
struct LifecyclePlugin {
    fail_load: bool,
    unloaded: Arc<AtomicUsize>,
}

impl GeyserPlugin for LifecyclePlugin {
    fn name(&self) -> &'static str {
        "lifecycle"
    }

    fn subscriptions(&self) -> EventSubscriptions {
        EventSubscriptions::all()
    }

    fn on_load(&mut self) -> PluginResult {
        if self.fail_load {
            Err(PluginError::new("expected test load failure"))
        } else {
            Ok(())
        }
    }

    fn on_event(&mut self, _event: Arc<EventEnvelope>) -> PluginResult {
        Ok(())
    }

    fn on_unload(&mut self) -> PluginResult {
        self.unloaded.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

impl GeyserPlugin for BlockingPlugin {
    fn name(&self) -> &'static str {
        "blocking"
    }

    fn subscriptions(&self) -> EventSubscriptions {
        EventSubscriptions::all()
    }

    fn on_event(&mut self, _event: Arc<EventEnvelope>) -> PluginResult {
        let _ = self.entered.send(());
        let _ = self.release.recv();
        Ok(())
    }
}

fn best_chain_event(height: u32) -> PluginEvent {
    PluginEvent::BestChainChanged(BestChainChange::Reset {
        height: zakura_chain::block::Height(height),
        hash: zakura_chain::block::Hash([0; 32]),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blocked_plugin_does_not_block_another_plugin() {
    let (entered_tx, entered_rx) = std_mpsc::channel();
    let (release_tx, release_rx) = std_mpsc::channel();
    let handled = Arc::new(AtomicUsize::new(0));

    let mut registry = PluginRegistry::new();
    let release_rx = std::sync::Mutex::new(Some(release_rx));
    registry.register("blocking", move |_| {
        Ok(Box::new(BlockingPlugin {
            entered: entered_tx.clone(),
            release: release_rx
                .lock()
                .expect("test mutex is not poisoned")
                .take()
                .expect("factory is called once"),
        }))
    });
    let handled_for_factory = handled.clone();
    registry.register("counting", move |_| {
        Ok(Box::new(CountingPlugin {
            handled: handled_for_factory.clone(),
        }))
    });

    let tiny_queue = QueueConfig {
        capacity_events: 1,
        capacity_bytes: 1024,
        overflow: OverflowPolicy::DisablePlugin,
    };
    let config = Config {
        enabled: true,
        shutdown_timeout: Duration::from_secs(1),
        total_queue_bytes: 2048,
        plugins: vec![
            PluginConfig {
                name: "slow".to_owned(),
                kind: "blocking".to_owned(),
                queue: tiny_queue,
                ..PluginConfig::default()
            },
            PluginConfig {
                name: "fast".to_owned(),
                kind: "counting".to_owned(),
                queue: QueueConfig {
                    capacity_events: 8,
                    capacity_bytes: 1024,
                    ..QueueConfig::default()
                },
                ..PluginConfig::default()
            },
        ],
    };

    let manager = GeyserPluginManager::start(config, registry).expect("manager starts");
    let publisher = manager.publisher();
    assert_eq!(publisher.try_publish(best_chain_event(1)).routed, 2);
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("blocking callback starts");

    assert_eq!(publisher.try_publish(best_chain_event(2)).routed, 2);
    let overflow = publisher.try_publish(best_chain_event(3));
    assert_eq!(overflow.routed, 1);
    assert_eq!(overflow.rejected, 1);

    timeout(Duration::from_secs(1), async {
        while handled.load(Ordering::Relaxed) < 3 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("fast plugin handles all events while slow plugin is blocked");

    release_tx.send(()).expect("release blocking callback");
    manager.shutdown().await;
}

#[tokio::test]
async fn disabled_manager_rejects_without_starting_workers() {
    let manager = GeyserPluginManager::start(Config::default(), PluginRegistry::new())
        .expect("disabled config is valid");

    let report = manager.publisher().try_publish(best_chain_event(1));
    assert!(report.not_accepting);
    assert_eq!(report.routed, 0);

    manager.shutdown().await;
}

#[tokio::test]
async fn startup_failure_unloads_plugins_that_were_already_loaded() {
    let first_unloaded = Arc::new(AtomicUsize::new(0));
    let failing_unloaded = Arc::new(AtomicUsize::new(0));
    let mut registry = PluginRegistry::new();
    let first_unloaded_for_factory = first_unloaded.clone();
    registry.register("loads", move |_| {
        Ok(Box::new(LifecyclePlugin {
            fail_load: false,
            unloaded: first_unloaded_for_factory.clone(),
        }))
    });
    let failing_unloaded_for_factory = failing_unloaded.clone();
    registry.register("fails", move |_| {
        Ok(Box::new(LifecyclePlugin {
            fail_load: true,
            unloaded: failing_unloaded_for_factory.clone(),
        }))
    });

    let config = Config {
        enabled: true,
        plugins: vec![
            PluginConfig {
                name: "first".to_owned(),
                kind: "loads".to_owned(),
                ..PluginConfig::default()
            },
            PluginConfig {
                name: "required".to_owned(),
                kind: "fails".to_owned(),
                required_at_startup: true,
                ..PluginConfig::default()
            },
        ],
        ..Config::default()
    };

    let result = GeyserPluginManager::start(config, registry);
    assert!(matches!(result, Err(StartError::LoadPlugin { .. })));
    assert_eq!(first_unloaded.load(Ordering::Relaxed), 1);
    assert_eq!(failing_unloaded.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn dropping_manager_requests_plugin_unload() {
    let unloaded = Arc::new(AtomicUsize::new(0));
    let unloaded_for_factory = unloaded.clone();
    let mut registry = PluginRegistry::new();
    registry.register("lifecycle", move |_| {
        Ok(Box::new(LifecyclePlugin {
            fail_load: false,
            unloaded: unloaded_for_factory.clone(),
        }))
    });
    let config = Config {
        enabled: true,
        plugins: vec![PluginConfig {
            name: "lifecycle".to_owned(),
            kind: "lifecycle".to_owned(),
            ..PluginConfig::default()
        }],
        ..Config::default()
    };

    let manager = GeyserPluginManager::start(config, registry).expect("manager starts");
    drop(manager);

    timeout(Duration::from_secs(1), async {
        while unloaded.load(Ordering::Relaxed) == 0 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("dropped manager unloads its plugin");
}
