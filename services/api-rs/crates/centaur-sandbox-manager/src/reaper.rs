//! Background garbage collection for leaked sandboxes.
//!
//! Sessions pause idle sandboxes (replicas to zero), but paused sandboxes and
//! sandboxes whose sessions never go idle still need a restart-surviving
//! backstop. The reaper sweeps the backend's observed sandboxes and stops any
//! that exceed the configured max lifetime, releasing the sandbox, its proxy
//! resources, and its node pod slots.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use centaur_sandbox_core::ObservedSandbox;
use centaur_sandbox_core::SandboxResult;
use tokio::time::{MissedTickBehavior, interval, timeout};
use tracing::{info, warn};

use crate::SandboxManager;

#[derive(Clone, Copy, Debug)]
pub struct SandboxReaperConfig {
    /// How often to sweep.
    pub interval: Duration,
    /// Stop any sandbox older than this regardless of status. `None` disables
    /// the max-lifetime sweep.
    pub max_lifetime: Option<Duration>,
}

impl SandboxReaperConfig {
    pub fn is_enabled(&self) -> bool {
        !self.interval.is_zero()
    }
}

pub struct SandboxReaper {
    manager: Arc<SandboxManager>,
    config: SandboxReaperConfig,
}

impl SandboxReaper {
    pub fn new(manager: Arc<SandboxManager>, config: SandboxReaperConfig) -> Self {
        Self { manager, config }
    }

    pub fn spawn(self) {
        tokio::spawn(async move {
            let mut tick = interval(self.config.interval);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if let Err(error) = self.reap_once().await {
                    warn!(%error, "sandbox reaper sweep failed");
                }
            }
        });
    }

    /// Sweep once and return how many sandboxes were stopped. A failed stop is
    /// logged and skipped so one wedged sandbox cannot stall the sweep.
    pub async fn reap_once(&self) -> SandboxResult<usize> {
        let now = SystemTime::now();
        let mut reaped = 0;
        for observed in self.manager.list_observed().await? {
            let Some(reason) = reap_reason(&observed, now, &self.config) else {
                continue;
            };
            let Some(resource_uid) = observed.resource_uid.as_deref() else {
                warn!(sandbox_id = %observed.id.as_str(), reason, "skipping max-lifetime reaper without a stable resource UID");
                continue;
            };
            match timeout(
                Duration::from_secs(10),
                self.manager.stop_exact(&observed.id, Some(resource_uid)),
            )
            .await
            {
                Ok(Ok(())) => {
                    reaped += 1;
                    info!(
                        sandbox_id = %observed.id.as_str(),
                        reason,
                        "reaped expired sandbox"
                    );
                }
                Ok(Err(error)) => {
                    warn!(
                        sandbox_id = %observed.id.as_str(),
                        reason,
                        %error,
                        "failed to reap expired sandbox"
                    );
                }
                Err(_) => warn!(
                    sandbox_id = %observed.id.as_str(),
                    reason,
                    "timed out reaping sandbox; cleanup remains retryable"
                ),
            }
        }
        Ok(reaped)
    }
}

fn reap_reason(
    observed: &ObservedSandbox,
    now: SystemTime,
    config: &SandboxReaperConfig,
) -> Option<&'static str> {
    // Kubernetes reports a finalizer-retained deleting CR as Gone while still
    // including its UID. It is mandatory retry work even when max-lifetime
    // reaping is disabled and no session receives another request.
    if observed.status == centaur_sandbox_core::SandboxStatus::Gone
        && observed.resource_uid.is_some()
    {
        return Some("terminating_cleanup");
    }
    if observed.status.is_terminal() {
        return None;
    }
    if let (Some(max_lifetime), Some(created_at)) = (config.max_lifetime, observed.created_at)
        && now
            .duration_since(created_at)
            .is_ok_and(|age| age >= max_lifetime)
    {
        return Some("max_lifetime");
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use centaur_sandbox_core::{
        SandboxBackend, SandboxError, SandboxHandle, SandboxId, SandboxIo, SandboxSpec,
        SandboxStatus,
    };

    use super::*;

    fn config(max_lifetime: Option<Duration>) -> SandboxReaperConfig {
        SandboxReaperConfig {
            interval: Duration::from_secs(60),
            max_lifetime,
        }
    }

    fn observed(status: centaur_sandbox_core::SandboxStatus) -> ObservedSandbox {
        ObservedSandbox::new("sandbox-1", "fake", status)
    }

    #[test]
    fn reaps_running_sandbox_past_max_lifetime() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Running)
            .with_created_at(Some(now - Duration::from_secs(100_000)));

        let reason = reap_reason(&sandbox, now, &config(Some(Duration::from_secs(86_400))));

        assert_eq!(reason, Some("max_lifetime"));
    }

    #[test]
    fn reaps_workflow_run_sandbox_past_max_lifetime() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Running)
            .with_component(Some("workflow-run".to_owned()))
            .with_created_at(Some(now - Duration::from_secs(100_000)));

        let reason = reap_reason(&sandbox, now, &config(Some(Duration::from_secs(86_400))));

        assert_eq!(reason, Some("max_lifetime"));
    }

    #[test]
    fn reaps_suspended_sandbox_past_max_lifetime() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Suspended)
            .with_created_at(Some(now - Duration::from_secs(100_000)))
            .with_suspended_since(Some(now - Duration::from_secs(60)));

        let reason = reap_reason(&sandbox, now, &config(Some(Duration::from_secs(86_400))));

        assert_eq!(reason, Some("max_lifetime"));
    }

    #[test]
    fn keeps_running_sandbox_within_max_lifetime() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Running)
            .with_created_at(Some(now - Duration::from_secs(60)));

        let reason = reap_reason(&sandbox, now, &config(Some(Duration::from_secs(86_400))));

        assert_eq!(reason, None);
    }

    #[test]
    fn ignores_terminal_sandboxes() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Gone)
            .with_created_at(Some(now - Duration::from_secs(100_000)));

        let reason = reap_reason(&sandbox, now, &config(Some(Duration::from_secs(86_400))));

        assert_eq!(reason, None);
    }

    #[test]
    fn disabled_max_lifetime_keeps_nonterminating_sandboxes() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Suspended)
            .with_created_at(Some(now - Duration::from_secs(100_000)))
            .with_suspended_since(Some(now - Duration::from_secs(100_000)));
        let config = config(None);

        assert!(config.is_enabled());
        assert_eq!(reap_reason(&sandbox, now, &config), None);
    }

    #[test]
    fn terminating_sandbox_is_mandatory_cleanup_without_max_lifetime() {
        let now = SystemTime::now();
        let sandbox = observed(centaur_sandbox_core::SandboxStatus::Gone)
            .with_resource_uid(Some("uid-terminating".to_owned()));

        assert_eq!(
            reap_reason(&sandbox, now, &config(None)),
            Some("terminating_cleanup")
        );
    }

    struct ReplacementBackend {
        observed: Vec<ObservedSandbox>,
        current_resource_uid: String,
        exact_stop_uids: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl SandboxBackend for ReplacementBackend {
        fn name(&self) -> &'static str {
            "replacement-test"
        }

        async fn create(&self, _spec: SandboxSpec) -> SandboxResult<SandboxHandle> {
            unreachable!("reaper does not create sandboxes")
        }

        async fn open_io(&self, _id: &SandboxId) -> SandboxResult<SandboxIo> {
            unreachable!("reaper does not open sandbox I/O")
        }

        async fn status(&self, _id: &SandboxId) -> SandboxResult<SandboxStatus> {
            Ok(SandboxStatus::Running)
        }

        async fn observe(&self, id: &SandboxId) -> SandboxResult<ObservedSandbox> {
            Ok(
                ObservedSandbox::new(id.clone(), self.name(), SandboxStatus::Running)
                    .with_resource_uid(Some(self.current_resource_uid.clone())),
            )
        }

        async fn list_observed(&self) -> SandboxResult<Vec<ObservedSandbox>> {
            Ok(self.observed.clone())
        }

        async fn stop(&self, _id: &SandboxId) -> SandboxResult<()> {
            Err(SandboxError::backend(
                "name-only stop must not be used by reaper",
            ))
        }

        async fn stop_exact(
            &self,
            _id: &SandboxId,
            expected_resource_uid: Option<&str>,
        ) -> SandboxResult<()> {
            self.exact_stop_uids
                .lock()
                .unwrap()
                .push(expected_resource_uid.unwrap_or_default().to_owned());
            if expected_resource_uid != Some(self.current_resource_uid.as_str()) {
                return Err(SandboxError::backend(
                    "same-name replacement survived exact stop",
                ));
            }
            Ok(())
        }

        async fn pause(&self, _id: &SandboxId) -> SandboxResult<()> {
            unreachable!("reaper does not pause sandboxes")
        }

        async fn resume(&self, _id: &SandboxId) -> SandboxResult<()> {
            unreachable!("reaper does not resume sandboxes")
        }
    }

    #[tokio::test]
    async fn reaper_does_not_stop_a_same_name_uid_replacement() {
        let id = SandboxId::new("sandbox-1");
        let backend = Arc::new(ReplacementBackend {
            observed: vec![
                ObservedSandbox::new(id, "replacement-test", SandboxStatus::Running)
                    .with_resource_uid(Some("uid-old".to_owned()))
                    .with_created_at(Some(SystemTime::now() - Duration::from_secs(100_000))),
            ],
            current_resource_uid: "uid-replacement".to_owned(),
            exact_stop_uids: Mutex::new(Vec::new()),
        });
        let reaper = SandboxReaper::new(
            Arc::new(SandboxManager::new(backend.clone())),
            config(Some(Duration::from_secs(1))),
        );

        assert_eq!(reaper.reap_once().await.unwrap(), 0);
        assert_eq!(
            backend.exact_stop_uids.lock().unwrap().as_slice(),
            ["uid-old"]
        );
    }

    #[tokio::test]
    async fn reaper_resumes_terminating_cleanup_without_max_lifetime() {
        let id = SandboxId::new("sandbox-terminating");
        let backend = Arc::new(ReplacementBackend {
            observed: vec![
                ObservedSandbox::new(id, "replacement-test", SandboxStatus::Gone)
                    .with_resource_uid(Some("uid-terminating".to_owned())),
            ],
            current_resource_uid: "uid-terminating".to_owned(),
            exact_stop_uids: Mutex::new(Vec::new()),
        });
        let reaper =
            SandboxReaper::new(Arc::new(SandboxManager::new(backend.clone())), config(None));

        assert_eq!(reaper.reap_once().await.unwrap(), 1);
        assert_eq!(
            backend.exact_stop_uids.lock().unwrap().as_slice(),
            ["uid-terminating"]
        );
    }
}
