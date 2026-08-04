use std::time::Duration;

use tokio::time::{MissedTickBehavior, interval};
use tracing::warn;

use crate::{RuntimeContext, SessionRuntimeError, record_idle_pause};

#[derive(Clone, Copy, Debug)]
pub struct SessionSandboxCleanupConfig {
    /// How often to sweep. `None` disables the cleanup worker entirely.
    pub interval: Option<Duration>,
    /// Pause session sandboxes whose latest execution has been terminal longer
    /// than this. `None` disables the idle backstop arm.
    pub idle_backstop: Option<Duration>,
}

impl SessionSandboxCleanupConfig {
    pub fn is_enabled(&self) -> bool {
        self.interval.is_some()
    }
}

#[derive(Debug, Default)]
pub struct SessionSandboxCleanupReport {
    pub idle_pause_attempts: usize,
    pub failed_idle_pauses: usize,
}

pub struct SessionSandboxCleanupWorker {
    ctx: RuntimeContext,
    config: SessionSandboxCleanupConfig,
}

impl SessionSandboxCleanupWorker {
    pub(crate) fn new(ctx: RuntimeContext, config: SessionSandboxCleanupConfig) -> Self {
        Self { ctx, config }
    }

    pub(crate) fn spawn(mut self) {
        let Some(interval_duration) = self.config.interval else {
            return;
        };
        tokio::spawn(async move {
            let mut tick = interval(interval_duration);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if let Err(error) = self.reap_once().await {
                    warn!(%error, "session sandbox cleanup worker sweep failed");
                }
            }
        });
    }

    pub(crate) async fn reap_once(
        &mut self,
    ) -> Result<SessionSandboxCleanupReport, SessionRuntimeError> {
        let mut report = SessionSandboxCleanupReport::default();
        self.pause_idle_sandboxes(&mut report).await?;
        Ok(report)
    }

    async fn pause_idle_sandboxes(
        &self,
        report: &mut SessionSandboxCleanupReport,
    ) -> Result<(), SessionRuntimeError> {
        let Some(idle_backstop) = self.config.idle_backstop else {
            return Ok(());
        };
        for candidate in self
            .ctx
            .store
            .list_idle_sandbox_candidates(idle_backstop)
            .await?
        {
            report.idle_pause_attempts += 1;
            if let Err(error) = record_idle_pause(
                &self.ctx,
                &candidate.thread_key,
                &candidate.execution_id,
                &candidate.sandbox_id,
                candidate.idle_timeout,
            )
            .await
            {
                report.failed_idle_pauses += 1;
                warn!(
                    thread_key = %candidate.thread_key,
                    execution_id = %candidate.execution_id,
                    sandbox_id = %candidate.sandbox_id,
                    %error,
                    "session sandbox cleanup worker failed to pause idle sandbox"
                );
            }
        }
        Ok(())
    }
}
