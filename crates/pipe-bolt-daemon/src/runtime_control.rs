use std::sync::Arc;

use async_trait::async_trait;
use pipe_bolt_api::dto::{
    ForwarderCountersResponse, PersistenceWriterCountersResponse, RuntimeCountersResponse,
    RuntimeLifecycleState, RuntimePipelineCountersResponse, RuntimeReloadResponse,
    RuntimeStatusResponse,
};
use pipe_bolt_api::{RuntimeControl, RuntimeControlError};
use pipe_bolt_domain::{ProjectConfig, ProjectId};
use pipe_bolt_storage::postgres::PostgresStorage;
use time::OffsetDateTime;
use tokio::sync::Mutex;

use crate::runtime::{ProjectRuntime, RuntimePersistence, RuntimeSettings};

pub struct RuntimeSupervisor {
    project_id: ProjectId,
    storage: Arc<PostgresStorage>,
    runtime_settings: RuntimeSettings,
    persistence: RuntimePersistence,
    state: Mutex<RuntimeState>,
    reload_lock: Mutex<()>,
}

impl RuntimeSupervisor {
    pub fn new(
        initial_config: ProjectConfig,
        runtime: ProjectRuntime,
        runtime_settings: RuntimeSettings,
        persistence: RuntimePersistence,
        storage: Arc<PostgresStorage>,
    ) -> Self {
        Self {
            project_id: initial_config.id.clone(),
            storage,
            runtime_settings,
            persistence,
            state: Mutex::new(RuntimeState {
                phase: RuntimeLifecycleState::Running,
                slot: Some(RuntimeSlot {
                    config: initial_config,
                    runtime,
                    started_at: OffsetDateTime::now_utc(),
                }),
                last_reload_at: None,
                last_reload_error: None,
            }),
            reload_lock: Mutex::new(()),
        }
    }

    pub async fn shutdown(&self) -> Result<(), crate::runtime::RuntimeError> {
        let slot = {
            let mut state = self.state.lock().await;
            state.phase = RuntimeLifecycleState::Stopped;
            state.slot.take()
        };

        if let Some(slot) = slot {
            slot.runtime.shutdown().await?;
        }

        Ok(())
    }

    fn ensure_project(&self, project_id: &ProjectId) -> Result<(), RuntimeControlError> {
        if *project_id != self.project_id {
            return Err(RuntimeControlError::ProjectNotManaged {
                project_id: project_id.to_string(),
            });
        }

        Ok(())
    }
}

#[async_trait]
impl RuntimeControl for RuntimeSupervisor {
    async fn status(
        &self,
        project_id: &ProjectId,
    ) -> Result<RuntimeStatusResponse, RuntimeControlError> {
        self.ensure_project(project_id)?;

        let state = self.state.lock().await;
        Ok(status_from_state(&self.project_id, &state))
    }

    async fn reload(
        &self,
        project_id: &ProjectId,
        reason: Option<String>,
    ) -> Result<RuntimeReloadResponse, RuntimeControlError> {
        self.ensure_project(project_id)?;

        let reload_guard = self
            .reload_lock
            .try_lock()
            .map_err(|_| RuntimeControlError::ReloadInProgress)?;
        let _reload_guard = reload_guard;

        let next_config = self
            .storage
            .load_project_config(project_id)
            .await
            .map_err(|error| RuntimeControlError::Storage {
                reason: error.to_string(),
            })?
            .ok_or_else(|| RuntimeControlError::ProjectNotManaged {
                project_id: project_id.to_string(),
            })?;

        ProjectRuntime::validate_config(&next_config).map_err(|error| {
            RuntimeControlError::InvalidConfig {
                reason: error.to_string(),
            }
        })?;

        let old_slot = {
            let mut state = self.state.lock().await;
            state.phase = RuntimeLifecycleState::Reloading;
            state
                .slot
                .take()
                .ok_or_else(|| RuntimeControlError::RuntimeUnavailable {
                    reason: "runtime is not running".to_owned(),
                })?
        };

        let previous_version = old_slot.config.version;
        let old_shutdown_error = old_slot
            .runtime
            .shutdown()
            .await
            .err()
            .map(|error| error.to_string());

        match ProjectRuntime::start(
            next_config.clone(),
            self.runtime_settings.clone(),
            Some(self.persistence.clone()),
        )
        .await
        {
            Ok(runtime) => {
                let reloaded_at = OffsetDateTime::now_utc();
                let active_version = next_config.version;
                let mut state = self.state.lock().await;
                state.phase = RuntimeLifecycleState::Running;
                state.slot = Some(RuntimeSlot {
                    config: next_config,
                    runtime,
                    started_at: reloaded_at,
                });
                state.last_reload_at = Some(reloaded_at);
                state.last_reload_error = old_shutdown_error.clone();

                tracing::info!(
                    project_id = %self.project_id,
                    previous_version,
                    active_version,
                    reason = reason.as_deref().unwrap_or(""),
                    "project runtime reloaded"
                );

                Ok(RuntimeReloadResponse {
                    project_id: self.project_id.clone(),
                    previous_version,
                    active_version,
                    reloaded_at,
                    old_runtime_shutdown_error: old_shutdown_error,
                })
            }
            Err(start_error) => {
                let rollback_message = match ProjectRuntime::start(
                    old_slot.config.clone(),
                    self.runtime_settings.clone(),
                    Some(self.persistence.clone()),
                )
                .await
                {
                    Ok(runtime) => {
                        let mut state = self.state.lock().await;
                        state.phase = RuntimeLifecycleState::Running;
                        state.slot = Some(RuntimeSlot {
                            config: old_slot.config,
                            runtime,
                            started_at: OffsetDateTime::now_utc(),
                        });
                        "rollback succeeded".to_owned()
                    }
                    Err(rollback_error) => {
                        let mut state = self.state.lock().await;
                        state.phase = RuntimeLifecycleState::Stopped;
                        state.slot = None;
                        format!("rollback failed: {rollback_error}")
                    }
                };

                let message = format!("reload failed: {start_error}; {rollback_message}");
                let mut state = self.state.lock().await;
                state.last_reload_at = Some(OffsetDateTime::now_utc());
                state.last_reload_error = Some(message.clone());

                tracing::warn!(project_id = %self.project_id, error = %message, "project runtime reload failed");

                Err(RuntimeControlError::StartFailed { reason: message })
            }
        }
    }
}

struct RuntimeState {
    phase: RuntimeLifecycleState,
    slot: Option<RuntimeSlot>,
    last_reload_at: Option<OffsetDateTime>,
    last_reload_error: Option<String>,
}

struct RuntimeSlot {
    config: ProjectConfig,
    runtime: ProjectRuntime,
    started_at: OffsetDateTime,
}

fn status_from_state(project_id: &ProjectId, state: &RuntimeState) -> RuntimeStatusResponse {
    let Some(slot) = &state.slot else {
        return RuntimeStatusResponse {
            project_id: project_id.clone(),
            state: state.phase,
            active_version: None,
            started_at: None,
            last_reload_at: state.last_reload_at,
            last_reload_error: state.last_reload_error.clone(),
            counters: RuntimeCountersResponse::default(),
        };
    };

    RuntimeStatusResponse {
        project_id: project_id.clone(),
        state: state.phase,
        active_version: Some(slot.config.version),
        started_at: Some(slot.started_at),
        last_reload_at: state.last_reload_at,
        last_reload_error: state.last_reload_error.clone(),
        counters: counters_from_runtime(&slot.runtime),
    }
}

fn counters_from_runtime(runtime: &ProjectRuntime) -> RuntimeCountersResponse {
    let pipeline = runtime.runtime_stats();
    let forwarder = runtime.forwarder_stats();

    RuntimeCountersResponse {
        pipeline: RuntimePipelineCountersResponse {
            normalized_total: pipeline.normalized_total,
            matched_rule_total: pipeline.matched_rule_total,
            action_intent_total: pipeline.action_intent_total,
            dispatch_failed_total: pipeline.dispatch_failed_total,
            realtime_event_published_total: pipeline.realtime_event_published_total,
            realtime_event_no_receiver_total: pipeline.realtime_event_no_receiver_total,
            forward_outcome_total: pipeline.forward_outcome_total,
            delivery_outcome_persist_failed_total: pipeline.delivery_outcome_persist_failed_total,
        },
        forwarder: ForwarderCountersResponse {
            accepted_total: forwarder.accepted_total,
            backpressure_total: forwarder.backpressure_total,
            delivered_total: forwarder.delivered_total,
            rejected_total: forwarder.rejected_total,
            timed_out_total: forwarder.timed_out_total,
            failed_total: forwarder.failed_total,
            response_too_large_total: forwarder.response_too_large_total,
            outcome_dropped_total: forwarder.outcome_dropped_total,
        },
        persistence_writer: runtime.persistence_writer_stats().map(|stats| {
            PersistenceWriterCountersResponse {
                enqueued_total: stats.enqueued_total,
                queue_full_total: stats.queue_full_total,
                queue_closed_total: stats.queue_closed_total,
                write_succeeded_total: stats.write_succeeded_total,
                write_failed_total: stats.write_failed_total,
                write_timeout_total: stats.write_timeout_total,
            }
        }),
    }
}
