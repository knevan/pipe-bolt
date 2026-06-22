use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pipe_bolt_core::forwarder::{
    ForwardDeliveryOutcome, ForwardDeliveryStatus, ForwardFailureReason,
};
use pipe_bolt_domain::ProjectId;
use pipe_bolt_storage::model::{NewSinkDeliveryOutcome, SinkDeliveryStatus};
use pipe_bolt_storage::postgres::PostgresStorage;
use thiserror::Error;
use tokio::sync::{Mutex, mpsc};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::{Instant, timeout};

const DEFAULT_QUEUE_CAPACITY: usize = 4096;
const DEFAULT_WORKER_COUNT: usize = 2;
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PersistenceWriterSettings {
    pub queue_capacity: usize,
    pub worker_count: usize,
    pub write_timeout: Duration,
    pub shutdown_drain_timeout: Duration,
}

impl Default for PersistenceWriterSettings {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            worker_count: DEFAULT_WORKER_COUNT,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            shutdown_drain_timeout: DEFAULT_SHUTDOWN_DRAIN_TIMEOUT,
        }
    }
}

impl PersistenceWriterSettings {
    pub fn validate(&self) -> Result<(), PersistenceWriterError> {
        if self.queue_capacity == 0 {
            return Err(PersistenceWriterError::InvalidConfig {
                reason: "persistence writer queue_capacity must be greater than zero",
            });
        }
        if self.worker_count == 0 {
            return Err(PersistenceWriterError::InvalidConfig {
                reason: "persistence writer worker_count must be greater than zero",
            });
        }
        if self.write_timeout.is_zero() || self.shutdown_drain_timeout.is_zero() {
            return Err(PersistenceWriterError::InvalidConfig {
                reason: "persistence writer timeouts must be greater than zero",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
pub struct PersistenceWriterStatsSnapshot {
    pub enqueued_total: u64,
    pub queue_full_total: u64,
    pub queue_closed_total: u64,
    pub write_succeeded_total: u64,
    pub write_failed_total: u64,
    pub write_timeout_total: u64,
}

#[derive(Debug, Default)]
pub struct PersistenceWriterStats {
    enqueued_total: AtomicU64,
    queue_full_total: AtomicU64,
    queue_closed_total: AtomicU64,
    write_succeeded_total: AtomicU64,
    write_failed_total: AtomicU64,
    write_timeout_total: AtomicU64,
}

impl PersistenceWriterStats {
    pub fn snapshot(&self) -> PersistenceWriterStatsSnapshot {
        PersistenceWriterStatsSnapshot {
            enqueued_total: self.enqueued_total.load(Ordering::Relaxed),
            queue_full_total: self.queue_full_total.load(Ordering::Relaxed),
            queue_closed_total: self.queue_closed_total.load(Ordering::Relaxed),
            write_succeeded_total: self.write_succeeded_total.load(Ordering::Relaxed),
            write_failed_total: self.write_failed_total.load(Ordering::Relaxed),
            write_timeout_total: self.write_timeout_total.load(Ordering::Relaxed),
        }
    }

    fn record_enqueued(&self) {
        self.enqueued_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_queue_full(&self) {
        self.queue_full_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_queue_closed(&self) {
        self.queue_closed_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_write_succeeded(&self) {
        self.write_succeeded_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_write_failed(&self) {
        self.write_failed_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_write_timeout(&self) {
        self.write_timeout_total.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Error)]
pub enum PersistenceWriterError {
    #[error("invalid persistence writer config: {reason}")]
    InvalidConfig { reason: &'static str },

    #[error("persistence writer worker join failed")]
    Join(#[source] JoinError),

    #[error("persistence writer shutdown drain timed out")]
    DrainTimeout,
}

#[derive(Clone)]
pub struct PersistenceWriterHandle {
    project_id: ProjectId,
    tx: mpsc::Sender<NewSinkDeliveryOutcome>,
    stats: Arc<PersistenceWriterStats>,
}

impl PersistenceWriterHandle {
    pub fn try_record_forward_outcome(&self, outcome: &ForwardDeliveryOutcome) {
        let outcome = NewSinkDeliveryOutcome {
            project_id: self.project_id.clone(),
            event_id: outcome.event_id.clone(),
            sink_id: outcome.sink_id.clone(),
            status: map_delivery_status(&outcome.status),
            correlation_id: None,
            duration_ms: None,
            attempt: 1,
        };

        match self.tx.try_send(outcome) {
            Ok(()) => self.stats.record_enqueued(),
            Err(mpsc::error::TrySendError::Full(outcome)) => {
                self.stats.record_queue_full();
                tracing::warn!(
                    event_id = %outcome.event_id,
                    sink_id = %outcome.sink_id,
                    "delivery outcome persistence queue full"
                );
            }
            Err(mpsc::error::TrySendError::Closed(outcome)) => {
                self.stats.record_queue_closed();
                tracing::warn!(
                    event_id = %outcome.event_id,
                    sink_id = %outcome.sink_id,
                    "delivery outcome persistence queue closed"
                );
            }
        }
    }

    pub fn stats(&self) -> PersistenceWriterStatsSnapshot {
        self.stats.snapshot()
    }
}

pub struct RuntimePersistenceWriter {
    handle: PersistenceWriterHandle,
    workers: Vec<JoinHandle<()>>,
    drain_timeout: Duration,
}

impl RuntimePersistenceWriter {
    pub fn spawn(
        project_id: ProjectId,
        storage: Arc<PostgresStorage>,
        settings: PersistenceWriterSettings,
    ) -> Result<Self, PersistenceWriterError> {
        settings.validate()?;

        let (tx, rx) = mpsc::channel(settings.queue_capacity);
        let rx = Arc::new(Mutex::new(rx));
        let stats = Arc::new(PersistenceWriterStats::default());
        let mut workers = Vec::with_capacity(settings.worker_count);

        for worker_index in 0..settings.worker_count {
            workers.push(tokio::spawn(persistence_worker(
                worker_index,
                Arc::clone(&storage),
                Arc::clone(&rx),
                Arc::clone(&stats),
                settings.write_timeout,
            )));
        }

        Ok(Self {
            handle: PersistenceWriterHandle {
                project_id,
                tx,
                stats,
            },
            workers,
            drain_timeout: settings.shutdown_drain_timeout,
        })
    }

    pub fn handle(&self) -> PersistenceWriterHandle {
        self.handle.clone()
    }

    pub async fn shutdown(self) -> Result<(), PersistenceWriterError> {
        drop(self.handle);

        let deadline = Instant::now() + self.drain_timeout;
        for mut worker in self.workers {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                worker.abort();
                return Err(PersistenceWriterError::DrainTimeout);
            }

            match timeout(remaining, &mut worker).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(PersistenceWriterError::Join(error)),
                Err(_) => {
                    worker.abort();
                    return Err(PersistenceWriterError::DrainTimeout);
                }
            }
        }

        Ok(())
    }
}

async fn persistence_worker(
    worker_index: usize,
    storage: Arc<PostgresStorage>,
    rx: Arc<Mutex<mpsc::Receiver<NewSinkDeliveryOutcome>>>,
    stats: Arc<PersistenceWriterStats>,
    write_timeout: Duration,
) {
    loop {
        let outcome = {
            let mut rx = rx.lock().await;
            rx.recv().await
        };

        let Some(outcome) = outcome else {
            break;
        };

        let event_id = outcome.event_id.clone();
        let sink_id = outcome.sink_id.clone();
        match timeout(write_timeout, storage.record_sink_delivery_outcome(outcome)).await {
            Ok(Ok(_delivery_id)) => stats.record_write_succeeded(),
            Ok(Err(error)) => {
                stats.record_write_failed();
                tracing::warn!(
                    worker_index,
                    event_id = %event_id,
                    sink_id = %sink_id,
                    error = %error,
                    "delivery outcome persistence write failed"
                );
            }
            Err(_) => {
                stats.record_write_timeout();
                tracing::warn!(
                    worker_index,
                    event_id = %event_id,
                    sink_id = %sink_id,
                    "delivery outcome persistence write timed out"
                );
            }
        }
    }
}

fn map_delivery_status(status: &ForwardDeliveryStatus) -> SinkDeliveryStatus {
    match status {
        ForwardDeliveryStatus::Delivered {
            http_status,
            response_body_bytes,
        } => SinkDeliveryStatus::Delivered {
            http_status: *http_status,
            response_body_bytes: *response_body_bytes,
        },
        ForwardDeliveryStatus::HttpRejected {
            http_status,
            response_body_bytes,
        } => SinkDeliveryStatus::HttpRejected {
            http_status: *http_status,
            response_body_bytes: *response_body_bytes,
        },
        ForwardDeliveryStatus::TimedOut => SinkDeliveryStatus::TimedOut,
        ForwardDeliveryStatus::ResponseTooLarge { max } => {
            SinkDeliveryStatus::ResponseTooLarge { max: *max }
        }
        ForwardDeliveryStatus::Failed { reason } => SinkDeliveryStatus::Failed {
            reason: forward_failure_reason_name(*reason).to_owned(),
        },
    }
}

fn forward_failure_reason_name(reason: ForwardFailureReason) -> &'static str {
    match reason {
        ForwardFailureReason::RequestFailed => "request_failed",
        ForwardFailureReason::ResponseReadFailed => "response_read_failed",
        ForwardFailureReason::WorkerJoinFailed => "worker_join_failed",
        ForwardFailureReason::OutcomeReceiverClosed => "outcome_receiver_closed",
    }
}
