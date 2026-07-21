use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pipe_bolt_core::command::{CommandRenderContext, CommandRenderError, CommandTemplateRenderer};
use pipe_bolt_core::config::{MqttClientConfig, MqttReconnectConfig, MqttTlsMode};
use pipe_bolt_core::dispatcher::action::{
    ActionDispatcher, DispatchLimits, RealtimeEventSink, RealtimePublishReceipt,
};
use pipe_bolt_core::error::{DispatchError, MqttEngineError};
use pipe_bolt_core::forwarder::{
    BoundedHttpForwarder, EgressPolicy, ForwardDeliveryOutcome, ForwardLimits, ForwarderStats,
    ForwarderStatsSnapshot,
};
use pipe_bolt_core::message::envelope::MqttMessage;
use pipe_bolt_core::mqtt::engine::{MqttEngine, MqttHandle};
use pipe_bolt_core::pipeline::normalize_routed_message;
use pipe_bolt_core::pipeline::normalizer::{EventNormalizer, NormalizerLimits};
use pipe_bolt_core::pipeline::router::ConfigRouteMatcher;
use pipe_bolt_core::router::matcher::MqttRouter;
use pipe_bolt_core::rule::rules::{RuleEngine, RuleEngineLimits};
use pipe_bolt_domain::{
    ActionIntentTemplate, BackpressurePolicy, BrokerConnectionConfig, BrokerId,
    CommandExecutionRequest, CommandTemplate, CommandTemplateId, MqttQos, NormalizedEvent,
    PayloadSchemaMapping, ProjectConfig, ProjectId, RenderedCommand, SinkKind, TlsMode,
    TopicRouteConfig,
};
use pipe_bolt_storage::postgres::PostgresStorage;
use rumqttc::QoS;
use thiserror::Error;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::timeout;

use crate::persistence_writer::{
    PersistenceWriterError, PersistenceWriterHandle, PersistenceWriterSettings,
    PersistenceWriterStatsSnapshot, RuntimePersistenceWriter,
};

const DEFAULT_REALTIME_EVENT_CAPACITY: usize = 1024;
const DEFAULT_COMMAND_EXECUTION_CAPACITY: usize = 256;
const DEFAULT_WORKER_JOIN_TIMEOUT: Duration = Duration::from_secs(10);

type RuntimeDispatcher = ActionDispatcher<RuntimeRealtimeSink, BoundedHttpForwarder>;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RuntimeSettings {
    pub forward_limits: ForwardLimits,
    pub egress_policy: EgressPolicy,
    pub normalizer_limits: NormalizerLimits,
    pub rule_limits: RuleEngineLimits,
    pub dispatch_limits: DispatchLimits,
    pub realtime_event_capacity: usize,
    pub command_queue_capacity: usize,
    pub realtime_bridge_bind_addr: SocketAddr,
    pub worker_join_timeout: Duration,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            forward_limits: ForwardLimits::default(),
            egress_policy: EgressPolicy::default(),
            normalizer_limits: NormalizerLimits::default(),
            rule_limits: RuleEngineLimits::default(),
            dispatch_limits: DispatchLimits::default(),
            realtime_event_capacity: DEFAULT_REALTIME_EVENT_CAPACITY,
            command_queue_capacity: DEFAULT_COMMAND_EXECUTION_CAPACITY,
            realtime_bridge_bind_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
            worker_join_timeout: DEFAULT_WORKER_JOIN_TIMEOUT,
        }
    }
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("project is disabled")]
    ProjectDisabled,

    #[error("invalid runtime config: {0}")]
    InvalidConfig(&'static str),

    #[error(
        "multiple enabled brokers are not supported by this runtime slice: {count} enabled brokers"
    )]
    MultipleEnabledBrokersUnsupported { count: usize },

    #[error("duplicate {collection} id '{id}'")]
    DuplicateId {
        collection: &'static str,
        id: String,
    },

    #[error("no enabled broker configured")]
    NoEnabledBroker,

    #[error("broker '{broker_id}' has no enabled route")]
    NoEnabledRoutesForBroker { broker_id: String },

    #[error("enabled route '{route_id}' references an unknown or disabled broker '{broker_id}'")]
    RouteReferencesUnavailableBroker { route_id: String, broker_id: String },

    #[error("enabled route '{route_id}' references unknown schema mapping '{schema_mapping_id}'")]
    RouteReferencesUnknownSchemaMapping {
        route_id: String,
        schema_mapping_id: String,
    },

    #[error("route '{route_id}' uses unsupported backpressure policy '{policy}'")]
    UnsupportedBackpressurePolicy {
        route_id: String,
        policy: &'static str,
    },

    #[error(
        "enabled command template '{template_id}' references an unknown or disabled broker '{broker_id}'"
    )]
    CommandTemplateReferencesUnavailableBroker {
        template_id: String,
        broker_id: String,
    },

    #[error("command template '{template_id}' was not found")]
    CommandTemplateNotFound { template_id: String },

    #[error("command template '{template_id}' is disabled")]
    CommandTemplateDisabled { template_id: String },

    #[error("broker '{broker_id}' is not running")]
    CommandBrokerUnavailable { broker_id: String },

    #[error("command template '{template_id}' render failed: {reason}")]
    CommandTemplateRender { template_id: String, reason: String },

    #[error("command payload is too large: max {max} bytes, got {actual} bytes")]
    CommandPayloadTooLarge { max: usize, actual: usize },

    #[error("enabled rule '{rule_id}' references unknown sink '{sink_id}'")]
    RuleReferencesUnknownSink { rule_id: String, sink_id: String },

    #[error("enabled rule '{rule_id}' references disabled sink '{sink_id}'")]
    RuleReferencesDisabledSink { rule_id: String, sink_id: String },

    #[error("enabled rule '{rule_id}' references unsupported sink '{sink_id}'")]
    RuleReferencesUnsupportedSink { rule_id: String, sink_id: String },

    #[error("domain config error: {0}")]
    Domain(#[from] pipe_bolt_domain::DomainError),

    #[error("MQTT runtime error: {0}")]
    Mqtt(#[from] MqttEngineError),

    #[error("dispatch runtime error: {0}")]
    Dispatch(#[from] DispatchError),

    #[error("worker '{name}' join timed out")]
    WorkerJoinTimeout { name: &'static str },

    #[error("worker '{name}' join failed")]
    WorkerJoin {
        name: &'static str,
        #[source]
        source: JoinError,
    },

    #[error("persistence writer error: {0}")]
    PersistenceWriter(#[from] PersistenceWriterError),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
pub struct RuntimeStatsSnapshot {
    pub normalized_total: u64,
    pub matched_rule_total: u64,
    pub action_intent_total: u64,
    pub dispatch_failed_total: u64,
    pub realtime_event_published_total: u64,
    pub realtime_event_no_receiver_total: u64,
    pub forward_outcome_total: u64,
    pub delivery_outcome_persist_failed_total: u64,
    pub command_queued_total: u64,
    pub command_queue_full_total: u64,
    pub command_queue_closed_total: u64,
    pub command_render_failed_total: u64,
    pub command_publish_enqueue_failed_total: u64,
}

#[derive(Debug, Default)]
pub struct RuntimeStats {
    normalized_total: AtomicU64,
    matched_rule_total: AtomicU64,
    action_intent_total: AtomicU64,
    dispatch_failed_total: AtomicU64,
    realtime_event_published_total: AtomicU64,
    realtime_event_no_receiver_total: AtomicU64,
    forward_outcome_total: AtomicU64,
    delivery_outcome_persist_failed_total: AtomicU64,
    command_queued_total: AtomicU64,
    command_queue_full_total: AtomicU64,
    command_queue_closed_total: AtomicU64,
    command_render_failed_total: AtomicU64,
    command_publish_enqueue_failed_total: AtomicU64,
}

impl RuntimeStats {
    pub fn snapshot(&self) -> RuntimeStatsSnapshot {
        RuntimeStatsSnapshot {
            normalized_total: self.normalized_total.load(Ordering::Relaxed),
            matched_rule_total: self.matched_rule_total.load(Ordering::Relaxed),
            action_intent_total: self.action_intent_total.load(Ordering::Relaxed),
            dispatch_failed_total: self.dispatch_failed_total.load(Ordering::Relaxed),
            realtime_event_published_total: self
                .realtime_event_published_total
                .load(Ordering::Relaxed),
            realtime_event_no_receiver_total: self
                .realtime_event_no_receiver_total
                .load(Ordering::Relaxed),
            forward_outcome_total: self.forward_outcome_total.load(Ordering::Relaxed),
            delivery_outcome_persist_failed_total: self
                .delivery_outcome_persist_failed_total
                .load(Ordering::Relaxed),
            command_queued_total: self.command_queued_total.load(Ordering::Relaxed),
            command_queue_full_total: self.command_queue_full_total.load(Ordering::Relaxed),
            command_queue_closed_total: self.command_queue_closed_total.load(Ordering::Relaxed),
            command_render_failed_total: self.command_render_failed_total.load(Ordering::Relaxed),
            command_publish_enqueue_failed_total: self
                .command_publish_enqueue_failed_total
                .load(Ordering::Relaxed),
        }
    }

    fn record_normalized(&self) {
        self.normalized_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_rule_evaluation(&self, matched_rules: usize, action_intents: usize) {
        self.matched_rule_total
            .fetch_add(saturating_usize_to_u64(matched_rules), Ordering::Relaxed);
        self.action_intent_total
            .fetch_add(saturating_usize_to_u64(action_intents), Ordering::Relaxed);
    }

    fn record_dispatch_failures(&self, failures: usize) {
        self.dispatch_failed_total
            .fetch_add(saturating_usize_to_u64(failures), Ordering::Relaxed);
    }

    fn record_realtime_event_published(&self) {
        self.realtime_event_published_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_realtime_event_no_receiver(&self) {
        self.realtime_event_no_receiver_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_forward_outcome(&self) {
        self.forward_outcome_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_command_queued(&self) {
        self.command_queued_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_command_queue_full(&self) {
        self.command_queue_full_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_command_queue_closed(&self) {
        self.command_queue_closed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_command_render_failed(&self) {
        self.command_render_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_command_publish_enqueue_failed(&self) {
        self.command_publish_enqueue_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum RuntimeCommandQueueError {
    #[error("command execution queue is full")]
    Full,

    #[error("command execution queue is closed")]
    Closed,
}

#[derive(Clone)]
pub struct RuntimeCommandQueueHandle {
    tx: mpsc::Sender<CommandExecutionRequest>,
    stats: Arc<RuntimeStats>,
}

impl RuntimeCommandQueueHandle {
    pub fn try_enqueue(
        &self,
        request: CommandExecutionRequest,
    ) -> Result<(), RuntimeCommandQueueError> {
        self.tx.try_send(request).map_err(|error| match error {
            TrySendError::Full(_) => {
                self.stats.record_command_queue_full();
                RuntimeCommandQueueError::Full
            }
            TrySendError::Closed(_) => {
                self.stats.record_command_queue_closed();
                RuntimeCommandQueueError::Closed
            }
        })?;

        self.stats.record_command_queued();
        Ok(())
    }
}

#[derive(Clone)]
pub struct RuntimePersistence {
    project_id: ProjectId,
    storage: Arc<PostgresStorage>,
    writer_settings: PersistenceWriterSettings,
}

impl RuntimePersistence {
    pub fn new(project_id: ProjectId, storage: Arc<PostgresStorage>) -> Self {
        Self {
            project_id,
            storage,
            writer_settings: PersistenceWriterSettings::default(),
        }
    }

    pub fn with_writer_settings(mut self, writer_settings: PersistenceWriterSettings) -> Self {
        self.writer_settings = writer_settings;
        self
    }
}

#[derive(Clone)]
pub struct RuntimeRealtimeSink {
    tx: broadcast::Sender<NormalizedEvent>,
    stats: Arc<RuntimeStats>,
}

impl RuntimeRealtimeSink {
    fn new(tx: broadcast::Sender<NormalizedEvent>, stats: Arc<RuntimeStats>) -> Self {
        Self { tx, stats }
    }
}

impl RealtimeEventSink for RuntimeRealtimeSink {
    fn try_publish(&self, event: NormalizedEvent) -> Result<RealtimePublishReceipt, DispatchError> {
        match self.tx.send(event) {
            Ok(_) => self.stats.record_realtime_event_published(),
            Err(_) => self.stats.record_realtime_event_no_receiver(),
        }

        Ok(RealtimePublishReceipt { accepted: true })
    }
}

pub struct ProjectRuntime {
    mqtt_engines: Vec<BrokerRuntime>,
    command_queue: RuntimeCommandQueueHandle,
    shutdown_tx: watch::Sender<bool>,
    workers: Vec<RuntimeWorker>,
    stats: Arc<RuntimeStats>,
    forwarder_stats: Arc<ForwarderStats>,
    realtime_tx: broadcast::Sender<NormalizedEvent>,
    worker_join_timeout: Duration,
    persistence_writer: Option<RuntimePersistenceWriter>,
}

pub struct QueuedRuntimeCommand {
    pub command_template_id: CommandTemplateId,
    pub broker_id: BrokerId,
    pub topic: String,
    pub qos: MqttQos,
    pub retain: bool,
    pub payload_size_bytes: u64,
}

struct BrokerRuntime {
    broker_id: BrokerId,
    engine: MqttEngine,
}

trait CommandPublishSink: Send + Sync {
    fn try_enqueue_rendered(&self, command: RenderedCommand) -> Result<(), MqttEngineError>;
}

struct MqttCommandPublishSink {
    handle: MqttHandle,
}

impl CommandPublishSink for MqttCommandPublishSink {
    fn try_enqueue_rendered(&self, command: RenderedCommand) -> Result<(), MqttEngineError> {
        let topic = command.topic().as_str().to_owned();
        let qos = map_qos(command.qos());
        let retain = command.retain();
        self.handle
            .try_enqueue_command(topic, qos, retain, command.into_payload())
    }
}

struct CommandProcessorBroker {
    broker_id: BrokerId,
    sink: Arc<dyn CommandPublishSink>,
}

struct CommandProcessor {
    project_id: ProjectId,
    templates: Arc<Vec<CommandTemplate>>,
    brokers: Arc<Vec<CommandProcessorBroker>>,
    renderer: CommandTemplateRenderer,
    stats: Arc<RuntimeStats>,
}

#[derive(Debug, Error)]
enum RuntimeCommandProcessorError {
    #[error("command request targets project '{actual}', but runtime owns project '{expected}'")]
    ProjectMismatch {
        expected: ProjectId,
        actual: ProjectId,
    },

    #[error("command template '{template_id}' was not found")]
    CommandTemplateNotFound { template_id: CommandTemplateId },

    #[error("command template '{template_id}' is disabled")]
    CommandTemplateDisabled { template_id: CommandTemplateId },

    #[error("command render failed: {source}")]
    Render {
        #[source]
        source: Box<CommandRenderError>,
    },

    #[error("broker '{broker_id}' is not running")]
    BrokerUnavailable { broker_id: BrokerId },

    #[error("MQTT command enqueue failed for broker '{broker_id}': {source}")]
    PublishEnqueue {
        broker_id: BrokerId,
        #[source]
        source: Box<MqttEngineError>,
    },
}

impl ProjectRuntime {
    pub fn validate_config(config: &ProjectConfig) -> Result<(), RuntimeError> {
        validate_runtime_config(config)
    }

    pub async fn start(
        config: ProjectConfig,
        settings: RuntimeSettings,
        persistence: Option<RuntimePersistence>,
    ) -> Result<Self, RuntimeError> {
        validate_runtime_settings(&settings)?;
        validate_runtime_config(&config)?;

        let stats = Arc::new(RuntimeStats::default());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (realtime_tx, _) = broadcast::channel(settings.realtime_event_capacity);
        let (command_queue, command_rx) =
            command_queue_channel(settings.command_queue_capacity, Arc::clone(&stats));
        let realtime_sink = RuntimeRealtimeSink::new(realtime_tx.clone(), Arc::clone(&stats));

        let (forwarder, forward_worker, forward_outcomes) =
            BoundedHttpForwarder::try_channel_with_policy(
                config.sinks.clone(),
                settings.forward_limits,
                settings.egress_policy.clone(),
            )?;
        let forwarder_stats = forwarder.stats();

        let rule_engine = RuleEngine::with_limits(config.rules.clone(), settings.rule_limits)?;
        let dispatcher =
            ActionDispatcher::with_limits(realtime_sink, forwarder, settings.dispatch_limits);
        let schema_mappings = Arc::new(config.schema_mappings.clone());
        let pending_brokers = build_pending_broker_runtimes(
            &config,
            &settings,
            schema_mappings,
            rule_engine,
            dispatcher,
            Arc::clone(&stats),
        )?;

        let mut mqtt_engines = Vec::with_capacity(pending_brokers.len());
        for pending in pending_brokers {
            match MqttEngine::spawn(pending.config, pending.router) {
                Ok(engine) => mqtt_engines.push(BrokerRuntime {
                    broker_id: pending.broker_id,
                    engine,
                }),
                Err(error) => {
                    shutdown_mqtt_engines(mqtt_engines).await;
                    return Err(RuntimeError::from(error));
                }
            }
        }

        let persistence_writer = persistence
            .map(|persistence| {
                RuntimePersistenceWriter::spawn(
                    persistence.project_id,
                    persistence.storage,
                    persistence.writer_settings,
                )
            })
            .transpose()?;

        let persistence_handle = persistence_writer
            .as_ref()
            .map(RuntimePersistenceWriter::handle);
        let command_processor = build_command_processor(&config, &mqtt_engines, Arc::clone(&stats));

        let mut workers = Vec::new();
        workers.push(RuntimeWorker::spawn("forwarder", async move {
            forward_worker.run(shutdown_rx).await;
            Ok(())
        }));
        workers.push(RuntimeWorker::spawn(
            "command-processor",
            run_command_processor(command_rx, shutdown_tx.subscribe(), command_processor),
        ));
        workers.push(RuntimeWorker::spawn(
            "forward-outcome-consumer",
            consume_forward_outcomes(
                forward_outcomes,
                shutdown_tx.subscribe(),
                Arc::clone(&stats),
                persistence_handle,
            ),
        ));

        tracing::info!(
            project_id = %config.id,
            broker_count = mqtt_engines.len(),
            "project runtime started"
        );

        Ok(Self {
            mqtt_engines,
            command_queue,
            shutdown_tx,
            workers,
            stats,
            forwarder_stats,
            realtime_tx,
            worker_join_timeout: settings.worker_join_timeout,
            persistence_writer,
        })
    }

    pub fn runtime_stats(&self) -> RuntimeStatsSnapshot {
        self.stats.snapshot()
    }

    pub fn forwarder_stats(&self) -> ForwarderStatsSnapshot {
        self.forwarder_stats.snapshot()
    }

    pub fn subscribe_realtime_events(&self) -> broadcast::Receiver<NormalizedEvent> {
        self.realtime_tx.subscribe()
    }

    pub fn persistence_writer_stats(&self) -> Option<PersistenceWriterStatsSnapshot> {
        self.persistence_writer
            .as_ref()
            .map(|writer| writer.handle().stats())
    }

    pub fn command_queue_handle(&self) -> RuntimeCommandQueueHandle {
        self.command_queue.clone()
    }

    pub fn execute_command(
        &self,
        config: &ProjectConfig,
        command_template_id: &CommandTemplateId,
        params: &BTreeMap<String, serde_json::Value>,
    ) -> Result<QueuedRuntimeCommand, RuntimeError> {
        let template = config
            .command_templates
            .iter()
            .find(|template| &template.id == command_template_id)
            .ok_or_else(|| RuntimeError::CommandTemplateNotFound {
                template_id: command_template_id.to_string(),
            })?;

        if !template.enabled {
            return Err(RuntimeError::CommandTemplateDisabled {
                template_id: command_template_id.to_string(),
            });
        }

        let rendered = CommandTemplateRenderer::default()
            .render_draft(template, params)
            .map_err(command_render_error_to_runtime)?;
        let engine = self
            .mqtt_engines
            .iter()
            .find(|broker| broker.broker_id == template.broker_id)
            .ok_or_else(|| RuntimeError::CommandBrokerUnavailable {
                broker_id: template.broker_id.to_string(),
            })?;

        let topic = rendered.topic.as_str().to_owned();
        engine.engine.handle().try_enqueue_command(
            topic.clone(),
            map_qos(template.qos),
            template.retain,
            rendered.payload,
        )?;

        Ok(QueuedRuntimeCommand {
            command_template_id: template.id.clone(),
            broker_id: template.broker_id.clone(),
            topic,
            qos: template.qos,
            retain: template.retain,
            payload_size_bytes: rendered.payload_size_bytes,
        })
    }

    pub async fn shutdown(self) -> Result<(), RuntimeError> {
        let Self {
            mqtt_engines,
            command_queue: _,
            shutdown_tx,
            workers,
            worker_join_timeout,
            persistence_writer,
            ..
        } = self;
        let mut first_error = None;

        let _ = shutdown_tx.send(true);

        if let Err(error) = join_workers(workers, worker_join_timeout).await {
            remember_first_error(&mut first_error, error);
        }

        for broker in mqtt_engines {
            if let Err(error) = broker.engine.shutdown().await {
                remember_first_error(&mut first_error, RuntimeError::from(error));
            }
        }

        if let Some(writer) = persistence_writer
            && let Err(error) = writer.shutdown().await
        {
            remember_first_error(&mut first_error, RuntimeError::from(error));
        }

        if let Some(error) = first_error {
            return Err(error);
        }

        tracing::info!("project runtime stopped");
        Ok(())
    }
}

struct RuntimeWorker {
    name: &'static str,
    handle: JoinHandle<Result<(), RuntimeError>>,
}

impl RuntimeWorker {
    fn spawn(
        name: &'static str,
        future: impl Future<Output = Result<(), RuntimeError>> + Send + 'static,
    ) -> Self {
        Self {
            name,
            handle: tokio::spawn(future),
        }
    }
}

struct PendingBrokerRuntime {
    broker_id: BrokerId,
    config: MqttClientConfig,
    router: MqttRouter,
}

fn build_pending_broker_runtimes(
    config: &ProjectConfig,
    settings: &RuntimeSettings,
    schema_mappings: Arc<Vec<PayloadSchemaMapping>>,
    rule_engine: RuleEngine,
    dispatcher: RuntimeDispatcher,
    stats: Arc<RuntimeStats>,
) -> Result<Vec<PendingBrokerRuntime>, RuntimeError> {
    let enabled_brokers = config
        .brokers
        .iter()
        .filter(|broker| broker.enabled)
        .collect::<Vec<_>>();

    let mut pending = Vec::with_capacity(enabled_brokers.len());

    for broker in enabled_brokers {
        let routes = enabled_routes_for_broker(config, &broker.id);
        if routes.is_empty() {
            return Err(RuntimeError::NoEnabledRoutesForBroker {
                broker_id: broker.id.to_string(),
            });
        }

        let mqtt_config = build_mqtt_client_config(broker, &routes)?;
        let matcher = ConfigRouteMatcher::new(config.id.clone(), routes.clone())?;
        let router = build_pipeline_router(
            &routes,
            matcher,
            EventNormalizer::new(settings.normalizer_limits),
            Arc::clone(&schema_mappings),
            rule_engine.clone(),
            dispatcher.clone(),
            Arc::clone(&stats),
        )?;

        pending.push(PendingBrokerRuntime {
            broker_id: broker.id.clone(),
            config: mqtt_config,
            router,
        });
    }

    Ok(pending)
}

fn command_queue_channel(
    capacity: usize,
    stats: Arc<RuntimeStats>,
) -> (
    RuntimeCommandQueueHandle,
    mpsc::Receiver<CommandExecutionRequest>,
) {
    let (tx, rx) = mpsc::channel(capacity);
    (RuntimeCommandQueueHandle { tx, stats }, rx)
}

fn build_command_processor(
    config: &ProjectConfig,
    mqtt_engines: &[BrokerRuntime],
    stats: Arc<RuntimeStats>,
) -> CommandProcessor {
    let brokers = mqtt_engines
        .iter()
        .map(|broker| {
            let sink: Arc<dyn CommandPublishSink> = Arc::new(MqttCommandPublishSink {
                handle: broker.engine.handle(),
            });

            CommandProcessorBroker {
                broker_id: broker.broker_id.clone(),
                sink,
            }
        })
        .collect::<Vec<_>>();

    CommandProcessor {
        project_id: config.id.clone(),
        templates: Arc::new(config.command_templates.clone()),
        brokers: Arc::new(brokers),
        renderer: CommandTemplateRenderer::default(),
        stats,
    }
}

async fn run_command_processor(
    mut command_rx: mpsc::Receiver<CommandExecutionRequest>,
    mut shutdown_rx: watch::Receiver<bool>,
    processor: CommandProcessor,
) -> Result<(), RuntimeError> {
    loop {
        tokio::select! {
            biased;

            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    command_rx.close();
                    break;
                }
            }

            request = command_rx.recv() => {
                let Some(request) = request else {
                    break;
                };

                process_and_record_command_request(&processor, &request);
            }
        }
    }

    while let Some(request) = command_rx.recv().await {
        process_and_record_command_request(&processor, &request);
    }

    Ok(())
}

fn process_and_record_command_request(
    processor: &CommandProcessor,
    request: &CommandExecutionRequest,
) {
    match process_command_request(processor, request) {
        Ok(()) => {
            tracing::info!(
                project_id = %request.project_id,
                command_execution_id = %request.command_execution_id,
                command_template_id = %request.command_template_id,
                correlation_id = %request.correlation_id,
                "command request rendered and enqueued to MQTT publish queue"
            );
        }
        Err(error) => {
            record_command_processor_error(&processor.stats, &error);
            tracing::warn!(
                project_id = %request.project_id,
                command_execution_id = %request.command_execution_id,
                command_template_id = %request.command_template_id,
                correlation_id = %request.correlation_id,
                error = %error,
                "command processor failed"
            );
        }
    }
}

fn process_command_request(
    processor: &CommandProcessor,
    request: &CommandExecutionRequest,
) -> Result<(), RuntimeCommandProcessorError> {
    if request.project_id != processor.project_id {
        return Err(RuntimeCommandProcessorError::ProjectMismatch {
            expected: processor.project_id.clone(),
            actual: request.project_id.clone(),
        });
    }

    let template = processor
        .templates
        .iter()
        .find(|template| template.id == request.command_template_id)
        .ok_or_else(|| RuntimeCommandProcessorError::CommandTemplateNotFound {
            template_id: request.command_template_id.clone(),
        })?;

    if !template.enabled {
        return Err(RuntimeCommandProcessorError::CommandTemplateDisabled {
            template_id: template.id.clone(),
        });
    }

    let rendered = processor
        .renderer
        .render(
            CommandRenderContext {
                project_id: request.project_id.clone(),
                command_execution_id: request.command_execution_id.clone(),
                correlation_id: request.correlation_id.clone(),
            },
            template,
            &request.params,
        )
        .map_err(|source| RuntimeCommandProcessorError::Render {
            source: Box::new(source),
        })?;

    let broker_id = rendered.broker_id().clone();
    let broker = processor
        .brokers
        .iter()
        .find(|broker| broker.broker_id == broker_id)
        .ok_or_else(|| RuntimeCommandProcessorError::BrokerUnavailable {
            broker_id: broker_id.clone(),
        })?;

    broker
        .sink
        .try_enqueue_rendered(rendered)
        .map_err(|source| RuntimeCommandProcessorError::PublishEnqueue {
            broker_id,
            source: Box::new(source),
        })
}

fn record_command_processor_error(stats: &RuntimeStats, error: &RuntimeCommandProcessorError) {
    match error {
        RuntimeCommandProcessorError::Render { .. } => stats.record_command_render_failed(),
        RuntimeCommandProcessorError::PublishEnqueue { .. } => {
            stats.record_command_publish_enqueue_failed();
        }
        RuntimeCommandProcessorError::ProjectMismatch { .. }
        | RuntimeCommandProcessorError::CommandTemplateNotFound { .. }
        | RuntimeCommandProcessorError::CommandTemplateDisabled { .. }
        | RuntimeCommandProcessorError::BrokerUnavailable { .. } => {}
    }
}

fn enabled_routes_for_broker(
    config: &ProjectConfig,
    broker_id: &BrokerId,
) -> Vec<TopicRouteConfig> {
    config
        .routes
        .iter()
        .filter(|route| route.enabled && &route.broker_id == broker_id)
        .cloned()
        .collect()
}

fn build_pipeline_router(
    routes: &[TopicRouteConfig],
    matcher: ConfigRouteMatcher,
    normalizer: EventNormalizer,
    schema_mappings: Arc<Vec<PayloadSchemaMapping>>,
    rule_engine: RuleEngine,
    dispatcher: RuntimeDispatcher,
    stats: Arc<RuntimeStats>,
) -> Result<MqttRouter, RuntimeError> {
    let mut router = MqttRouter::new();
    let mut filters = HashSet::with_capacity(routes.len());

    for route in routes {
        let filter = route.topic_filter.as_str().to_owned();
        if !filters.insert(filter.clone()) {
            continue;
        }

        let matcher = matcher.clone();
        let normalizer = normalizer.clone();
        let schema_mappings = Arc::clone(&schema_mappings);
        let rule_engine = rule_engine.clone();
        let dispatcher = dispatcher.clone();
        let stats = Arc::clone(&stats);

        router = router.route(filter, move |message: MqttMessage, _params| {
            let matcher = matcher.clone();
            let normalizer = normalizer.clone();
            let schema_mappings = Arc::clone(&schema_mappings);
            let rule_engine = rule_engine.clone();
            let dispatcher = dispatcher.clone();
            let stats = Arc::clone(&stats);

            async move {
                handle_pipeline_message(
                    &matcher,
                    &normalizer,
                    schema_mappings.as_ref().as_slice(),
                    &rule_engine,
                    &dispatcher,
                    &stats,
                    &message,
                )
            }
        })?;
    }

    Ok(router)
}

fn handle_pipeline_message(
    matcher: &ConfigRouteMatcher,
    normalizer: &EventNormalizer,
    schema_mappings: &[PayloadSchemaMapping],
    rule_engine: &RuleEngine,
    dispatcher: &RuntimeDispatcher,
    stats: &RuntimeStats,
    message: &MqttMessage,
) -> Result<(), MqttEngineError> {
    let Some(event) = normalize_routed_message(matcher, normalizer, schema_mappings, message)?
    else {
        tracing::debug!(
            topic = message.topic(),
            "matched handler could not normalize route"
        );
        return Ok(());
    };

    stats.record_normalized();

    let evaluation = rule_engine.evaluate(&event)?;
    stats.record_rule_evaluation(evaluation.matched_rules.len(), evaluation.intents.len());

    let dispatch = dispatcher.dispatch(&event, &evaluation.intents)?;
    stats.record_dispatch_failures(dispatch.failed.len());

    if !dispatch.failed.is_empty() {
        tracing::warn!(
            event_id = %event.id,
            failed_actions = dispatch.failed.len(),
            "one or more action intents failed at dispatch boundary"
        );
    }

    Ok(())
}

fn build_mqtt_client_config(
    broker: &BrokerConnectionConfig,
    routes: &[TopicRouteConfig],
) -> Result<MqttClientConfig, RuntimeError> {
    let mut config =
        MqttClientConfig::new(broker.client_id.as_str(), broker.host.as_str(), broker.port)
            .with_keep_alive(broker.keep_alive)
            .with_clean_session(broker.clean_session)
            .with_tls(map_tls_mode(broker.tls))
            .with_reconnect(MqttReconnectConfig {
                min_delay: broker.reconnect.min_delay,
                max_delay: broker.reconnect.max_delay,
            });

    if let Some(credentials) = &broker.credentials {
        config = config.with_credentials(
            credentials.username.as_str(),
            credentials.password.expose_secret(),
        );
    }

    for (topic, qos) in merged_subscriptions(routes) {
        config = config.with_subscription(topic, qos);
    }

    config.validate()?;
    Ok(config)
}

fn merged_subscriptions(routes: &[TopicRouteConfig]) -> Vec<(String, QoS)> {
    let mut subscriptions = BTreeMap::<String, QoS>::new();

    for route in routes {
        let topic = route.topic_filter.as_str().to_owned();
        let qos = map_qos(route.qos);

        subscriptions
            .entry(topic)
            .and_modify(|existing| *existing = max_qos(*existing, qos))
            .or_insert(qos);
    }

    subscriptions.into_iter().collect()
}

fn map_tls_mode(tls: TlsMode) -> MqttTlsMode {
    match tls {
        TlsMode::Disabled => MqttTlsMode::Disabled,
        TlsMode::NativeRoots => MqttTlsMode::EnabledWithNativeRoot,
    }
}

fn map_qos(qos: MqttQos) -> QoS {
    match qos {
        MqttQos::AtMostOnce => QoS::AtMostOnce,
        MqttQos::AtLeastOnce => QoS::AtLeastOnce,
        MqttQos::ExactlyOnce => QoS::ExactlyOnce,
    }
}

fn max_qos(left: QoS, right: QoS) -> QoS {
    if qos_rank(right) > qos_rank(left) {
        right
    } else {
        left
    }
}

fn qos_rank(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

fn command_render_error_to_runtime(error: CommandRenderError) -> RuntimeError {
    if let CommandRenderError::PayloadTooLarge { max, actual, .. } = error {
        return RuntimeError::CommandPayloadTooLarge { max, actual };
    }

    RuntimeError::CommandTemplateRender {
        template_id: error.template_id().to_string(),
        reason: error.to_string(),
    }
}

async fn consume_forward_outcomes(
    mut outcomes: mpsc::Receiver<ForwardDeliveryOutcome>,
    mut shutdown_rx: watch::Receiver<bool>,
    stats: Arc<RuntimeStats>,
    persistence: Option<PersistenceWriterHandle>,
) -> Result<(), RuntimeError> {
    loop {
        tokio::select! {
            biased;

            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    outcomes.close();
                    break;
                }
            }

            outcome = outcomes.recv() => {
                let Some(outcome) = outcome else {
                    break;
                };

                record_forward_outcome(&stats, persistence.as_ref(), &outcome);
            }
        }
    }

    while let Some(outcome) = outcomes.recv().await {
        record_forward_outcome(&stats, persistence.as_ref(), &outcome);
    }

    Ok(())
}

fn record_forward_outcome(
    stats: &RuntimeStats,
    persistence: Option<&PersistenceWriterHandle>,
    outcome: &ForwardDeliveryOutcome,
) {
    stats.record_forward_outcome();

    if let Some(persistence) = persistence {
        persistence.try_record_forward_outcome(outcome);
    }

    tracing::info!(
        event_id = %outcome.event_id,
        sink_id = %outcome.sink_id,
        status = ?outcome.status,
        "forward delivery outcome"
    );
}

async fn shutdown_mqtt_engines(mqtt_engines: Vec<BrokerRuntime>) {
    for broker in mqtt_engines {
        if let Err(error) = broker.engine.shutdown().await {
            tracing::warn!(error = %error, "failed to shutdown MQTT engine during startup rollback");
        }
    }
}

async fn join_workers(
    workers: Vec<RuntimeWorker>,
    join_timeout: Duration,
) -> Result<(), RuntimeError> {
    let mut first_error = None;

    for worker in workers {
        let mut handle = worker.handle;

        match timeout(join_timeout, &mut handle).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => remember_first_error(&mut first_error, error),
            Ok(Err(source)) => remember_first_error(
                &mut first_error,
                RuntimeError::WorkerJoin {
                    name: worker.name,
                    source,
                },
            ),
            Err(_) => {
                handle.abort();
                let _ = handle.await;
                remember_first_error(
                    &mut first_error,
                    RuntimeError::WorkerJoinTimeout { name: worker.name },
                );
            }
        }
    }

    if let Some(error) = first_error {
        return Err(error);
    }

    Ok(())
}

fn remember_first_error(first_error: &mut Option<RuntimeError>, error: RuntimeError) {
    if first_error.is_none() {
        *first_error = Some(error);
    }
}

fn validate_runtime_settings(settings: &RuntimeSettings) -> Result<(), RuntimeError> {
    if settings.realtime_event_capacity == 0 {
        return Err(RuntimeError::InvalidConfig(
            "realtime event capacity must be greater than zero",
        ));
    }

    if settings.worker_join_timeout.is_zero() {
        return Err(RuntimeError::InvalidConfig(
            "worker join timeout must be greater than zero",
        ));
    }

    if settings.command_queue_capacity == 0 {
        return Err(RuntimeError::InvalidConfig(
            "command queue capacity must be greater than zero",
        ));
    }

    Ok(())
}

fn validate_runtime_config(config: &ProjectConfig) -> Result<(), RuntimeError> {
    config.validate()?;

    if !config.enabled {
        return Err(RuntimeError::ProjectDisabled);
    }

    validate_unique_ids(config)?;
    validate_single_enabled_broker(config)?;
    validate_routes(config)?;
    validate_command_templates(config)?;
    validate_rule_sink_references(config)?;

    Ok(())
}

fn validate_unique_ids(config: &ProjectConfig) -> Result<(), RuntimeError> {
    let mut broker_ids = HashSet::with_capacity(config.brokers.len());
    for broker in &config.brokers {
        insert_unique_id(&mut broker_ids, &broker.id, "broker")?;
    }

    let mut route_ids = HashSet::with_capacity(config.routes.len());
    for route in &config.routes {
        insert_unique_id(&mut route_ids, &route.id, "route")?;
    }

    let mut schema_mapping_ids = HashSet::with_capacity(config.schema_mappings.len());
    for mapping in &config.schema_mappings {
        insert_unique_id(&mut schema_mapping_ids, &mapping.id, "schema_mapping")?;
    }

    let mut rule_ids = HashSet::with_capacity(config.rules.len());
    for rule in &config.rules {
        rule.validate()?;
        insert_unique_id(&mut rule_ids, &rule.id, "rule")?;
    }

    let mut command_template_ids = HashSet::with_capacity(config.command_templates.len());
    for template in &config.command_templates {
        insert_unique_id(&mut command_template_ids, &template.id, "command_template")?;
    }

    let mut sink_ids = HashSet::with_capacity(config.sinks.len());
    for sink in &config.sinks {
        insert_unique_id(&mut sink_ids, &sink.id, "sink")?;
    }

    Ok(())
}

fn insert_unique_id<T>(
    seen: &mut HashSet<T>,
    id: &T,
    collection: &'static str,
) -> Result<(), RuntimeError>
where
    T: Clone + Eq + Hash + ToString,
{
    if !seen.insert(id.clone()) {
        return Err(RuntimeError::DuplicateId {
            collection,
            id: id.to_string(),
        });
    }

    Ok(())
}

fn validate_single_enabled_broker(config: &ProjectConfig) -> Result<(), RuntimeError> {
    let enabled_count = config
        .brokers
        .iter()
        .filter(|broker| broker.enabled)
        .count();

    match enabled_count {
        0 => Err(RuntimeError::NoEnabledBroker),
        1 => Ok(()),
        count => Err(RuntimeError::MultipleEnabledBrokersUnsupported { count }),
    }
}

fn validate_routes(config: &ProjectConfig) -> Result<(), RuntimeError> {
    let enabled_broker_ids = config
        .brokers
        .iter()
        .filter(|broker| broker.enabled)
        .map(|broker| broker.id.clone())
        .collect::<HashSet<_>>();
    let schema_mapping_ids = config
        .schema_mappings
        .iter()
        .map(|mapping| mapping.id.clone())
        .collect::<HashSet<_>>();

    let mut enabled_route_count = 0usize;

    for route in config.routes.iter().filter(|route| route.enabled) {
        enabled_route_count += 1;

        if !enabled_broker_ids.contains(&route.broker_id) {
            return Err(RuntimeError::RouteReferencesUnavailableBroker {
                route_id: route.id.to_string(),
                broker_id: route.broker_id.to_string(),
            });
        }

        if let Some(schema_mapping_id) = &route.schema_mapping_id
            && !schema_mapping_ids.contains(schema_mapping_id)
        {
            return Err(RuntimeError::RouteReferencesUnknownSchemaMapping {
                route_id: route.id.to_string(),
                schema_mapping_id: schema_mapping_id.to_string(),
            });
        }

        if route.backpressure != BackpressurePolicy::Reject {
            return Err(RuntimeError::UnsupportedBackpressurePolicy {
                route_id: route.id.to_string(),
                policy: backpressure_policy_name(route.backpressure),
            });
        }
    }

    if enabled_route_count == 0 {
        let broker_id = config
            .brokers
            .iter()
            .find(|broker| broker.enabled)
            .map(|broker| broker.id.to_string())
            .unwrap_or_else(|| "<none>".to_owned());

        return Err(RuntimeError::NoEnabledRoutesForBroker { broker_id });
    }

    Ok(())
}

fn validate_command_templates(config: &ProjectConfig) -> Result<(), RuntimeError> {
    let enabled_broker_ids = config
        .brokers
        .iter()
        .filter(|broker| broker.enabled)
        .map(|broker| broker.id.clone())
        .collect::<HashSet<_>>();

    for template in config
        .command_templates
        .iter()
        .filter(|template| template.enabled)
    {
        if !enabled_broker_ids.contains(&template.broker_id) {
            return Err(RuntimeError::CommandTemplateReferencesUnavailableBroker {
                template_id: template.id.to_string(),
                broker_id: template.broker_id.to_string(),
            });
        }
    }

    Ok(())
}

fn validate_rule_sink_references(config: &ProjectConfig) -> Result<(), RuntimeError> {
    for rule in config.rules.iter().filter(|rule| rule.enabled) {
        for action in &rule.actions {
            let ActionIntentTemplate::ForwardToSink { sink_id } = action else {
                continue;
            };

            let Some(sink) = config.sinks.iter().find(|sink| &sink.id == sink_id) else {
                return Err(RuntimeError::RuleReferencesUnknownSink {
                    rule_id: rule.id.to_string(),
                    sink_id: sink_id.to_string(),
                });
            };

            if !sink.enabled {
                return Err(RuntimeError::RuleReferencesDisabledSink {
                    rule_id: rule.id.to_string(),
                    sink_id: sink_id.to_string(),
                });
            }

            if !matches!(&sink.kind, SinkKind::Webhook { .. }) {
                return Err(RuntimeError::RuleReferencesUnsupportedSink {
                    rule_id: rule.id.to_string(),
                    sink_id: sink_id.to_string(),
                });
            }
        }
    }

    Ok(())
}

fn backpressure_policy_name(policy: BackpressurePolicy) -> &'static str {
    match policy {
        BackpressurePolicy::DropNewest => "drop_newest",
        BackpressurePolicy::DropOldest => "drop_oldest",
        BackpressurePolicy::Reject => "reject",
        BackpressurePolicy::BlockProducer => "block_producer",
    }
}

fn saturating_usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::time::Duration;

    use pipe_bolt_domain::{
        ActionIntentTemplate, BackpressurePolicy, BrokerConnectionConfig, BrokerId,
        CommandExecutionId, CommandSource, CommandTemplate, CommandTemplateId, DeviceIdExtraction,
        HttpMethod, MqttQos, PayloadCodecKind, ProjectConfig, ProjectId, ReconnectPolicy,
        RuleDefinition, RuleId, RuleTrigger, SinkDefinition, SinkId, SinkKind, TlsMode,
        TopicFilter, TopicRouteConfig,
    };

    use super::*;

    #[test]
    fn runtime_rejects_disabled_project() {
        let mut config = project_config();
        config.enabled = false;

        let error = validate_runtime_config(&config).expect_err("disabled project error");

        assert!(matches!(error, RuntimeError::ProjectDisabled));
    }

    #[test]
    fn runtime_rejects_duplicate_ids() {
        let mut config = project_config();
        config
            .routes
            .push(route("route-telemetry", "devices/+/status"));

        let error = validate_runtime_config(&config).expect_err("duplicate route error");

        assert!(matches!(
            error,
            RuntimeError::DuplicateId {
                collection: "route",
                ..
            }
        ));
    }

    #[test]
    fn runtime_rejects_multiple_enabled_brokers() {
        let mut config = project_config();
        config.brokers.push(broker("broker-secondary", true));

        let error = validate_runtime_config(&config).expect_err("multiple broker error");

        assert!(matches!(
            error,
            RuntimeError::MultipleEnabledBrokersUnsupported { count: 2 }
        ));
    }

    #[test]
    fn runtime_rejects_route_to_unknown_or_disabled_broker() {
        let mut config = project_config();
        config.routes[0].broker_id = BrokerId::new("missing-broker").expect("broker id");

        let error = validate_runtime_config(&config).expect_err("missing broker error");

        assert!(matches!(
            error,
            RuntimeError::RouteReferencesUnavailableBroker { .. }
        ));
    }

    #[test]
    fn runtime_rejects_unsupported_backpressure_policy() {
        let mut config = project_config();
        config.routes[0].backpressure = BackpressurePolicy::DropOldest;

        let error = validate_runtime_config(&config).expect_err("backpressure error");

        assert!(matches!(
            error,
            RuntimeError::UnsupportedBackpressurePolicy { .. }
        ));
    }

    #[test]
    fn runtime_rejects_rule_to_unknown_sink() {
        let mut config = project_config();
        config.rules[0].actions = vec![ActionIntentTemplate::ForwardToSink {
            sink_id: SinkId::new("missing-sink").expect("sink id"),
        }];

        let error = validate_runtime_config(&config).expect_err("missing sink error");

        assert!(matches!(
            error,
            RuntimeError::RuleReferencesUnknownSink { .. }
        ));
    }

    #[test]
    fn runtime_accepts_enabled_command_template_when_broker_is_enabled() {
        let mut config = project_config();
        config.command_templates.push(command_template("command-1"));

        validate_runtime_config(&config).expect("enabled command template supported");
    }

    #[test]
    fn runtime_rejects_enabled_command_template_when_broker_disabled() {
        let mut config = project_config();
        let mut template = command_template("command-1");
        template.broker_id = BrokerId::new("broker-disabled").expect("broker id");
        config.brokers.push(broker("broker-disabled", false));
        config.command_templates.push(template);

        let error = validate_runtime_config(&config).expect_err("command template broker error");

        assert!(matches!(
            error,
            RuntimeError::CommandTemplateReferencesUnavailableBroker { .. }
        ));
    }

    #[test]
    fn runtime_settings_should_reject_zero_command_queue_capacity() {
        let settings = RuntimeSettings {
            command_queue_capacity: 0,
            ..RuntimeSettings::default()
        };

        let error = validate_runtime_settings(&settings).expect_err("invalid settings");

        assert!(matches!(error, RuntimeError::InvalidConfig(_)));
    }

    #[test]
    fn command_queue_should_report_full_when_capacity_is_exhausted() {
        let stats = Arc::new(RuntimeStats::default());
        let (handle, _rx) = command_queue_channel(1, Arc::clone(&stats));
        handle
            .try_enqueue(command_request("command-exec-1"))
            .expect("first request queued");

        let error = handle
            .try_enqueue(command_request("command-exec-2"))
            .expect_err("queue full");

        assert_eq!(error, RuntimeCommandQueueError::Full);
    }

    #[test]
    fn command_queue_should_report_closed_when_receiver_is_dropped() {
        let stats = Arc::new(RuntimeStats::default());
        let (handle, rx) = command_queue_channel(1, stats);
        drop(rx);

        let error = handle
            .try_enqueue(command_request("command-exec-1"))
            .expect_err("queue closed");

        assert_eq!(error, RuntimeCommandQueueError::Closed);
    }

    #[test]
    fn command_processor_should_render_and_enqueue_request_when_request_is_valid() {
        let sink = Arc::new(RecordingCommandSink::default());
        let processor =
            command_processor_with_sink(command_template("command-1"), Arc::clone(&sink));
        let request = command_request_with_params(BTreeMap::from([
            ("device_id".to_owned(), serde_json::json!("device-1")),
            ("state".to_owned(), serde_json::json!("ON")),
        ]));

        process_command_request(&processor, &request).expect("command processed");
        let commands = sink.commands.lock().expect("commands lock");

        assert_eq!(
            commands[0].topic().as_str(),
            "devices/device-1/commands/relay"
        );
    }

    #[test]
    fn command_processor_should_return_render_error_when_params_are_missing() {
        let sink = Arc::new(RecordingCommandSink::default());
        let processor = command_processor_with_sink(command_template("command-1"), sink);
        let request = command_request_with_params(BTreeMap::from([(
            "state".to_owned(),
            serde_json::json!("ON"),
        )]));

        let error = process_command_request(&processor, &request).expect_err("render error");

        assert!(matches!(error, RuntimeCommandProcessorError::Render { .. }));
    }

    #[test]
    fn command_processor_should_return_publish_error_when_mqtt_queue_rejects_command() {
        let sink = Arc::new(FailingCommandSink);
        let processor = command_processor_with_sink(command_template("command-1"), sink);
        let request = command_request_with_params(BTreeMap::from([
            ("device_id".to_owned(), serde_json::json!("device-1")),
            ("state".to_owned(), serde_json::json!("ON")),
        ]));

        let error = process_command_request(&processor, &request).expect_err("publish error");

        assert!(matches!(
            error,
            RuntimeCommandProcessorError::PublishEnqueue { .. }
        ));
    }

    #[tokio::test]
    async fn shutdown_join_workers_reports_timeout() {
        let worker = RuntimeWorker::spawn("stuck-worker", async {
            std::future::pending::<Result<(), RuntimeError>>().await
        });

        let error = join_workers(vec![worker], Duration::from_millis(1))
            .await
            .expect_err("worker join timeout");

        assert!(matches!(
            error,
            RuntimeError::WorkerJoinTimeout {
                name: "stuck-worker"
            }
        ));
    }

    #[test]
    fn runtime_builds_subscription_set_from_enabled_routes() {
        let routes = vec![
            route_with_qos("route-1", "devices/+/telemetry", MqttQos::AtMostOnce),
            route_with_qos("route-2", "devices/+/telemetry", MqttQos::ExactlyOnce),
        ];

        let subscriptions = merged_subscriptions(&routes);

        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].0, "devices/+/telemetry");
        assert_eq!(subscriptions[0].1, QoS::ExactlyOnce);
    }

    fn project_config() -> ProjectConfig {
        ProjectConfig {
            id: ProjectId::new("project-local").expect("project id"),
            tenant_id: None,
            name: "Local Project".to_owned(),
            description: None,
            enabled: true,
            version: 1,
            brokers: vec![broker("broker-local", true)],
            routes: vec![route("route-telemetry", "devices/+/telemetry")],
            schema_mappings: Vec::new(),
            rules: vec![stream_rule()],
            command_templates: Vec::new(),
            sinks: Vec::new(),
        }
    }

    fn broker(id: &str, enabled: bool) -> BrokerConnectionConfig {
        BrokerConnectionConfig {
            id: BrokerId::new(id).expect("broker id"),
            name: id.to_owned(),
            host: "localhost".to_owned(),
            port: 1883,
            tls: TlsMode::Disabled,
            credentials: None,
            keep_alive: Duration::from_secs(30),
            clean_session: false,
            client_id: format!("pipe-bolt-{id}"),
            reconnect: ReconnectPolicy::default(),
            enabled,
        }
    }

    fn route(id: &str, topic_filter: &str) -> TopicRouteConfig {
        route_with_qos(id, topic_filter, MqttQos::AtLeastOnce)
    }

    fn route_with_qos(id: &str, topic_filter: &str, qos: MqttQos) -> TopicRouteConfig {
        TopicRouteConfig {
            id: pipe_bolt_domain::RouteId::new(id).expect("route id"),
            broker_id: BrokerId::new("broker-local").expect("broker id"),
            name: id.to_owned(),
            topic_filter: TopicFilter::new(topic_filter).expect("topic filter"),
            codec: PayloadCodecKind::Json,
            schema_mapping_id: None,
            device_id: DeviceIdExtraction::TopicWildcardIndex { index: 0 },
            event_type: "telemetry".to_owned(),
            qos,
            enabled: true,
            backpressure: BackpressurePolicy::Reject,
        }
    }

    fn stream_rule() -> RuleDefinition {
        RuleDefinition {
            id: RuleId::new("rule-stream-all").expect("rule id"),
            name: "Stream All Events".to_owned(),
            enabled: true,
            trigger: RuleTrigger::EventReceived,
            condition: None,
            actions: vec![ActionIntentTemplate::StreamToUi],
        }
    }

    fn command_template(id: &str) -> CommandTemplate {
        CommandTemplate {
            id: CommandTemplateId::new(id).expect("command template id"),
            name: "Turn Relay On".to_owned(),
            broker_id: BrokerId::new("broker-local").expect("broker id"),
            topic_template: "devices/{device_id}/commands/relay".to_owned(),
            payload_template: serde_json::json!({ "relay": 1, "state": "{state}" }),
            qos: MqttQos::AtLeastOnce,
            retain: false,
            enabled: true,
        }
    }

    fn command_request(command_execution_id: &str) -> CommandExecutionRequest {
        command_request_with_id_and_params(
            command_execution_id,
            BTreeMap::from([
                ("device_id".to_owned(), serde_json::json!("device-1")),
                ("state".to_owned(), serde_json::json!("ON")),
            ]),
        )
    }

    fn command_request_with_params(
        params: BTreeMap<String, serde_json::Value>,
    ) -> CommandExecutionRequest {
        command_request_with_id_and_params("command-exec-1", params)
    }

    fn command_request_with_id_and_params(
        command_execution_id: &str,
        params: BTreeMap<String, serde_json::Value>,
    ) -> CommandExecutionRequest {
        CommandExecutionRequest {
            project_id: ProjectId::new("project-local").expect("project id"),
            command_execution_id: CommandExecutionId::new(command_execution_id)
                .expect("command execution id"),
            command_template_id: CommandTemplateId::new("command-1").expect("command template id"),
            params,
            source: CommandSource::Api,
            actor_id: None,
            source_event_id: None,
            correlation_id: "corr-1".to_owned(),
            reason: Some("test command".to_owned()),
        }
    }

    fn command_processor_with_sink<S>(template: CommandTemplate, sink: Arc<S>) -> CommandProcessor
    where
        S: CommandPublishSink + 'static,
    {
        let sink: Arc<dyn CommandPublishSink> = sink;

        CommandProcessor {
            project_id: ProjectId::new("project-local").expect("project id"),
            templates: Arc::new(vec![template]),
            brokers: Arc::new(vec![CommandProcessorBroker {
                broker_id: BrokerId::new("broker-local").expect("broker id"),
                sink,
            }]),
            renderer: CommandTemplateRenderer::default(),
            stats: Arc::new(RuntimeStats::default()),
        }
    }

    #[derive(Default)]
    struct RecordingCommandSink {
        commands: Mutex<Vec<RenderedCommand>>,
    }

    impl CommandPublishSink for RecordingCommandSink {
        fn try_enqueue_rendered(&self, command: RenderedCommand) -> Result<(), MqttEngineError> {
            self.commands.lock().expect("commands lock").push(command);
            Ok(())
        }
    }

    struct FailingCommandSink;

    impl CommandPublishSink for FailingCommandSink {
        fn try_enqueue_rendered(&self, _command: RenderedCommand) -> Result<(), MqttEngineError> {
            Err(MqttEngineError::CommandQueueFull)
        }
    }

    #[allow(dead_code)]
    fn webhook_sink(id: &str) -> SinkDefinition {
        SinkDefinition {
            id: SinkId::new(id).expect("sink id"),
            name: id.to_owned(),
            enabled: true,
            kind: SinkKind::Webhook {
                url: "https://example.com/events".to_owned(),
                method: HttpMethod::Post,
                headers: Vec::new(),
                timeout: Duration::from_secs(5),
            },
        }
    }
}
