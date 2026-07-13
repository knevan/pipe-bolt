use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

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
use pipe_bolt_core::mqtt::engine::MqttEngine;
use pipe_bolt_core::pipeline::normalize_routed_message;
use pipe_bolt_core::pipeline::normalizer::{EventNormalizer, NormalizerLimits};
use pipe_bolt_core::pipeline::router::ConfigRouteMatcher;
use pipe_bolt_core::router::matcher::MqttRouter;
use pipe_bolt_core::rule::rules::{RuleEngine, RuleEngineLimits};
use pipe_bolt_domain::{
    ActionIntentTemplate, BackpressurePolicy, BrokerConnectionConfig, BrokerId, CommandTemplate,
    CommandTemplateId, MqttQos, NormalizedEvent, PayloadSchemaMapping, ProjectConfig, ProjectId,
    SinkKind, TlsMode, TopicName, TopicRouteConfig,
};
use pipe_bolt_storage::postgres::PostgresStorage;
use rumqttc::QoS;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::timeout;

use crate::persistence_writer::{
    PersistenceWriterError, PersistenceWriterHandle, PersistenceWriterSettings,
    PersistenceWriterStatsSnapshot, RuntimePersistenceWriter,
};

const DEFAULT_REALTIME_EVENT_CAPACITY: usize = 1024;
const DEFAULT_WORKER_JOIN_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_COMMAND_PAYLOAD_BYTES: usize = 64 * 1024;

type RuntimeDispatcher = ActionDispatcher<RuntimeRealtimeSink, BoundedHttpForwarder>;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RuntimeSettings {
    pub forward_limits: ForwardLimits,
    pub egress_policy: EgressPolicy,
    pub normalizer_limits: NormalizerLimits,
    pub rule_limits: RuleEngineLimits,
    pub dispatch_limits: DispatchLimits,
    pub realtime_event_capacity: usize,
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

        let mut workers = Vec::new();
        workers.push(RuntimeWorker::spawn("forwarder", async move {
            forward_worker.run(shutdown_rx).await;
            Ok(())
        }));
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

        let rendered = render_command_template(template, params)?;
        let engine = self
            .mqtt_engines
            .iter()
            .find(|broker| broker.broker_id == template.broker_id)
            .ok_or_else(|| RuntimeError::CommandBrokerUnavailable {
                broker_id: template.broker_id.to_string(),
            })?;

        engine.engine.handle().try_enqueue_command(
            rendered.topic.clone(),
            map_qos(template.qos),
            template.retain,
            rendered.payload,
        )?;

        Ok(QueuedRuntimeCommand {
            command_template_id: template.id.clone(),
            broker_id: template.broker_id.clone(),
            topic: rendered.topic,
            qos: template.qos,
            retain: template.retain,
            payload_size_bytes: rendered.payload_size_bytes,
        })
    }

    pub async fn shutdown(self) -> Result<(), RuntimeError> {
        let Self {
            mqtt_engines,
            shutdown_tx,
            workers,
            worker_join_timeout,
            persistence_writer,
            ..
        } = self;
        let mut first_error = None;

        for broker in mqtt_engines {
            if let Err(error) = broker.engine.shutdown().await {
                remember_first_error(&mut first_error, RuntimeError::from(error));
            }
        }

        let _ = shutdown_tx.send(true);

        if let Err(error) = join_workers(workers, worker_join_timeout).await {
            remember_first_error(&mut first_error, error);
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

struct RenderedRuntimeCommand {
    topic: String,
    payload: Vec<u8>,
    payload_size_bytes: u64,
}

fn render_command_template(
    template: &CommandTemplate,
    params: &BTreeMap<String, serde_json::Value>,
) -> Result<RenderedRuntimeCommand, RuntimeError> {
    let topic = render_topic_template(template, params)?;
    TopicName::new(topic.as_str()).map_err(|error| RuntimeError::CommandTemplateRender {
        template_id: template.id.to_string(),
        reason: error.to_string(),
    })?;

    let payload_value = render_payload_value(template, &template.payload_template, params)?;
    let payload = serde_json::to_vec(&payload_value).map_err(|error| {
        RuntimeError::CommandTemplateRender {
            template_id: template.id.to_string(),
            reason: error.to_string(),
        }
    })?;
    if payload.len() > MAX_COMMAND_PAYLOAD_BYTES {
        return Err(RuntimeError::CommandPayloadTooLarge {
            max: MAX_COMMAND_PAYLOAD_BYTES,
            actual: payload.len(),
        });
    }

    Ok(RenderedRuntimeCommand {
        topic,
        payload_size_bytes: saturating_usize_to_u64(payload.len()),
        payload,
    })
}

fn render_topic_template(
    template: &CommandTemplate,
    params: &BTreeMap<String, serde_json::Value>,
) -> Result<String, RuntimeError> {
    let mut output = String::with_capacity(template.topic_template.len());
    let mut rest = template.topic_template.as_str();

    while let Some(start) = rest.find('{') {
        output.push_str(&rest[..start]);
        let after_start = &rest[start + 1..];
        let Some(end) = after_start.find('}') else {
            return Err(template_render_error(
                template,
                "unclosed topic placeholder",
            ));
        };
        let key = &after_start[..end];
        output.push_str(&topic_param(template, key, params)?);
        rest = &after_start[end + 1..];
    }
    output.push_str(rest);

    if output.contains('}') {
        return Err(template_render_error(
            template,
            "unopened topic placeholder",
        ));
    }

    Ok(output)
}

fn render_payload_value(
    template: &CommandTemplate,
    value: &serde_json::Value,
    params: &BTreeMap<String, serde_json::Value>,
) -> Result<serde_json::Value, RuntimeError> {
    match value {
        serde_json::Value::String(text) => render_payload_string(template, text, params),
        serde_json::Value::Array(values) => values
            .iter()
            .map(|value| render_payload_value(template, value, params))
            .collect::<Result<Vec<_>, _>>()
            .map(serde_json::Value::Array),
        serde_json::Value::Object(object) => object
            .iter()
            .map(|(key, value)| Ok((key.clone(), render_payload_value(template, value, params)?)))
            .collect::<Result<serde_json::Map<_, _>, RuntimeError>>()
            .map(serde_json::Value::Object),
        _ => Ok(value.clone()),
    }
}

fn render_payload_string(
    template: &CommandTemplate,
    text: &str,
    params: &BTreeMap<String, serde_json::Value>,
) -> Result<serde_json::Value, RuntimeError> {
    if let Some(key) = exact_placeholder_key(text) {
        return params
            .get(key)
            .cloned()
            .ok_or_else(|| template_render_error(template, &format!("missing parameter '{key}'")));
    }

    render_embedded_placeholders(template, text, params).map(serde_json::Value::String)
}

fn render_embedded_placeholders(
    template: &CommandTemplate,
    text: &str,
    params: &BTreeMap<String, serde_json::Value>,
) -> Result<String, RuntimeError> {
    let mut output = String::with_capacity(text.len());
    let mut rest = text;

    while let Some(start) = rest.find('{') {
        output.push_str(&rest[..start]);
        let after_start = &rest[start + 1..];
        let Some(end) = after_start.find('}') else {
            return Err(template_render_error(
                template,
                "unclosed payload placeholder",
            ));
        };
        let key = &after_start[..end];
        output.push_str(&scalar_param(template, key, params)?);
        rest = &after_start[end + 1..];
    }
    output.push_str(rest);

    if output.contains('}') {
        return Err(template_render_error(
            template,
            "unopened payload placeholder",
        ));
    }

    Ok(output)
}

fn exact_placeholder_key(text: &str) -> Option<&str> {
    text.strip_prefix('{')?.strip_suffix('}')
}

fn topic_param(
    template: &CommandTemplate,
    key: &str,
    params: &BTreeMap<String, serde_json::Value>,
) -> Result<String, RuntimeError> {
    let value = scalar_param(template, key, params)?;
    if value.is_empty()
        || value.contains('/')
        || value.contains('+')
        || value.contains('#')
        || value.contains('\0')
        || value.chars().any(char::is_control)
    {
        return Err(template_render_error(
            template,
            &format!("parameter '{key}' must be a single MQTT topic segment"),
        ));
    }

    Ok(value)
}

fn scalar_param(
    template: &CommandTemplate,
    key: &str,
    params: &BTreeMap<String, serde_json::Value>,
) -> Result<String, RuntimeError> {
    if key.trim().is_empty() {
        return Err(template_render_error(
            template,
            "placeholder name must not be empty",
        ));
    }

    let value = params
        .get(key)
        .ok_or_else(|| template_render_error(template, &format!("missing parameter '{key}'")))?;

    match value {
        serde_json::Value::String(value) => Ok(value.clone()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        serde_json::Value::Null | serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(template_render_error(
                template,
                &format!("parameter '{key}' must be scalar"),
            ))
        }
    }
}

fn template_render_error(template: &CommandTemplate, reason: &str) -> RuntimeError {
    RuntimeError::CommandTemplateRender {
        template_id: template.id.to_string(),
        reason: reason.to_owned(),
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
    use std::time::Duration;

    use pipe_bolt_domain::{
        ActionIntentTemplate, BackpressurePolicy, BrokerConnectionConfig, BrokerId,
        CommandTemplate, CommandTemplateId, DeviceIdExtraction, HttpMethod, MqttQos,
        PayloadCodecKind, ProjectConfig, ProjectId, ReconnectPolicy, RuleDefinition, RuleId,
        RuleTrigger, SinkDefinition, SinkId, SinkKind, TlsMode, TopicFilter, TopicRouteConfig,
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
    fn command_template_should_preserve_json_type_when_payload_is_exact_placeholder() {
        let template = command_template("command-1");
        let params = BTreeMap::from([("state".to_owned(), serde_json::json!(true))]);

        let rendered = render_payload_string(&template, "{state}", &params).expect("rendered");

        assert_eq!(rendered, serde_json::json!(true));
    }

    #[test]
    fn command_template_should_reject_topic_segment_escape() {
        let template = command_template("command-1");
        let params = BTreeMap::from([("device_id".to_owned(), serde_json::json!("a/b"))]);

        let error = render_topic_template(&template, &params).expect_err("segment escape");

        assert!(matches!(error, RuntimeError::CommandTemplateRender { .. }));
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
