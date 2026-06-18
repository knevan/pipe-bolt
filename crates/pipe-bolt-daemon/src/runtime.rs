use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pipe_bolt_core::config::{MqttClientConfig, MqttReconnectConfig, MqttTlsMode};
use pipe_bolt_core::dispatcher::action::{
    ActionDispatcher, BoundedRealtimeEventSink, DispatchLimits,
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
use pipe_bolt_core::web::realtime::router::{default_bind_addr, serve_realtime_bridge_with_state};
use pipe_bolt_core::web::realtime::state::RealtimeBridgeState;
use pipe_bolt_domain::{
    ActionIntentTemplate, BrokerConnectionConfig, BrokerId, MqttQos, PayloadSchemaMapping,
    ProjectConfig, SinkKind, TlsMode, TopicRouteConfig,
};
use rumqttc::QoS;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinError, JoinHandle};

const DEFAULT_REALTIME_EVENT_QUEUE_CAPACITY: usize = 1024;

type RuntimeDispatcher = ActionDispatcher<BoundedRealtimeEventSink, BoundedHttpForwarder>;

#[derive(Debug, Clone)]
pub struct RuntimeSettings {
    pub forward_limits: ForwardLimits,
    pub egress_policy: EgressPolicy,
    pub normalizer_limits: NormalizerLimits,
    pub rule_limits: RuleEngineLimits,
    pub dispatch_limits: DispatchLimits,
    pub realtime_event_queue_capacity: usize,
    pub realtime_bridge_bind_addr: SocketAddr,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            forward_limits: ForwardLimits::default(),
            egress_policy: EgressPolicy::default(),
            normalizer_limits: NormalizerLimits::default(),
            rule_limits: RuleEngineLimits::default(),
            dispatch_limits: DispatchLimits::default(),
            realtime_event_queue_capacity: DEFAULT_REALTIME_EVENT_QUEUE_CAPACITY,
            realtime_bridge_bind_addr: default_bind_addr(),
        }
    }
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("project is disabled")]
    ProjectDisabled,

    #[error("invalid runtime config: {0}")]
    InvalidConfig(&'static str),

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

    #[error("worker '{name}' join failed")]
    WorkerJoin {
        name: &'static str,
        #[source]
        source: JoinError,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
pub struct RuntimeStatsSnapshot {
    pub normalized_total: u64,
    pub route_miss_total: u64,
    pub matched_rule_total: u64,
    pub action_intent_total: u64,
    pub dispatch_failed_total: u64,
    pub realtime_event_total: u64,
    pub forward_outcome_total: u64,
}

#[derive(Debug, Default)]
pub struct RuntimeStats {
    normalized_total: AtomicU64,
    route_miss_total: AtomicU64,
    matched_rule_total: AtomicU64,
    action_intent_total: AtomicU64,
    dispatch_failed_total: AtomicU64,
    realtime_event_total: AtomicU64,
    forward_outcome_total: AtomicU64,
}

impl RuntimeStats {
    pub fn snapshot(&self) -> RuntimeStatsSnapshot {
        RuntimeStatsSnapshot {
            normalized_total: self.normalized_total.load(Ordering::Relaxed),
            route_miss_total: self.route_miss_total.load(Ordering::Relaxed),
            matched_rule_total: self.matched_rule_total.load(Ordering::Relaxed),
            action_intent_total: self.action_intent_total.load(Ordering::Relaxed),
            dispatch_failed_total: self.dispatch_failed_total.load(Ordering::Relaxed),
            realtime_event_total: self.realtime_event_total.load(Ordering::Relaxed),
            forward_outcome_total: self.forward_outcome_total.load(Ordering::Relaxed),
        }
    }

    fn record_normalized(&self) {
        self.normalized_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_route_miss(&self) {
        self.route_miss_total.fetch_add(1, Ordering::Relaxed);
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

    fn record_realtime_event(&self) {
        self.realtime_event_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_forward_outcome(&self) {
        self.forward_outcome_total.fetch_add(1, Ordering::Relaxed);
    }
}

pub struct ProjectRuntime {
    mqtt_engines: Vec<MqttEngine>,
    shutdown_tx: watch::Sender<bool>,
    workers: Vec<RuntimeWorker>,
    stats: Arc<RuntimeStats>,
    forwarder_stats: Arc<ForwarderStats>,
}

impl ProjectRuntime {
    pub fn start(config: ProjectConfig, settings: RuntimeSettings) -> Result<Self, RuntimeError> {
        validate_runtime_config(&config)?;

        let stats = Arc::new(RuntimeStats::default());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let (forwarder, forward_worker, forward_outcomes) =
            BoundedHttpForwarder::try_channel_with_policy(
                config.sinks.clone(),
                settings.forward_limits,
                settings.egress_policy.clone(),
            )?;
        let forwarder_stats = forwarder.stats();

        let (realtime_sink, realtime_events) =
            BoundedRealtimeEventSink::try_channel(settings.realtime_event_queue_capacity)?;
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
            ),
        ));
        workers.push(RuntimeWorker::spawn(
            "realtime-event-consumer",
            consume_realtime_events(realtime_events, shutdown_tx.subscribe(), Arc::clone(&stats)),
        ));

        let mut mqtt_engines = Vec::with_capacity(pending_brokers.len());

        for pending in pending_brokers {
            mqtt_engines.push(MqttEngine::spawn(pending.config, pending.router)?);
        }

        if let Some(engine) = mqtt_engines.first() {
            let realtime_state = RealtimeBridgeState::new(engine.handle());
            let bind_addr = settings.realtime_bridge_bind_addr;
            let shutdown_rx = shutdown_tx.subscribe();
            workers.push(RuntimeWorker::spawn("realtime-bridge", async move {
                serve_realtime_bridge_with_state(bind_addr, realtime_state, shutdown_rx)
                    .await
                    .map_err(RuntimeError::from)
            }));
        }

        Ok(Self {
            mqtt_engines,
            shutdown_tx,
            workers,
            stats,
            forwarder_stats,
        })
    }

    pub fn runtime_stats(&self) -> RuntimeStatsSnapshot {
        self.stats.snapshot()
    }

    pub fn forwarder_stats(&self) -> ForwarderStatsSnapshot {
        self.forwarder_stats.snapshot()
    }

    pub async fn shutdown(self) -> Result<(), RuntimeError> {
        let Self {
            mqtt_engines,
            shutdown_tx,
            workers,
            ..
        } = self;

        for engine in mqtt_engines {
            engine.shutdown().await?;
        }

        let _ = shutdown_tx.send(true);
        join_workers(workers).await
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

    if enabled_brokers.is_empty() {
        return Err(RuntimeError::NoEnabledBroker);
    }

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
        stats.record_route_miss();
        return Ok(());
    };

    stats.record_normalized();

    let evaluation = rule_engine.evaluate(&event)?;
    stats.record_rule_evaluation(evaluation.matched_rules.len(), evaluation.intents.len());

    let dispatch = dispatcher.dispatch(&event, &evaluation.intents)?;
    stats.record_dispatch_failures(dispatch.failed.len());

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

async fn consume_forward_outcomes(
    mut outcomes: mpsc::Receiver<ForwardDeliveryOutcome>,
    mut shutdown_rx: watch::Receiver<bool>,
    stats: Arc<RuntimeStats>,
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

                record_forward_outcome(&stats, outcome);
            }
        }
    }

    while let Some(outcome) = outcomes.recv().await {
        record_forward_outcome(&stats, outcome);
    }

    Ok(())
}

fn record_forward_outcome(stats: &RuntimeStats, outcome: ForwardDeliveryOutcome) {
    stats.record_forward_outcome();
    eprintln!(
        "forward delivery outcome: event_id={} sink_id={} status={:?}",
        outcome.event_id, outcome.sink_id, outcome.status
    );
}

async fn consume_realtime_events(
    mut events: mpsc::Receiver<pipe_bolt_domain::NormalizedEvent>,
    mut shutdown_rx: watch::Receiver<bool>,
    stats: Arc<RuntimeStats>,
) -> Result<(), RuntimeError> {
    loop {
        tokio::select! {
            biased;

            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    events.close();
                    break;
                }
            }

            event = events.recv() => {
                let Some(_event) = event else {
                    break;
                };

                stats.record_realtime_event();
            }
        }
    }

    while let Some(_event) = events.recv().await {
        stats.record_realtime_event();
    }

    Ok(())
}

async fn join_workers(workers: Vec<RuntimeWorker>) -> Result<(), RuntimeError> {
    let mut first_error = None;

    for worker in workers {
        match worker.handle.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if first_error.is_none() => {
                first_error = Some(error);
            }
            Ok(Err(_)) => {}
            Err(source) if first_error.is_none() => {
                first_error = Some(RuntimeError::WorkerJoin {
                    name: worker.name,
                    source,
                });
            }
            Err(_) => {}
        }
    }

    if let Some(error) = first_error {
        return Err(error);
    }

    Ok(())
}

fn validate_runtime_config(config: &ProjectConfig) -> Result<(), RuntimeError> {
    config.validate()?;

    if !config.enabled {
        return Err(RuntimeError::ProjectDisabled);
    }

    if config.routes.is_empty() {
        return Err(RuntimeError::InvalidConfig(
            "project config must include at least one route",
        ));
    }

    validate_unique_ids(config)?;
    validate_route_references(config)?;
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

fn validate_route_references(config: &ProjectConfig) -> Result<(), RuntimeError> {
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

    for route in config.routes.iter().filter(|route| route.enabled) {
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

fn saturating_usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
