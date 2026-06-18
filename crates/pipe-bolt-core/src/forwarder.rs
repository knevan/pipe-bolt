use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use pipe_bolt_domain::{EventId, HttpMethod, NormalizedEvent, SinkDefinition, SinkId, SinkKind};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Method, Url};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::error::DispatchError;

const DEFAULT_FORWARD_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_FORWARD_RESULT_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_HEADERS: usize = 32;
const DEFAULT_MAX_HEADER_NAME_BYTES: usize = 128;
const DEFAULT_MAX_HEADER_VALUE_BYTES: usize = 4096;
const DEFAULT_MAX_IN_FLIGHT: usize = 32;
const DEFAULT_MAX_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const MIN_TIMEOUT: Duration = Duration::from_millis(10);

/// Runtime limits for bounded sink forwarding.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ForwardLimits {
    pub queue_capacity: usize,
    pub result_queue_capacity: usize,
    pub max_request_body_bytes: usize,
    pub max_response_body_bytes: usize,
    pub max_headers: usize,
    pub max_header_name_bytes: usize,
    pub max_header_value_bytes: usize,
    pub max_in_flight: usize,
    pub max_timeout: Duration,
    pub graceful_shutdown_timeout: Duration,
}

impl Default for ForwardLimits {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_FORWARD_QUEUE_CAPACITY,
            result_queue_capacity: DEFAULT_FORWARD_RESULT_QUEUE_CAPACITY,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            max_response_body_bytes: DEFAULT_MAX_RESPONSE_BODY_BYTES,
            max_headers: DEFAULT_MAX_HEADERS,
            max_header_name_bytes: DEFAULT_MAX_HEADER_NAME_BYTES,
            max_header_value_bytes: DEFAULT_MAX_HEADER_VALUE_BYTES,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            max_timeout: DEFAULT_MAX_TIMEOUT,
            graceful_shutdown_timeout: DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT,
        }
    }
}

/// Egress policy for webhook targets.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EgressPolicy {
    pub require_https: bool,
    pub allow_private_networks: bool,
    pub allowed_hosts: Vec<String>,
}

impl Default for EgressPolicy {
    fn default() -> Self {
        Self {
            require_https: true,
            allow_private_networks: false,
            allowed_hosts: Vec::new(),
        }
    }
}

/// Request accepted by the local forwarder queue.
#[derive(Debug, Clone)]
pub struct ForwardRequest {
    pub event: NormalizedEvent,
    pub sink_id: SinkId,
    pub projection: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Result returned synchronously by the dispatch boundary.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ForwardReceipt {
    pub sink_id: SinkId,
    pub accepted: bool,
}

/// Asynchronous delivery result emitted by the worker after HTTP execution.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ForwardDeliveryOutcome {
    pub event_id: EventId,
    pub sink_id: SinkId,
    pub status: ForwardDeliveryStatus,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ForwardDeliveryStatus {
    Delivered {
        http_status: u16,
        response_body_bytes: usize,
    },
    HttpRejected {
        http_status: u16,
        response_body_bytes: usize,
    },
    TimedOut,
    ResponseTooLarge {
        max: usize,
    },
    Failed {
        reason: ForwardFailureReason,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ForwardFailureReason {
    RequestFailed,
    ResponseReadFailed,
    WorkerJoinFailed,
    OutcomeReceiverClosed,
}

/// Snapshot of in-memory forwarder counters.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Default)]
pub struct ForwarderStatsSnapshot {
    pub accepted_total: u64,
    pub backpressure_total: u64,
    pub delivered_total: u64,
    pub rejected_total: u64,
    pub timed_out_total: u64,
    pub failed_total: u64,
    pub response_too_large_total: u64,
    pub outcome_dropped_total: u64,
}

#[derive(Debug, Default)]
pub struct ForwarderStats {
    accepted_total: AtomicU64,
    backpressure_total: AtomicU64,
    delivered_total: AtomicU64,
    rejected_total: AtomicU64,
    timed_out_total: AtomicU64,
    failed_total: AtomicU64,
    response_too_large_total: AtomicU64,
    outcome_dropped_total: AtomicU64,
}

impl ForwarderStats {
    pub fn snapshot(&self) -> ForwarderStatsSnapshot {
        ForwarderStatsSnapshot {
            accepted_total: self.accepted_total.load(Ordering::Relaxed),
            backpressure_total: self.backpressure_total.load(Ordering::Relaxed),
            delivered_total: self.delivered_total.load(Ordering::Relaxed),
            rejected_total: self.rejected_total.load(Ordering::Relaxed),
            timed_out_total: self.timed_out_total.load(Ordering::Relaxed),
            failed_total: self.failed_total.load(Ordering::Relaxed),
            response_too_large_total: self.response_too_large_total.load(Ordering::Relaxed),
            outcome_dropped_total: self.outcome_dropped_total.load(Ordering::Relaxed),
        }
    }

    fn record_acceptance(&self) {
        self.accepted_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_backpressure(&self) {
        self.backpressure_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_outcome(&self, outcome: &ForwardDeliveryOutcome) {
        match outcome.status {
            ForwardDeliveryStatus::Delivered { .. } => {
                self.delivered_total.fetch_add(1, Ordering::Relaxed);
            }
            ForwardDeliveryStatus::HttpRejected { .. } => {
                self.rejected_total.fetch_add(1, Ordering::Relaxed);
            }
            ForwardDeliveryStatus::TimedOut => {
                self.timed_out_total.fetch_add(1, Ordering::Relaxed);
            }
            ForwardDeliveryStatus::ResponseTooLarge { .. } => {
                self.response_too_large_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            ForwardDeliveryStatus::Failed { .. } => {
                self.failed_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn record_outcome_drop(&self) {
        self.outcome_dropped_total.fetch_add(1, Ordering::Relaxed);
    }
}

/// Minimal side-effect boundary required by ActionDispatcher for ForwardToSink.
pub trait EventForwarder {
    fn try_forward(&self, request: ForwardRequest) -> Result<ForwardReceipt, DispatchError>;
}

/// Bounded HTTP forwarder. Clone is cheap and only clones queue and registry handles.
#[derive(Debug, Clone)]
pub struct BoundedHttpForwarder {
    tx: mpsc::Sender<QueuedForwardRequest>,
    registry: Arc<SinkRegistry>,
    limits: ForwardLimits,
    stats: Arc<ForwarderStats>,
}

impl BoundedHttpForwarder {
    /// Creates a bounded forwarder using the default egress policy.
    pub fn try_channel(
        sinks: Vec<SinkDefinition>,
        limits: ForwardLimits,
    ) -> Result<
        (
            Self,
            HttpForwardWorker,
            mpsc::Receiver<ForwardDeliveryOutcome>,
        ),
        DispatchError,
    > {
        Self::try_channel_with_policy(sinks, limits, EgressPolicy::default())
    }

    /// Creates a bounded forwarder and a worker. The caller owns worker lifecycle.
    pub fn try_channel_with_policy(
        sinks: Vec<SinkDefinition>,
        limits: ForwardLimits,
        egress_policy: EgressPolicy,
    ) -> Result<
        (
            Self,
            HttpForwardWorker,
            mpsc::Receiver<ForwardDeliveryOutcome>,
        ),
        DispatchError,
    > {
        validate_limits(limits)?;

        let registry = Arc::new(SinkRegistry::compile(sinks, limits, &egress_policy)?);
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            .map_err(|_| DispatchError::InvalidConfig {
                reason: "failed to build HTTP forwarder client",
            })?;

        let (tx, rx) = mpsc::channel(limits.queue_capacity);
        let (result_tx, result_rx) = mpsc::channel(limits.result_queue_capacity);
        let stats = Arc::new(ForwarderStats::default());
        let forwarder = Self {
            tx,
            registry,
            limits,
            stats: Arc::clone(&stats),
        };
        let worker = HttpForwardWorker {
            rx,
            result_tx,
            client,
            limits,
            stats,
        };

        Ok((forwarder, worker, result_rx))
    }

    pub fn stats(&self) -> Arc<ForwarderStats> {
        Arc::clone(&self.stats)
    }
}

impl EventForwarder for BoundedHttpForwarder {
    fn try_forward(&self, request: ForwardRequest) -> Result<ForwardReceipt, DispatchError> {
        let target = self.registry.target(&request.sink_id)?;
        let event_id = request.event.id.clone();
        let payload = encode_payload(&request, self.limits.max_request_body_bytes)?;
        let sink_id = request.sink_id;

        self.tx
            .try_send(QueuedForwardRequest {
                event_id,
                sink_id: sink_id.clone(),
                target,
                payload,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    self.stats.record_backpressure();
                    DispatchError::ForwarderBackpressure
                }
                mpsc::error::TrySendError::Closed(_) => DispatchError::ForwarderUnavailable,
            })?;

        self.stats.record_acceptance();

        Ok(ForwardReceipt {
            sink_id,
            accepted: true,
        })
    }
}

/// Explicit no-op implementation for deployments that do not enable forwarding yet.
#[derive(Debug, Copy, Clone, Default)]
pub struct DisabledForwarder;

impl EventForwarder for DisabledForwarder {
    fn try_forward(&self, request: ForwardRequest) -> Result<ForwardReceipt, DispatchError> {
        Err(DispatchError::SinkNotFound {
            sink_id: request.sink_id,
        })
    }
}

#[derive(Debug)]
pub struct HttpForwardWorker {
    rx: mpsc::Receiver<QueuedForwardRequest>,
    result_tx: mpsc::Sender<ForwardDeliveryOutcome>,
    client: Client,
    limits: ForwardLimits,
    stats: Arc<ForwarderStats>,
}

impl HttpForwardWorker {
    /// Runs one bounded worker loop. Spawn a fixed number of these if parallelism is needed.
    pub async fn run(mut self, mut shutdown_rx: watch::Receiver<bool>) {
        let semaphore = Arc::new(Semaphore::new(self.limits.max_in_flight));
        let mut tasks = JoinSet::new();

        loop {
            tokio::select! {
                biased;

                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        self.rx.close();
                        break;
                    }
                }

                joined = tasks.join_next(), if !tasks.is_empty() => {
                    record_join_result(joined, &self.stats);
                }

                request = self.rx.recv() => {
                    let Some(request) = request else {
                        break;
                    };

                    let permit = match Arc::clone(&semaphore).acquire_owned().await {
                        Ok(permit) => permit,
                        Err(_) => break,
                    };
                    spawn_delivery(
                        &mut tasks,
                        self.client.clone(),
                        self.result_tx.clone(),
                        self.limits,
                        Arc::clone(&self.stats),
                        request,
                        permit,
                    );
                }
            }
        }

        self.drain_remaining(tasks, semaphore).await;
    }

    async fn drain_remaining(mut self, mut tasks: JoinSet<()>, semaphore: Arc<Semaphore>) {
        let drain = async {
            while let Some(request) = self.rx.recv().await {
                let permit = match Arc::clone(&semaphore).acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => break,
                };
                spawn_delivery(
                    &mut tasks,
                    self.client.clone(),
                    self.result_tx.clone(),
                    self.limits,
                    Arc::clone(&self.stats),
                    request,
                    permit,
                );
            }

            while let Some(joined) = tasks.join_next().await {
                record_join_result(Some(joined), &self.stats);
            }
        };

        if timeout(self.limits.graceful_shutdown_timeout, drain)
            .await
            .is_err()
        {
            tasks.abort_all();
            while let Some(joined) = tasks.join_next().await {
                record_join_result(Some(joined), &self.stats);
            }
        }
    }
}

fn spawn_delivery(
    tasks: &mut JoinSet<()>,
    client: Client,
    result_tx: mpsc::Sender<ForwardDeliveryOutcome>,
    limits: ForwardLimits,
    stats: Arc<ForwarderStats>,
    request: QueuedForwardRequest,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    tasks.spawn(async move {
        let outcome = execute_request(&client, request, limits).await;
        stats.record_outcome(&outcome);

        if result_tx.try_send(outcome).is_err() {
            stats.record_outcome_drop();
        }

        drop(permit);
    });
}

fn record_join_result(joined: Option<Result<(), tokio::task::JoinError>>, stats: &ForwarderStats) {
    if matches!(joined, Some(Err(_))) {
        stats.failed_total.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct QueuedForwardRequest {
    event_id: EventId,
    sink_id: SinkId,
    target: Arc<ForwardTarget>,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct SinkRegistry {
    sinks: HashMap<SinkId, SinkState>,
}

impl SinkRegistry {
    fn compile(
        sinks: Vec<SinkDefinition>,
        limits: ForwardLimits,
        egress_policy: &EgressPolicy,
    ) -> Result<Self, DispatchError> {
        let mut compiled = HashMap::with_capacity(sinks.len());

        for sink in sinks {
            if compiled.contains_key(&sink.id) {
                return Err(DispatchError::InvalidConfig {
                    reason: "duplicate sink id",
                });
            }

            let state = if !sink.enabled {
                SinkState::Disabled
            } else {
                match sink.kind {
                    SinkKind::Webhook {
                        url,
                        method,
                        headers,
                        timeout,
                    } => SinkState::Enabled(Arc::new(ForwardTarget {
                        url: parse_url(&url, egress_policy)?,
                        method: map_method(method),
                        headers: compile_headers(headers, limits)?,
                        timeout: clamp_timeout(timeout, limits.max_timeout)?,
                    })),
                    SinkKind::Database { .. } => SinkState::Unsupported,
                }
            };

            compiled.insert(sink.id, state);
        }

        Ok(Self { sinks: compiled })
    }

    fn target(&self, sink_id: &SinkId) -> Result<Arc<ForwardTarget>, DispatchError> {
        match self.sinks.get(sink_id) {
            Some(SinkState::Enabled(target)) => Ok(Arc::clone(target)),
            Some(SinkState::Disabled) => Err(DispatchError::SinkDisabled {
                sink_id: sink_id.clone(),
            }),
            Some(SinkState::Unsupported) => Err(DispatchError::UnsupportedSinkKind {
                sink_id: sink_id.clone(),
            }),
            None => Err(DispatchError::SinkNotFound {
                sink_id: sink_id.clone(),
            }),
        }
    }
}

#[derive(Debug)]
enum SinkState {
    Enabled(Arc<ForwardTarget>),
    Disabled,
    Unsupported,
}

#[derive(Debug)]
struct ForwardTarget {
    url: Url,
    method: Method,
    headers: HeaderMap,
    timeout: Duration,
}

async fn execute_request(
    client: &Client,
    request: QueuedForwardRequest,
    limits: ForwardLimits,
) -> ForwardDeliveryOutcome {
    let event_id = request.event_id.clone();
    let sink_id = request.sink_id.clone();

    timeout(
        request.target.timeout,
        execute_request_inner(client, request, limits),
    )
    .await
    .unwrap_or(ForwardDeliveryOutcome {
        event_id,
        sink_id,
        status: ForwardDeliveryStatus::TimedOut,
    })
}

async fn execute_request_inner(
    client: &Client,
    request: QueuedForwardRequest,
    limits: ForwardLimits,
) -> ForwardDeliveryOutcome {
    let event_id = request.event_id;
    let sink_id = request.sink_id;
    let target = request.target;

    let response = match client
        .request(target.method.clone(), target.url.clone())
        .headers(target.headers.clone())
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(request.payload)
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => {
            return ForwardDeliveryOutcome {
                event_id,
                sink_id,
                status: ForwardDeliveryStatus::Failed {
                    reason: ForwardFailureReason::RequestFailed,
                },
            };
        }
    };

    let status = response.status();
    let response_body_bytes =
        match read_limited_response(response, limits.max_response_body_bytes).await {
            Ok(bytes) => bytes,
            Err(ResponseReadError::TooLarge) => {
                return ForwardDeliveryOutcome {
                    event_id,
                    sink_id,
                    status: ForwardDeliveryStatus::ResponseTooLarge {
                        max: limits.max_response_body_bytes,
                    },
                };
            }
            Err(ResponseReadError::ReadFailed) => {
                return ForwardDeliveryOutcome {
                    event_id,
                    sink_id,
                    status: ForwardDeliveryStatus::Failed {
                        reason: ForwardFailureReason::ResponseReadFailed,
                    },
                };
            }
        };

    let status = if status.is_success() {
        ForwardDeliveryStatus::Delivered {
            http_status: status.as_u16(),
            response_body_bytes,
        }
    } else {
        ForwardDeliveryStatus::HttpRejected {
            http_status: status.as_u16(),
            response_body_bytes,
        }
    };

    ForwardDeliveryOutcome {
        event_id,
        sink_id,
        status,
    }
}

fn encode_payload(request: &ForwardRequest, max_bytes: usize) -> Result<Vec<u8>, DispatchError> {
    let payload = match &request.projection {
        Some(projection) => serde_json::to_vec(projection),
        None => serde_json::to_vec(&request.event),
    }
    .map_err(|_| DispatchError::ForwardPayloadEncode {
        reason: "failed to serialize forward payload",
    })?;

    if payload.len() > max_bytes {
        return Err(DispatchError::ForwardPayloadTooLarge {
            actual: payload.len(),
            max: max_bytes,
        });
    }

    Ok(payload)
}

async fn read_limited_response(
    response: reqwest::Response,
    max_bytes: usize,
) -> Result<usize, ResponseReadError> {
    if let Some(content_length) = response.content_length()
        && content_length > max_bytes as u64
    {
        return Err(ResponseReadError::TooLarge);
    }

    let mut total = 0usize;
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| ResponseReadError::ReadFailed)?;
        total = total
            .checked_add(chunk.len())
            .ok_or(ResponseReadError::TooLarge)?;

        if total > max_bytes {
            return Err(ResponseReadError::TooLarge);
        }
    }

    Ok(total)
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ResponseReadError {
    TooLarge,
    ReadFailed,
}

fn validate_limits(limits: ForwardLimits) -> Result<(), DispatchError> {
    if limits.queue_capacity == 0 {
        return Err(DispatchError::InvalidConfig {
            reason: "forward queue capacity must be greater than zero",
        });
    }

    if limits.result_queue_capacity == 0 {
        return Err(DispatchError::InvalidConfig {
            reason: "forward result queue capacity must be greater than zero",
        });
    }

    if limits.max_request_body_bytes == 0 {
        return Err(DispatchError::InvalidConfig {
            reason: "max forward request body bytes must be greater than zero",
        });
    }

    if limits.max_response_body_bytes == 0 {
        return Err(DispatchError::InvalidConfig {
            reason: "max forward response body bytes must be greater than zero",
        });
    }

    if limits.max_headers == 0 {
        return Err(DispatchError::InvalidConfig {
            reason: "max forward headers must be greater than zero",
        });
    }

    if limits.max_header_name_bytes == 0 || limits.max_header_value_bytes == 0 {
        return Err(DispatchError::InvalidConfig {
            reason: "max forward header bytes must be greater than zero",
        });
    }

    if limits.max_in_flight == 0 {
        return Err(DispatchError::InvalidConfig {
            reason: "max forward in-flight requests must be greater than zero",
        });
    }

    if limits.max_timeout < MIN_TIMEOUT {
        return Err(DispatchError::InvalidConfig {
            reason: "max forward timeout is too small",
        });
    }

    if limits.graceful_shutdown_timeout < MIN_TIMEOUT {
        return Err(DispatchError::InvalidConfig {
            reason: "forward graceful shutdown timeout is too small",
        });
    }

    Ok(())
}

fn parse_url(value: &str, policy: &EgressPolicy) -> Result<Url, DispatchError> {
    let url = Url::parse(value).map_err(|_| DispatchError::InvalidConfig {
        reason: "sink webhook URL is invalid",
    })?;

    match url.scheme() {
        "https" => {}
        "http" if !policy.require_https => {}
        "http" => {
            return Err(DispatchError::InvalidConfig {
                reason: "sink webhook URL must use HTTPS",
            });
        }
        _ => {
            return Err(DispatchError::InvalidConfig {
                reason: "sink webhook URL must use HTTP or HTTPS",
            });
        }
    }

    let Some(host) = url.host_str() else {
        return Err(DispatchError::InvalidConfig {
            reason: "sink webhook URL must include a host",
        });
    };

    validate_egress_host(host, policy)?;
    Ok(url)
}

fn validate_egress_host(host: &str, policy: &EgressPolicy) -> Result<(), DispatchError> {
    let normalized = host.trim_end_matches('.').to_ascii_lowercase();

    if !policy.allowed_hosts.is_empty()
        && !policy
            .allowed_hosts
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(&normalized))
    {
        return Err(DispatchError::InvalidConfig {
            reason: "sink webhook host is not allowed by egress policy",
        });
    }

    if matches!(normalized.as_str(), "localhost" | "localhost.localdomain") {
        return Err(DispatchError::InvalidConfig {
            reason: "sink webhook host must not target localhost",
        });
    }

    if let Ok(ip) = normalized.parse::<IpAddr>()
        && !policy.allow_private_networks
        && is_blocked_ip(ip)
    {
        return Err(DispatchError::InvalidConfig {
            reason: "sink webhook host targets a private or reserved network",
        });
    }

    Ok(())
}

fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_multicast()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.is_unspecified()
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_multicast()
                || ip.is_unspecified()
                || matches!(ip.segments()[0] & 0xfe00, 0xfc00)
                || matches!(ip.segments()[0] & 0xffc0, 0xfe80)
        }
    }
}

fn compile_headers(
    headers: Vec<pipe_bolt_domain::HttpHeaderTemplate>,
    limits: ForwardLimits,
) -> Result<HeaderMap, DispatchError> {
    if headers.len() > limits.max_headers {
        return Err(DispatchError::InvalidConfig {
            reason: "sink webhook has too many headers",
        });
    }

    let mut compiled = HeaderMap::with_capacity(headers.len());

    for header in headers {
        if header.name.len() > limits.max_header_name_bytes {
            return Err(DispatchError::InvalidConfig {
                reason: "sink webhook header name is too large",
            });
        }

        if header.value.expose_secret().len() > limits.max_header_value_bytes {
            return Err(DispatchError::InvalidConfig {
                reason: "sink webhook header value is too large",
            });
        }

        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| {
            DispatchError::InvalidConfig {
                reason: "sink webhook header name is invalid",
            }
        })?;
        let value = HeaderValue::from_str(header.value.expose_secret()).map_err(|_| {
            DispatchError::InvalidConfig {
                reason: "sink webhook header value is invalid",
            }
        })?;

        if is_blocked_header(&name) {
            return Err(DispatchError::InvalidConfig {
                reason: "sink webhook header is controlled by the HTTP client",
            });
        }

        compiled.insert(name, value);
    }

    Ok(compiled)
}

fn is_blocked_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "content-length"
            | "host"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn clamp_timeout(timeout: Duration, max_timeout: Duration) -> Result<Duration, DispatchError> {
    if timeout < MIN_TIMEOUT {
        return Err(DispatchError::InvalidConfig {
            reason: "sink webhook timeout is too small",
        });
    }

    Ok(timeout.min(max_timeout))
}

fn map_method(method: HttpMethod) -> Method {
    match method {
        HttpMethod::Post => Method::POST,
        HttpMethod::Put => Method::PUT,
        HttpMethod::Patch => Method::PATCH,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use pipe_bolt_domain::{
        BrokerId, DecodedPayload, EventId, FieldValue, ProjectId, RouteId, SinkDefinition,
        TopicName,
    };
    use serde_json::json;
    use time::OffsetDateTime;

    use super::*;

    fn event() -> NormalizedEvent {
        let mut fields = BTreeMap::new();
        fields.insert(
            "temperature".to_owned(),
            FieldValue::Number(serde_json::Number::from(42)),
        );

        NormalizedEvent {
            id: EventId::new("evt-test").unwrap(),
            correlation_id: "evt-test".to_owned(),
            project_id: ProjectId::new("project-1").unwrap(),
            broker_id: BrokerId::new("broker-1").unwrap(),
            route_id: RouteId::new("route-1").unwrap(),
            schema_mapping_id: None,
            topic: TopicName::new("devices/device-1/telemetry").unwrap(),
            device_id: Some("device-1".to_owned()),
            event_type: "telemetry".to_owned(),
            received_at: OffsetDateTime::UNIX_EPOCH,
            payload_size_bytes: 16,
            payload: DecodedPayload::Json(json!({ "temperature": 42 })),
            fields,
            raw: None,
            normalization_errors: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    fn webhook_sink(id: &str, enabled: bool) -> SinkDefinition {
        SinkDefinition {
            id: SinkId::new(id).unwrap(),
            name: id.to_owned(),
            enabled,
            kind: SinkKind::Webhook {
                url: "https://example.com/events".to_owned(),
                method: HttpMethod::Post,
                headers: Vec::new(),
                timeout: Duration::from_secs(1),
            },
        }
    }

    #[test]
    fn accepts_valid_forward_request_into_bounded_queue() {
        let sink_id = SinkId::new("sink-1").unwrap();
        let (forwarder, _worker, _results) = BoundedHttpForwarder::try_channel(
            vec![webhook_sink("sink-1", true)],
            ForwardLimits {
                queue_capacity: 1,
                result_queue_capacity: 1,
                max_request_body_bytes: 64 * 1024,
                max_response_body_bytes: 64 * 1024,
                max_timeout: Duration::from_secs(5),
                ..ForwardLimits::default()
            },
        )
        .unwrap();

        let receipt = forwarder
            .try_forward(ForwardRequest {
                event: event(),
                sink_id: sink_id.clone(),
                projection: None,
            })
            .unwrap();

        assert_eq!(
            receipt,
            ForwardReceipt {
                sink_id,
                accepted: true
            }
        );
    }

    #[test]
    fn reports_forward_backpressure_without_blocking() {
        let sink_id = SinkId::new("sink-1").unwrap();
        let (forwarder, _worker, _results) = BoundedHttpForwarder::try_channel(
            vec![webhook_sink("sink-1", true)],
            ForwardLimits {
                queue_capacity: 1,
                result_queue_capacity: 1,
                max_request_body_bytes: 64 * 1024,
                max_response_body_bytes: 64 * 1024,
                max_timeout: Duration::from_secs(5),
                ..ForwardLimits::default()
            },
        )
        .unwrap();

        forwarder
            .try_forward(ForwardRequest {
                event: event(),
                sink_id: sink_id.clone(),
                projection: None,
            })
            .unwrap();

        let error = forwarder
            .try_forward(ForwardRequest {
                event: event(),
                sink_id,
                projection: None,
            })
            .unwrap_err();

        assert_eq!(error, DispatchError::ForwarderBackpressure);
    }

    #[test]
    fn rejects_unknown_sink_before_enqueue() {
        let (forwarder, _worker, _results) = BoundedHttpForwarder::try_channel(
            vec![webhook_sink("sink-1", true)],
            ForwardLimits::default(),
        )
        .unwrap();
        let missing = SinkId::new("missing").unwrap();

        let error = forwarder
            .try_forward(ForwardRequest {
                event: event(),
                sink_id: missing.clone(),
                projection: None,
            })
            .unwrap_err();

        assert_eq!(error, DispatchError::SinkNotFound { sink_id: missing });
    }

    #[test]
    fn rejects_disabled_sink_before_enqueue() {
        let sink_id = SinkId::new("sink-1").unwrap();
        let (forwarder, _worker, _results) = BoundedHttpForwarder::try_channel(
            vec![webhook_sink("sink-1", false)],
            ForwardLimits::default(),
        )
        .unwrap();

        let error = forwarder
            .try_forward(ForwardRequest {
                event: event(),
                sink_id: sink_id.clone(),
                projection: None,
            })
            .unwrap_err();

        assert_eq!(error, DispatchError::SinkDisabled { sink_id });
    }

    #[test]
    fn rejects_oversized_request_payload_before_enqueue() {
        let sink_id = SinkId::new("sink-1").unwrap();
        let (forwarder, _worker, _results) = BoundedHttpForwarder::try_channel(
            vec![webhook_sink("sink-1", true)],
            ForwardLimits {
                queue_capacity: 1,
                result_queue_capacity: 1,
                max_request_body_bytes: 8,
                max_response_body_bytes: 64 * 1024,
                max_timeout: Duration::from_secs(5),
                ..ForwardLimits::default()
            },
        )
        .unwrap();

        let error = forwarder
            .try_forward(ForwardRequest {
                event: event(),
                sink_id,
                projection: None,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            DispatchError::ForwardPayloadTooLarge { .. }
        ));
    }

    #[test]
    fn rejects_http_url_by_default() {
        let sink = SinkDefinition {
            id: SinkId::new("sink-1").unwrap(),
            name: "sink-1".to_owned(),
            enabled: true,
            kind: SinkKind::Webhook {
                url: "http://example.com/events".to_owned(),
                method: HttpMethod::Post,
                headers: Vec::new(),
                timeout: Duration::from_secs(1),
            },
        };

        let error =
            BoundedHttpForwarder::try_channel(vec![sink], ForwardLimits::default()).unwrap_err();

        assert!(matches!(error, DispatchError::InvalidConfig { .. }));
    }

    #[test]
    fn rejects_private_ip_webhook_target() {
        let sink = SinkDefinition {
            id: SinkId::new("sink-1").unwrap(),
            name: "sink-1".to_owned(),
            enabled: true,
            kind: SinkKind::Webhook {
                url: "https://127.0.0.1/events".to_owned(),
                method: HttpMethod::Post,
                headers: Vec::new(),
                timeout: Duration::from_secs(1),
            },
        };

        let error =
            BoundedHttpForwarder::try_channel(vec![sink], ForwardLimits::default()).unwrap_err();

        assert!(matches!(error, DispatchError::InvalidConfig { .. }));
    }

    #[test]
    fn rejects_hop_by_hop_headers() {
        let sink = SinkDefinition {
            id: SinkId::new("sink-1").unwrap(),
            name: "sink-1".to_owned(),
            enabled: true,
            kind: SinkKind::Webhook {
                url: "https://example.com/events".to_owned(),
                method: HttpMethod::Post,
                headers: vec![pipe_bolt_domain::HttpHeaderTemplate {
                    name: "Connection".to_owned(),
                    value: pipe_bolt_domain::SecretString::new("close").unwrap(),
                }],
                timeout: Duration::from_secs(1),
            },
        };

        let error =
            BoundedHttpForwarder::try_channel(vec![sink], ForwardLimits::default()).unwrap_err();

        assert!(matches!(error, DispatchError::InvalidConfig { .. }));
    }

    #[test]
    fn records_backpressure_metric() {
        let sink_id = SinkId::new("sink-1").unwrap();
        let (forwarder, _worker, _results) = BoundedHttpForwarder::try_channel(
            vec![webhook_sink("sink-1", true)],
            ForwardLimits {
                queue_capacity: 1,
                result_queue_capacity: 1,
                max_request_body_bytes: 64 * 1024,
                max_response_body_bytes: 64 * 1024,
                max_headers: 32,
                max_header_name_bytes: 128,
                max_header_value_bytes: 4096,
                max_in_flight: 1,
                max_timeout: Duration::from_secs(5),
                graceful_shutdown_timeout: Duration::from_secs(1),
            },
        )
        .unwrap();

        forwarder
            .try_forward(ForwardRequest {
                event: event(),
                sink_id: sink_id.clone(),
                projection: None,
            })
            .unwrap();

        let error = forwarder
            .try_forward(ForwardRequest {
                event: event(),
                sink_id,
                projection: None,
            })
            .unwrap_err();

        assert_eq!(error, DispatchError::ForwarderBackpressure);
        assert_eq!(forwarder.stats().snapshot().backpressure_total, 1);
    }
}
