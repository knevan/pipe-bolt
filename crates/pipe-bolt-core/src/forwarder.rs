use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use pipe_bolt_domain::{EventId, HttpMethod, NormalizedEvent, SinkDefinition, SinkId, SinkKind};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Method, Url};
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

use crate::error::DispatchError;

const DEFAULT_FORWARD_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_FORWARD_RESULT_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_TIMEOUT: Duration = Duration::from_secs(30);
const MIN_TIMEOUT: Duration = Duration::from_millis(10);

/// Runtime limits for bounded sink forwarding.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ForwardLimits {
    pub queue_capacity: usize,
    pub result_queue_capacity: usize,
    pub max_request_body_bytes: usize,
    pub max_response_body_bytes: usize,
    pub max_timeout: Duration,
}

impl Default for ForwardLimits {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_FORWARD_QUEUE_CAPACITY,
            result_queue_capacity: DEFAULT_FORWARD_RESULT_QUEUE_CAPACITY,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            max_response_body_bytes: DEFAULT_MAX_RESPONSE_BODY_BYTES,
            max_timeout: DEFAULT_MAX_TIMEOUT,
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
        reason: &'static str,
    },
}

/// Minimal side-effect boundary required by ActionDispatcher for ForwardToSink.
pub trait EventForwarder {
    fn try_forward(&self, request: ForwardRequest) -> Result<ForwardReceipt, DispatchError>;
}

/// Bounded HTTP forwarder. Clone is cheap and only clones queue and registry handles.
#[derive(Clone)]
pub struct BoundedHttpForwarder {
    tx: mpsc::Sender<QueuedForwardRequest>,
    registry: Arc<SinkRegistry>,
    limits: ForwardLimits,
}

impl BoundedHttpForwarder {
    /// Creates a bounded forwarder and a worker. The caller owns worker lifecycle.
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
        validate_limits(limits)?;

        let registry = Arc::new(SinkRegistry::compile(sinks, limits)?);
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            .map_err(|_| DispatchError::InvalidConfig {
                reason: "failed to build HTTP forwarder client",
            })?;

        let (tx, rx) = mpsc::channel(limits.queue_capacity);
        let (result_tx, result_rx) = mpsc::channel(limits.result_queue_capacity);
        let forwarder = Self {
            tx,
            registry,
            limits,
        };
        let worker = HttpForwardWorker {
            rx,
            result_tx,
            client,
            limits,
        };

        Ok((forwarder, worker, result_rx))
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
                mpsc::error::TrySendError::Full(_) => DispatchError::ForwarderBackpressure,
                mpsc::error::TrySendError::Closed(_) => DispatchError::ForwarderUnavailable,
            })?;

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

pub struct HttpForwardWorker {
    rx: mpsc::Receiver<QueuedForwardRequest>,
    result_tx: mpsc::Sender<ForwardDeliveryOutcome>,
    client: Client,
    limits: ForwardLimits,
}

impl HttpForwardWorker {
    /// Runs one bounded worker loop. Spawn a fixed number of these if parallelism is needed.
    pub async fn run(mut self, mut shutdown_rx: watch::Receiver<bool>) {
        loop {
            tokio::select! {
                biased;

                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }

                request = self.rx.recv() => {
                    let Some(request) = request else {
                        break;
                    };

                    let outcome = execute_request(&self.client, request, self.limits).await;
                    let _ = self.result_tx.try_send(outcome);
                }
            }
        }
    }
}

#[derive(Debug)]
struct QueuedForwardRequest {
    event_id: EventId,
    sink_id: SinkId,
    target: Arc<ForwardTarget>,
    payload: Vec<u8>,
}

struct SinkRegistry {
    sinks: HashMap<SinkId, SinkState>,
}

impl SinkRegistry {
    fn compile(sinks: Vec<SinkDefinition>, limits: ForwardLimits) -> Result<Self, DispatchError> {
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
                        url: parse_url(&url)?,
                        method: map_method(method),
                        headers: compile_headers(headers)?,
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
    let event_id = request.event_id;
    let sink_id = request.sink_id;
    let target = request.target;

    let send = client
        .request(target.method.clone(), target.url.clone())
        .headers(target.headers.clone())
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(request.payload)
        .send();

    let response = match timeout(target.timeout, send).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) => {
            return ForwardDeliveryOutcome {
                event_id,
                sink_id,
                status: ForwardDeliveryStatus::Failed {
                    reason: "HTTP forward request failed",
                },
            };
        }
        Err(_) => {
            return ForwardDeliveryOutcome {
                event_id,
                sink_id,
                status: ForwardDeliveryStatus::TimedOut,
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
                        reason: "HTTP forward response read failed",
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

    if limits.max_timeout < MIN_TIMEOUT {
        return Err(DispatchError::InvalidConfig {
            reason: "max forward timeout is too small",
        });
    }

    Ok(())
}

fn parse_url(value: &str) -> Result<Url, DispatchError> {
    let url = Url::parse(value).map_err(|_| DispatchError::InvalidConfig {
        reason: "sink webhook URL is invalid",
    })?;

    match url.scheme() {
        "http" | "https" => Ok(url),
        _ => Err(DispatchError::InvalidConfig {
            reason: "sink webhook URL must use HTTP or HTTPS",
        }),
    }
}

fn compile_headers(
    headers: Vec<pipe_bolt_domain::HttpHeaderTemplate>,
) -> Result<HeaderMap, DispatchError> {
    let mut compiled = HeaderMap::with_capacity(headers.len());

    for header in headers {
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

        if name == reqwest::header::CONTENT_LENGTH || name == reqwest::header::HOST {
            return Err(DispatchError::InvalidConfig {
                reason: "sink webhook header is controlled by the HTTP client",
            });
        }

        compiled.insert(name, value);
    }

    Ok(compiled)
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
}
