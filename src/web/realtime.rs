use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_stream::stream;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use futures_util::{SinkExt, StreamExt};
use salvo::prelude::*;
use salvo::websocket::{Message, WebSocket};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{mpsc, watch};
use tokio::time::{MissedTickBehavior, interval, timeout};

use crate::bus::TelemetryEvent;
use crate::error::MqttEngineError;
use crate::mqtt::engine::MqttHandle;

const DEFAULT_WEBSOCKET_CLIENT_BUFFER: usize = 64;
const DEFAULT_WEBSOCKET_PING_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_WEBSOCKET_SEND_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_SSE_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);
const WEBSOCKET_MAX_MESSAGE_SIZE: usize = 64 * 1024;
const WEBSOCKET_MAX_FRAME_SIZE: usize = 64 * 1024;

#[derive(Clone)]
pub struct RealtimeBridgeState {
    mqtt: MqttHandle,
    websocket_client_buffer: usize,
    websocket_ping_interval: Duration,
    websocket_send_timeout: Duration,
    sse_keep_alive_interval: Duration,
}

impl RealtimeBridgeState {
    pub fn new(mqtt: MqttHandle) -> Self {
        Self {
            mqtt,
            websocket_client_buffer: DEFAULT_WEBSOCKET_CLIENT_BUFFER,
            websocket_ping_interval: DEFAULT_WEBSOCKET_PING_INTERVAL,
            websocket_send_timeout: DEFAULT_WEBSOCKET_SEND_TIMEOUT,
            sse_keep_alive_interval: DEFAULT_SSE_KEEP_ALIVE_INTERVAL,
        }
    }

    pub fn with_websocket_client_buffer(mut self, capacity: usize) -> Self {
        self.websocket_client_buffer = capacity.max(1);
        self
    }

    pub fn with_websocket_ping_interval(mut self, interval: Duration) -> Self {
        if !interval.is_zero() {
            self.websocket_ping_interval = interval;
        }
        self
    }

    pub fn with_websocket_send_timeout(mut self, timeout: Duration) -> Self {
        if !timeout.is_zero() {
            self.websocket_send_timeout = timeout;
        }
        self
    }

    pub fn with_sse_keep_alive_interval(mut self, interval: Duration) -> Self {
        if !interval.is_zero() {
            self.sse_keep_alive_interval = interval;
        }
        self
    }

    fn subscribe_telemetry(&self) -> tokio::sync::broadcast::Receiver<TelemetryEvent> {
        self.mqtt.subscribe_telemetry()
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct TelemetryFilter {
    pub device: Option<String>,
    pub topic: Option<String>,
    pub topic_prefix: Option<String>,
    pub event_type: Option<String>,
}

impl TelemetryFilter {
    fn normalized(self) -> Self {
        Self {
            device: normalize_optional(self.device),
            topic: normalize_optional(self.topic),
            topic_prefix: normalize_optional(self.topic_prefix),
            event_type: normalize_optional(self.event_type),
        }
    }

    fn matches(&self, event: &TelemetryEvent) -> bool {
        if let Some(expected) = self.device.as_deref()
            && telemetry_device(&event.topic) != Some(expected)
        {
            return false;
        }

        if let Some(expected) = self.topic.as_deref()
            && event.topic != expected
        {
            return false;
        }

        if let Some(expected) = self.topic_prefix.as_deref()
            && !topic_matches_prefix(&event.topic, expected)
        {
            return false;
        }

        if let Some(expected) = self.event_type.as_deref()
            && telemetry_event_type(&event.topic) != Some(expected)
        {
            return false;
        }

        true
    }
}

#[derive(Debug, Serialize)]
pub struct TelemetryPayload {
    pub topic: String,
    pub device: Option<String>,
    pub event_type: Option<String>,
    pub payload_base64: String,
    pub payload_utf8: Option<String>,
    pub received_at_ms: u128,
}

impl TelemetryPayload {
    fn from_event(event: TelemetryEvent) -> Self {
        let device = telemetry_device(&event.topic).map(str::to_owned);
        let event_type = telemetry_event_type(&event.topic).map(str::to_owned);
        let payload_utf8 = String::from_utf8(event.payload.clone()).ok();

        Self {
            topic: event.topic,
            device,
            event_type,
            payload_base64: BASE64_STANDARD.encode(event.payload),
            payload_utf8,
            received_at_ms: system_time_ms(event.received_at),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BridgeServerMessage {
    Ready {
        transport: &'static str,
        filter: TelemetryFilterSnapshot,
    },
    Telemetry {
        data: TelemetryPayload,
    },
    Lagged {
        skipped: u64,
    },
    FilterUpdated {
        filter: TelemetryFilterSnapshot,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct TelemetryFilterSnapshot {
    pub device: Option<String>,
    pub topic: Option<String>,
    pub topic_prefix: Option<String>,
    pub event_type: Option<String>,
}

impl From<&TelemetryFilter> for TelemetryFilterSnapshot {
    fn from(value: &TelemetryFilter) -> Self {
        Self {
            device: value.device.clone(),
            topic: value.topic.clone(),
            topic_prefix: value.topic_prefix.clone(),
            event_type: value.event_type.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BridgeClientMessage {
    Subscribe {
        device: Option<String>,
        topic: Option<String>,
        topic_prefix: Option<String>,
        event_type: Option<String>,
    },
    Ping,
}

pub async fn serve_realtime_bridge(
    bind_addr: impl Into<SocketAddr>,
    mqtt: MqttHandle,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), MqttEngineError> {
    let state = RealtimeBridgeState::new(mqtt);
    serve_realtime_bridge_with_state(bind_addr, state, shutdown_rx).await
}

pub async fn serve_realtime_bridge_with_state(
    bind_addr: impl Into<SocketAddr>,
    state: RealtimeBridgeState,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), MqttEngineError> {
    let addr = bind_addr.into();
    let acceptor = TcpListener::new(addr).bind().await;
    let server = Server::new(acceptor);
    let router = realtime_router(state);

    tokio::select! {
        _ = server.serve(router) => Ok(()),
        _ = shutdown_rx.changed() => Ok(()),
    }
}

pub fn realtime_router(state: RealtimeBridgeState) -> Router {
    Router::new()
        .hoop(affix_state::inject(state))
        .push(Router::with_path("health").get(health))
        .push(
            Router::with_path("realtime").push(
                Router::with_path("telemetry")
                    .push(Router::with_path("sse").get(telemetry_sse))
                    .push(Router::with_path("ws").get(telemetry_ws)),
            ),
        )
}

#[handler]
async fn health(res: &mut Response) {
    res.render(Json(serde_json::json!({ "status": "ok" })));
}

#[handler]
async fn telemetry_sse(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), StatusError> {
    let state = depot
        .obtain::<RealtimeBridgeState>()
        .map_err(|_| StatusError::internal_server_error().brief("realtime bridge state missing"))?
        .clone();
    let filter = parse_filter(req)?.normalized();
    let mut rx = state.subscribe_telemetry();

    let event_stream = stream! {
        yield Ok::<_, Infallible>(sse_json_event(
            "bridge_ready",
            BridgeServerMessage::Ready {
                transport: "sse",
                filter: TelemetryFilterSnapshot::from(&filter),
            },
        ));

        loop {
            match rx.recv().await {
                Ok(event) => {
                    if !filter.matches(&event) {
                        continue;
                    }

                    yield Ok(sse_json_event(
                        "telemetry",
                        BridgeServerMessage::Telemetry {
                            data: TelemetryPayload::from_event(event),
                        },
                    ));
                }
                Err(RecvError::Lagged(skipped)) => {
                    yield Ok(sse_json_event(
                        "bridge_lagged",
                        BridgeServerMessage::Lagged { skipped },
                    ));
                }
                Err(RecvError::Closed) => {
                    break;
                }
            }
        }
    };

    SseKeepAlive::new(event_stream)
        .max_interval(state.sse_keep_alive_interval)
        .comment("ping")
        .stream(res);

    Ok(())
}

#[handler]
async fn telemetry_ws(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), StatusError> {
    let state = depot
        .obtain::<RealtimeBridgeState>()
        .map_err(|_| StatusError::internal_server_error().brief("realtime bridge state missing"))?
        .clone();
    let filter = parse_filter(req)?.normalized();

    WebSocketUpgrade::new()
        .max_message_size(WEBSOCKET_MAX_MESSAGE_SIZE)
        .max_frame_size(WEBSOCKET_MAX_FRAME_SIZE)
        .upgrade(req, res, move |socket| async move {
            handle_telemetry_socket(socket, state, filter).await;
        })
        .await
}

async fn handle_telemetry_socket(
    socket: WebSocket,
    state: RealtimeBridgeState,
    initial_filter: TelemetryFilter,
) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (client_tx, mut client_rx) = mpsc::channel::<Message>(state.websocket_client_buffer);
    let (filter_tx, mut filter_rx) = watch::channel(initial_filter.clone());
    let telemetry_state = state.clone();
    let writer_client_tx = client_tx.clone();

    let telemetry_task = tokio::spawn(async move {
        let mut rx = telemetry_state.subscribe_telemetry();

        if send_json_message(
            &writer_client_tx,
            BridgeServerMessage::Ready {
                transport: "websocket",
                filter: TelemetryFilterSnapshot::from(&initial_filter),
            },
        )
        .await
        .is_err()
        {
            return;
        }

        loop {
            tokio::select! {
                changed = filter_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }

                    let filter = filter_rx.borrow().clone();
                    if send_json_message(
                        &writer_client_tx,
                        BridgeServerMessage::FilterUpdated {
                            filter: TelemetryFilterSnapshot::from(&filter),
                        },
                    )
                    .await
                    .is_err()
                    {
                        break;
                    }
                }
                event = rx.recv() => {
                    match event {
                        Ok(event) => {
                            let filter = filter_rx.borrow().clone();
                            if !filter.matches(&event) {
                                continue;
                            }

                            if send_json_message(
                                &writer_client_tx,
                                BridgeServerMessage::Telemetry {
                                    data: TelemetryPayload::from_event(event),
                                },
                            )
                            .await
                            .is_err()
                            {
                                break;
                            }
                        }
                        Err(RecvError::Lagged(skipped)) => {
                            if send_json_message(&writer_client_tx, BridgeServerMessage::Lagged { skipped })
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
            }
        }
    });

    let mut heartbeat = interval(state.websocket_ping_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let ping  = Message::ping(Vec::new());
                if timeout(state.websocket_send_timeout, ws_tx.send(ping)).await.is_err() {
                    break;
                }
            }
            outbound = client_rx.recv() => {
                let Some(outbound) = outbound else {
                    break;
                };

                match timeout(state.websocket_send_timeout, ws_tx.send(outbound)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) | Err(_) => break,
                }
            }
            inbound = ws_rx.next() => {
                match inbound {
                    Some(Ok(message)) => {
                        if handle_client_message(message, &filter_tx, &client_tx).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(_)) | None => break,
                }
            }
        }
    }

    telemetry_task.abort();
}

async fn handle_client_message(
    message: Message,
    filter_tx: &watch::Sender<TelemetryFilter>,
    client_tx: &mpsc::Sender<Message>,
) -> Result<(), ()> {
    if message.is_close() {
        return Err(());
    }

    if message.is_ping() || message.is_pong() {
        return Ok(());
    }

    if !message.is_text() {
        return send_json_message(
            client_tx,
            BridgeServerMessage::Error {
                message: "only JSON text messages are supported".to_owned(),
            },
        )
        .await;
    }

    let text = match message.as_str() {
        Ok(text) => text,
        Err(_) => {
            return send_json_message(
                client_tx,
                BridgeServerMessage::Error {
                    message: "invalid UTF-8 websocket message".to_owned(),
                },
            )
            .await;
        }
    };

    match serde_json::from_str::<BridgeClientMessage>(text) {
        Ok(BridgeClientMessage::Subscribe {
            device,
            topic,
            topic_prefix,
            event_type,
        }) => {
            let filter = TelemetryFilter {
                device,
                topic,
                topic_prefix,
                event_type,
            }
            .normalized();

            filter_tx.send(filter).map_err(|_| ())
        }
        Ok(BridgeClientMessage::Ping) => {
            let filter_snapshot = {
                let filter_guard = filter_tx.borrow();
                TelemetryFilterSnapshot::from(&*filter_guard)
            };

            send_json_message(
                client_tx,
                BridgeServerMessage::Ready {
                    transport: "websocket",
                    filter: filter_snapshot,
                },
            )
            .await
        }
        Err(err) => {
            send_json_message(
                client_tx,
                BridgeServerMessage::Error {
                    message: format!("invalid client message: {err}"),
                },
            )
            .await
        }
    }
}

async fn send_json_message<T>(tx: &mpsc::Sender<Message>, value: T) -> Result<(), ()>
where
    T: Serialize,
{
    let text = serde_json::to_string(&value).map_err(|_| ())?;

    match tx.try_send(Message::text(text)) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => {
            let _ = tx.try_send(Message::close_with(1011u16, "client send queue full"));
            Err(())
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(()),
    }
}

fn parse_filter(req: &mut Request) -> Result<TelemetryFilter, StatusError> {
    let filter = TelemetryFilter {
        device: req.query::<String>("device"),
        topic: req.query::<String>("topic"),
        topic_prefix: req.query::<String>("topic_prefix"),
        event_type: req.query::<String>("event_type"),
    };

    validate_filter(&filter)?;
    Ok(filter)
}

fn topic_matches_prefix(topic: &str, prefix: &str) -> bool {
    topic == prefix
        || topic
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn validate_filter(filter: &TelemetryFilter) -> Result<(), StatusError> {
    if let Some(topic) = filter.topic.as_deref() {
        validate_topic_like("topic", topic)?;
    }

    if let Some(topic_prefix) = filter.topic_prefix.as_deref() {
        validate_topic_like("topic_prefix", topic_prefix)?;
    }

    if let Some(device) = filter.device.as_deref() {
        validate_simple_filter_value("device", device)?;
    }

    if let Some(event_type) = filter.event_type.as_deref() {
        validate_simple_filter_value("event_type", event_type)?;
    }

    Ok(())
}

fn validate_topic_like(name: &'static str, value: &str) -> Result<(), StatusError> {
    if value.trim().is_empty() || value.contains('+') || value.contains('#') {
        return Err(StatusError::bad_request().brief(format!(
            "{name} must not be empty and must not contain MQTT wildcards"
        )));
    }

    Ok(())
}

fn validate_simple_filter_value(name: &'static str, value: &str) -> Result<(), StatusError> {
    if value.trim().is_empty() || value.contains('/') || value.contains('+') || value.contains('#')
    {
        return Err(StatusError::bad_request()
            .brief(format!("{name} must be a non-empty single topic segment")));
    }

    Ok(())
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_owned();
        if value.is_empty() { None } else { Some(value) }
    })
}

fn sse_json_event<T>(name: &'static str, value: T) -> SseEvent
where
    T: Serialize,
{
    SseEvent::default()
        .name(name)
        .json(&value)
        .unwrap_or_else(|err| {
            SseEvent::default()
                .name("bridge_error")
                .text(format!("failed to serialize event: {err}"))
        })
}

fn telemetry_device(topic: &str) -> Option<&str> {
    let mut levels = topic.split('/');
    let namespace = levels.next()?;

    if namespace == "devices" || namespace == "device" {
        levels.next().filter(|device| !device.is_empty())
    } else {
        None
    }
}

fn telemetry_event_type(topic: &str) -> Option<&str> {
    let levels: Vec<&str> = topic.split('/').collect();

    match levels.as_slice() {
        ["devices" | "device", _, "telemetry", ..] => Some("telemetry"),
        ["devices" | "device", _, "status", ..] => Some("status"),
        ["devices" | "device", _, "event", event_type, ..] if !event_type.is_empty() => {
            Some(event_type)
        }
        _ => None,
    }
}

fn system_time_ms(value: SystemTime) -> u128 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

pub async fn graceful_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};

        if let Ok(mut signal) = signal(SignalKind::terminate()) {
            signal.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

pub fn default_bind_addr() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 8080))
}
