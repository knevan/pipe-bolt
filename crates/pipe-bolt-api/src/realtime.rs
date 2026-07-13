use std::convert::Infallible;
use std::time::Duration;

use async_stream::stream;
use futures_util::{SinkExt, StreamExt};
use pipe_bolt_domain::{NormalizedEvent, ProjectId};
use salvo::prelude::*;
use salvo::sse::{SseEvent, SseKeepAlive};
use salvo::websocket::{Message, WebSocket, WebSocketUpgrade};
use serde::Serialize;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{MissedTickBehavior, interval, timeout};

use crate::auth::{AUTH_CONTEXT_KEY, AuthContext, ManagementPermission};
use crate::dto::{
    RealtimeClientMessage, RealtimeEventResponse, RealtimeFilterSnapshot, RealtimeServerMessage,
};
use crate::error::{ApiError, write_error_response};
use crate::state::ApiState;

const SSE_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);
const WEBSOCKET_CLIENT_BUFFER: usize = 64;
const WEBSOCKET_MAX_MESSAGE_SIZE: usize = 64 * 1024;
const WEBSOCKET_MAX_FRAME_SIZE: usize = 64 * 1024;
const WEBSOCKET_PING_INTERVAL: Duration = Duration::from_secs(30);
const WEBSOCKET_SEND_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Default)]
struct RealtimeFilter {
    device_id: Option<String>,
    topic: Option<String>,
    topic_prefix: Option<String>,
    event_type: Option<String>,
    route_id: Option<String>,
}

impl RealtimeFilter {
    fn normalized(self) -> Self {
        Self {
            device_id: normalize_optional(self.device_id),
            topic: normalize_optional(self.topic),
            topic_prefix: normalize_optional(self.topic_prefix),
            event_type: normalize_optional(self.event_type),
            route_id: normalize_optional(self.route_id),
        }
    }

    fn matches(&self, event: &NormalizedEvent) -> bool {
        if let Some(expected) = self.device_id.as_deref()
            && event.device_id.as_deref() != Some(expected)
        {
            return false;
        }

        if let Some(expected) = self.topic.as_deref()
            && event.topic.as_str() != expected
        {
            return false;
        }

        if let Some(expected) = self.topic_prefix.as_deref()
            && !topic_matches_prefix(event.topic.as_str(), expected)
        {
            return false;
        }

        if let Some(expected) = self.event_type.as_deref()
            && event.event_type != expected
        {
            return false;
        }

        if let Some(expected) = self.route_id.as_deref()
            && event.route_id.as_str() != expected
        {
            return false;
        }

        true
    }
}

impl From<&RealtimeFilter> for RealtimeFilterSnapshot {
    fn from(value: &RealtimeFilter) -> Self {
        Self {
            device_id: value.device_id.clone(),
            topic: value.topic.clone(),
            topic_prefix: value.topic_prefix.clone(),
            event_type: value.event_type.clone(),
            route_id: value.route_id.clone(),
        }
    }
}

struct RealtimeSubscription {
    project_id: ProjectId,
    filter: RealtimeFilter,
    rx: broadcast::Receiver<NormalizedEvent>,
}

#[cfg_attr(feature = "salvo-oapi", endpoint(
    tags("realtime"),
    operation_id = "project_realtime_sse",
    security(("bearer_auth" = [])),
    responses(
        (status_code = 200, description = "Server-Sent Events stream of normalized events"),
        (status_code = 401, description = "Unauthorized"),
        (status_code = 403, description = "Forbidden"),
        (status_code = 503, description = "Runtime unavailable")
    )
))]
#[cfg_attr(not(feature = "salvo-oapi"), handler)]
pub async fn realtime_sse(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let subscription = match prepare_subscription(req, depot).await {
        Ok(subscription) => subscription,
        Err(error) => {
            write_error_response(res, &error);
            return;
        }
    };

    let project_id = subscription.project_id.to_string();
    let filter = subscription.filter;
    let mut rx = subscription.rx;
    let initial_filter = filter.clone();
    let event_stream = stream! {
        yield Ok::<_, Infallible>(sse_json_event(
            "ready",
            RealtimeServerMessage::Ready {
                transport: "sse".to_owned(),
                filter: RealtimeFilterSnapshot::from(&initial_filter),
            },
        ));

        loop {
            match rx.recv().await {
                Ok(event) => {
                    if !filter.matches(&event) {
                        continue;
                    }

                    yield Ok(sse_json_event(
                        "event",
                        RealtimeServerMessage::Event {
                            data: Box::new(RealtimeEventResponse::from_event(event)),
                        },
                    ));
                }
                Err(RecvError::Lagged(skipped)) => {
                    yield Ok(sse_json_event(
                        "lagged",
                        RealtimeServerMessage::Lagged { skipped },
                    ));
                }
                Err(RecvError::Closed) => break,
            }
        }
    };

    tracing::debug!(project_id = %project_id, transport = "sse", "realtime stream opened");
    SseKeepAlive::new(event_stream)
        .max_interval(SSE_KEEP_ALIVE_INTERVAL)
        .comment("ping")
        .stream(res);
}

#[cfg_attr(feature = "salvo-oapi", endpoint(
    tags("realtime"),
    operation_id = "project_realtime_ws",
    security(("bearer_auth" = [])),
    responses(
        (status_code = 101, description = "WebSocket upgrade accepted"),
        (status_code = 400, description = "Invalid WebSocket upgrade or filter"),
        (status_code = 401, description = "Unauthorized"),
        (status_code = 403, description = "Forbidden"),
        (status_code = 503, description = "Runtime unavailable")
    )
))]
#[cfg_attr(not(feature = "salvo-oapi"), handler)]
pub async fn realtime_ws(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let subscription = match prepare_subscription(req, depot).await {
        Ok(subscription) => subscription,
        Err(error) => {
            write_error_response(res, &error);
            return;
        }
    };

    let project_id = subscription.project_id.to_string();
    let upgrade = WebSocketUpgrade::new()
        .max_message_size(WEBSOCKET_MAX_MESSAGE_SIZE)
        .max_frame_size(WEBSOCKET_MAX_FRAME_SIZE)
        .upgrade(req, res, move |socket| async move {
            handle_socket(socket, project_id, subscription.filter, subscription.rx).await;
        })
        .await;

    if let Err(error) = upgrade {
        tracing::debug!(error = %error, "websocket upgrade rejected");
    }
}

async fn prepare_subscription(
    req: &mut Request,
    depot: &Depot,
) -> Result<RealtimeSubscription, ApiError> {
    let state = depot
        .obtain::<ApiState>()
        .cloned()
        .map_err(|_| ApiError::Internal {
            message: "API state missing".to_owned(),
        })?;
    let project_id = path_project_id(req)?;
    authorize_project(depot, &project_id, ManagementPermission::ProjectRead)?;
    let filter = parse_filter(req)?.normalized();
    let rx = state
        .runtime()
        .subscribe_realtime_events(&project_id)
        .await?;

    Ok(RealtimeSubscription {
        project_id,
        filter,
        rx,
    })
}

async fn handle_socket(
    socket: WebSocket,
    project_id: String,
    initial_filter: RealtimeFilter,
    mut rx: broadcast::Receiver<NormalizedEvent>,
) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (client_tx, mut client_rx) = mpsc::channel::<Message>(WEBSOCKET_CLIENT_BUFFER);
    let (filter_tx, mut filter_rx) = watch::channel(initial_filter.clone());
    let writer_client_tx = client_tx.clone();

    let telemetry_task = tokio::spawn(async move {
        if send_json_message(
            &writer_client_tx,
            RealtimeServerMessage::Ready {
                transport: "websocket".to_owned(),
                filter: RealtimeFilterSnapshot::from(&initial_filter),
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
                        RealtimeServerMessage::FilterUpdated {
                            filter: RealtimeFilterSnapshot::from(&filter),
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
                                RealtimeServerMessage::Event {
                                    data: Box::new(RealtimeEventResponse::from_event(event)),
                                },
                            )
                            .await
                            .is_err()
                            {
                                break;
                            }
                        }
                        Err(RecvError::Lagged(skipped)) => {
                            if send_json_message(&writer_client_tx, RealtimeServerMessage::Lagged { skipped })
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

    let mut heartbeat = interval(WEBSOCKET_PING_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tracing::debug!(project_id = %project_id, transport = "websocket", "realtime stream opened");

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if timeout(WEBSOCKET_SEND_TIMEOUT, ws_tx.send(Message::ping(Vec::new()))).await.is_err() {
                    break;
                }
            }
            outbound = client_rx.recv() => {
                let Some(outbound) = outbound else {
                    break;
                };

                match timeout(WEBSOCKET_SEND_TIMEOUT, ws_tx.send(outbound)).await {
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
    tracing::debug!(project_id = %project_id, transport = "websocket", "realtime stream closed");
}

async fn handle_client_message(
    message: Message,
    filter_tx: &watch::Sender<RealtimeFilter>,
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
            RealtimeServerMessage::Error {
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
                RealtimeServerMessage::Error {
                    message: "invalid UTF-8 websocket message".to_owned(),
                },
            )
            .await;
        }
    };

    match serde_json::from_str::<RealtimeClientMessage>(text) {
        Ok(RealtimeClientMessage::Subscribe {
            device_id,
            topic,
            topic_prefix,
            event_type,
            route_id,
        }) => {
            let filter = RealtimeFilter {
                device_id,
                topic,
                topic_prefix,
                event_type,
                route_id,
            }
            .normalized();
            validate_filter(&filter).map_err(|_| ())?;
            filter_tx.send(filter).map_err(|_| ())
        }
        Ok(RealtimeClientMessage::Ping) => {
            let filter_snapshot = {
                let filter_guard = filter_tx.borrow();
                RealtimeFilterSnapshot::from(&*filter_guard)
            };

            send_json_message(
                client_tx,
                RealtimeServerMessage::Ready {
                    transport: "websocket".to_owned(),
                    filter: filter_snapshot,
                },
            )
            .await
        }
        Err(error) => {
            send_json_message(
                client_tx,
                RealtimeServerMessage::Error {
                    message: format!("invalid client message: {error}"),
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

fn sse_json_event<T>(name: &'static str, value: T) -> SseEvent
where
    T: Serialize,
{
    match SseEvent::default().name(name).json(&value) {
        Ok(event) => event,
        Err(error) => SseEvent::default()
            .name("error")
            .text(format!("failed to serialize realtime event: {error}")),
    }
}

fn parse_filter(req: &Request) -> Result<RealtimeFilter, ApiError> {
    let filter = RealtimeFilter {
        device_id: req.query::<String>("device_id"),
        topic: req.query::<String>("topic"),
        topic_prefix: req.query::<String>("topic_prefix"),
        event_type: req.query::<String>("event_type"),
        route_id: req.query::<String>("route_id"),
    }
    .normalized();

    validate_filter(&filter)?;
    Ok(filter)
}

fn validate_filter(filter: &RealtimeFilter) -> Result<(), ApiError> {
    if let Some(topic) = filter.topic.as_deref() {
        validate_topic_like("topic", topic)?;
    }

    if let Some(topic_prefix) = filter.topic_prefix.as_deref() {
        validate_topic_like("topic_prefix", topic_prefix)?;
    }

    if let Some(device_id) = filter.device_id.as_deref() {
        validate_simple_filter_value("device_id", device_id)?;
    }

    if let Some(event_type) = filter.event_type.as_deref() {
        validate_simple_filter_value("event_type", event_type)?;
    }

    if let Some(route_id) = filter.route_id.as_deref() {
        validate_simple_filter_value("route_id", route_id)?;
    }

    Ok(())
}

fn validate_topic_like(name: &'static str, value: &str) -> Result<(), ApiError> {
    if value.trim().is_empty() || value.contains('+') || value.contains('#') {
        return Err(ApiError::BadRequest {
            message: format!("{name} must not be empty and must not contain MQTT wildcards"),
        });
    }

    Ok(())
}

fn validate_simple_filter_value(name: &'static str, value: &str) -> Result<(), ApiError> {
    if value.trim().is_empty() || value.contains('/') || value.contains('+') || value.contains('#')
    {
        return Err(ApiError::BadRequest {
            message: format!("{name} must be a non-empty single topic segment"),
        });
    }

    Ok(())
}

fn path_project_id(req: &Request) -> Result<ProjectId, ApiError> {
    let project_id = req
        .param::<String>("project_id")
        .ok_or_else(|| ApiError::BadRequest {
            message: "missing path parameter 'project_id'".to_owned(),
        })?;
    ProjectId::new(project_id).map_err(ApiError::from)
}

fn authorize_project(
    depot: &Depot,
    project_id: &ProjectId,
    permission: ManagementPermission,
) -> Result<(), ApiError> {
    let auth = depot
        .get::<AuthContext>(AUTH_CONTEXT_KEY)
        .map_err(|_| ApiError::Unauthorized)?;
    auth.authorize_project(project_id, permission)
}

fn topic_matches_prefix(topic: &str, prefix: &str) -> bool {
    topic == prefix
        || topic
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_owned();
        if value.is_empty() { None } else { Some(value) }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_prefix_should_match_topic_segments_only() {
        assert!(topic_matches_prefix("devices/a/telemetry", "devices/a"));
    }

    #[test]
    fn topic_prefix_should_reject_partial_segment_match() {
        assert!(!topic_matches_prefix("devices/abc/telemetry", "devices/a"));
    }
}
