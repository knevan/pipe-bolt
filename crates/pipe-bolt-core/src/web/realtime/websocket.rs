use futures_util::{SinkExt, StreamExt};
use salvo::prelude::*;
use salvo::websocket::{Message, WebSocket};
use serde::Serialize;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{mpsc, watch};
use tokio::time::{MissedTickBehavior, interval, timeout};

use crate::web::realtime::dto::{
    BridgeClientMessage, BridgeServerMessage, TelemetryFilterSnapshot, TelemetryPayload,
};
use crate::web::realtime::filter::{TelemetryFilter, parse_filter};
use crate::web::realtime::state::RealtimeBridgeState;

const WEBSOCKET_MAX_MESSAGE_SIZE: usize = 63 * 1024;
const WEBSOCKET_MAX_FRAME_SIZE: usize = 63 * 1024;

#[handler]
pub async fn telemetry_ws(
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
    let (client_tx, mut client_rx) = mpsc::channel::<Message>(state.websocket_client_buffer());
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

    let mut heartbeat = interval(state.websocket_ping_interval());
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let ping  = Message::ping(Vec::new());
                if timeout(state.websocket_send_timeout(), ws_tx.send(ping)).await.is_err() {
                    break;
                }
            }
            outbound = client_rx.recv() => {
                let Some(outbound) = outbound else {
                    break;
                };

                match timeout(state.websocket_send_timeout(), ws_tx.send(outbound)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) | Err(_) => break,
                }
            }
            inbound = ws_rx.next() => {
                match inbound {
                    Some(Ok(message)) => {
                        if handle_client_message(message, &state, &filter_tx, &client_tx).await.is_err() {
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
    state: &RealtimeBridgeState,
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
        Ok(BridgeClientMessage::Command { request }) => match state.enqueue_command(request) {
            Ok(receipt) => {
                send_json_message(
                    client_tx,
                    BridgeServerMessage::CommandQueue { data: receipt },
                )
                .await
            }
            Err(err) => {
                send_json_message(
                    client_tx,
                    BridgeServerMessage::Error {
                        message: err.message(),
                    },
                )
                .await
            }
        },
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

/// Sends a serialized server message to a bounded WebSocket outbound queue.
///
/// Full queues are treated as slow clients and closed to protect memory under telemetry bursts.
async fn send_json_message<T>(tx: &mpsc::Sender<Message>, value: T) -> Result<(), ()>
where
    T: Serialize,
{
    let text = serde_json::to_string(&value).map_err(|_| ())?;

    match tx.try_send(Message::text(text)) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => {
            // Close slow WebSocket clients instead of letting unbounded outbound buffers grow under telemetry bursts.
            let _ = tx.try_send(Message::close_with(1011u16, "client send queue full"));
            Err(())
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(()),
    }
}
