use std::convert::Infallible;

use async_stream::stream;
use salvo::prelude::*;
use serde::Serialize;
use tokio::sync::broadcast::error::RecvError;

use crate::web::realtime::dto::{BridgeServerMessage, TelemetryFilterSnapshot, TelemetryPayload};
use crate::web::realtime::filter::parse_filter;
use crate::web::realtime::state::RealtimeBridgeState;

#[handler]
pub async fn telemetry_sse(
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
        .max_interval(state.sse_keep_alive_interval())
        .comment("ping")
        .stream(res);

    Ok(())
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
