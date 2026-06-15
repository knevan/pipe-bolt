use std::net::SocketAddr;

use salvo::prelude::*;
use tokio::sync::watch;

use crate::error::MqttEngineError;
use crate::mqtt::engine::MqttHandle;
use crate::web::realtime::command::command_http;
use crate::web::realtime::sse::telemetry_sse;
use crate::web::realtime::state::RealtimeBridgeState;
use crate::web::realtime::websocket::telemetry_ws;

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
            Router::with_path("realtime")
                .push(
                    Router::with_path("telemetry")
                        .push(Router::with_path("sse").get(telemetry_sse))
                        .push(Router::with_path("ws").get(telemetry_ws)),
                )
                .push(Router::with_path("commands").post(command_http)),
        )
}

#[handler]
async fn health(res: &mut Response) {
    res.render(Json(serde_json::json!({ "status": "ok" })));
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
