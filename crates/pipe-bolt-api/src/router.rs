use std::net::SocketAddr;

use salvo::prelude::*;
use tokio::sync::watch;

use crate::handler::{
    get_audit_events, get_delivery_outcomes, get_failures, get_health, get_project_config,
    get_runtime_status, post_runtime_reload, put_project_config, resolve_failure,
};
use crate::state::ApiState;

pub async fn serve_management_api(
    bind_addr: SocketAddr,
    state: ApiState,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let router = management_router(state);
    let acceptor = TcpListener::new(bind_addr).bind().await;
    let server = Server::new(acceptor);

    tracing::info!(%bind_addr, "management API started");

    tokio::select! {
        _ = server.serve(router) => {}
        _ = shutdown_rx.changed() => {}
    }

    tracing::info!(%bind_addr, "management API stopped");
}

pub fn management_router(state: ApiState) -> Router {
    Router::new()
        .hoop(affix_state::inject(state))
        .push(Router::with_path("health").get(get_health))
        .push(
            Router::with_path("projects/{project_id}")
                .push(
                    Router::with_path("config")
                        .get(get_project_config)
                        .put(put_project_config),
                )
                .push(Router::with_path("audit-events").get(get_audit_events))
                .push(Router::with_path("failures").get(get_failures))
                .push(Router::with_path("failures/{failure_id}/resolve").post(resolve_failure))
                .push(Router::with_path("delivery-outcomes").get(get_delivery_outcomes))
                .push(
                    Router::with_path("runtime")
                        .push(Router::with_path("status").get(get_runtime_status))
                        .push(Router::with_path("reload").post(post_runtime_reload)),
                ),
        )
}
