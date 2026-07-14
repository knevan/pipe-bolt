use std::net::SocketAddr;

use salvo::logging::Logger;
use salvo::prelude::*;
use tokio::sync::watch;

use crate::handler::{
    get_audit_events, get_delivery_outcomes, get_failures, get_health, get_healthz,
    get_project_config, get_readyz, get_runtime_status, post_execute_command, post_runtime_reload,
    put_project_config, require_management_auth, resolve_failure,
};
#[cfg(feature = "salvo-oapi")]
use crate::openapi::attach_openapi;
use crate::realtime::{realtime_sse, realtime_ws};
use crate::state::ApiState;

pub async fn serve_management_api(
    bind_addr: SocketAddr,
    state: ApiState,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let router = management_router(state);
    let service = Service::new(router).hoop(Logger::new().log_status_error(false));
    let acceptor = TcpListener::new(bind_addr).bind().await;
    let server = Server::new(acceptor);

    tracing::info!(%bind_addr, "management API started");

    tokio::select! {
        _ = server.serve(service) => {}
        _ = shutdown_rx.changed() => {}
    }

    tracing::info!(%bind_addr, "management API stopped");
}

pub fn management_router(state: ApiState) -> Router {
    let router = Router::new()
        .hoop(affix_state::inject(state))
        .push(Router::with_path("health").get(get_health))
        .push(Router::with_path("healthz").get(get_healthz))
        .push(Router::with_path("readyz").get(get_readyz))
        .push(
            Router::with_path("projects/{project_id}")
                .hoop(require_management_auth)
                .push(
                    Router::with_path("config")
                        .get(get_project_config)
                        .put(put_project_config),
                )
                .push(Router::with_path("audit-events").get(get_audit_events))
                .push(
                    Router::with_path("commands/{command_template_id}/execute")
                        .post(post_execute_command),
                )
                .push(Router::with_path("failures").get(get_failures))
                .push(Router::with_path("failures/{failure_id}/resolve").post(resolve_failure))
                .push(Router::with_path("delivery-outcomes").get(get_delivery_outcomes))
                .push(
                    Router::with_path("runtime")
                        .push(Router::with_path("status").get(get_runtime_status))
                        .push(Router::with_path("reload").post(post_runtime_reload)),
                )
                .push(
                    Router::with_path("realtime")
                        .push(Router::with_path("sse").get(realtime_sse))
                        .push(Router::with_path("ws").get(realtime_ws)),
                ),
        );

    #[cfg(feature = "salvo-oapi")]
    {
        attach_openapi(router)
    }
    #[cfg(not(feature = "salvo-oapi"))]
    {
        router
    }
}
