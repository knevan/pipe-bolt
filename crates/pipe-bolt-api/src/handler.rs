use pipe_bolt_domain::{
    BrokerId, CommandTemplateId, ProjectConfig, ProjectId, SecretString, SinkId, SinkKind,
};
use pipe_bolt_storage::model::{AuditContext, FailureListQuery, OperationalListQuery};
use salvo::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::auth::{AUTH_CONTEXT_KEY, AuthContext, ManagementPermission};
use crate::dto::{
    ExecuteCommandRequest, HealthResponse, ListResponse, ProjectConfigResponse,
    ProjectConfigWriteResponse, ReadinessCheckResponse, ReadinessResponse, ResolveFailureRequest,
    ResolveFailureResponse, RuntimeReadinessResponse, RuntimeReloadRequest,
    UpdateProjectConfigRequest,
};
#[cfg(feature = "salvo-oapi")]
use crate::dto::{ExecuteCommandResponse, RuntimeReloadResponse, RuntimeStatusResponse};
use crate::error::{ApiError, write_error_response};
use crate::state::ApiState;

const DEFAULT_LIST_LIMIT: u32 = 100;
const MAX_LIST_LIMIT: u32 = 500;
const REDACTED_SECRET: &str = "<redacted>";
const RUNTIME_RELOAD_MAX_BODY_BYTES: usize = 8 * 1024;
const COMMAND_EXECUTE_MAX_BODY_BYTES: usize = 64 * 1024;

const MANAGEMENT_SERVICE_NAME: &str = "pipe-bolt-management-api";

#[cfg_attr(feature = "salvo-oapi", endpoint(
    tags("health"),
    operation_id = "get_health",
    responses((status_code = 200, description = "Management API liveness", body = HealthResponse))
))]
#[cfg_attr(not(feature = "salvo-oapi"), handler)]
pub async fn get_health(res: &mut Response) {
    render_json(
        res,
        StatusCode::OK,
        &HealthResponse {
            status: "ok".to_owned(),
            service: MANAGEMENT_SERVICE_NAME.to_owned(),
        },
    );
}

#[cfg_attr(feature = "salvo-oapi", endpoint(
    tags("health"),
    operation_id = "get_readyz",
    responses(
        (status_code = 200, description = "Management API is ready", body = ReadinessResponse),
        (status_code = 503, description = "Management API is not ready", body = ReadinessResponse)
    )
))]
#[cfg_attr(not(feature = "salvo-oapi"), handler)]
pub async fn get_readyz(depot: &mut Depot, res: &mut Response) {
    let response = match readiness_inner(depot).await {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(error = %error, "readiness check failed");
            ReadinessResponse::from_checks(
                MANAGEMENT_SERVICE_NAME,
                ReadinessCheckResponse::not_ready("storage check failed"),
                RuntimeReadinessResponse {
                    status: crate::dto::ReadinessStatus::NotReady,
                    project_id: "unknown".to_owned(),
                    lifecycle: crate::dto::RuntimeLifecycleState::Stopped,
                    active_version: None,
                    message: Some("runtime readiness unavailable".to_owned()),
                },
            )
        }
    };
    let status = response.status.http_status();

    render_json(res, status, &response);
}

#[handler]
pub async fn require_management_auth(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) {
    let state = match depot.obtain::<ApiState>() {
        Ok(state) => state.clone(),
        Err(_) => {
            let error = ApiError::Internal {
                message: "API state missing".to_owned(),
            };
            write_error_response(res, &error);
            ctrl.skip_rest();
            return;
        }
    };

    match state.authenticate(req.header::<String>("authorization").as_deref()) {
        Ok(auth_context) => {
            depot.insert(AUTH_CONTEXT_KEY, auth_context);
        }
        Err(error) => {
            write_error_response(res, &error);
            ctrl.skip_rest();
        }
    }
}

#[cfg_attr(feature = "salvo-oapi", endpoint(
    tags("config"),
    operation_id = "get_project_config",
    security(("bearer_auth" = [])),
    responses(
        (status_code = 200, description = "Active project configuration", body = ProjectConfigResponse),
        (status_code = 401, description = "Unauthorized"),
        (status_code = 403, description = "Forbidden"),
        (status_code = 404, description = "Project config not found")
    )
))]
#[cfg_attr(not(feature = "salvo-oapi"), handler)]
pub async fn get_project_config(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match get_project_config_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

#[cfg_attr(feature = "salvo-oapi", endpoint(
    tags("config"),
    operation_id = "put_project_config",
    security(("bearer_auth" = [])),
    responses(
        (status_code = 200, description = "Configuration successfully updated", body = ProjectConfigWriteResponse),
        (status_code = 400, description = "Invalid request or version mismatch"),
        (status_code = 401, description = "Unauthorized"),
        (status_code = 403, description = "Forbidden")
    )
))]
#[cfg_attr(not(feature = "salvo-oapi"), handler)]
pub async fn put_project_config(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match put_project_config_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

#[cfg_attr(feature = "salvo-oapi", endpoint(
    tags("audit"),
    operation_id = "get_audit_events",
    security(("bearer_auth" = [])),
    responses(
        (status_code = 200, description = "List of audit event records"),
        (status_code = 401, description = "Unauthorized")
    )
))]
#[cfg_attr(not(feature = "salvo-oapi"), handler)]
pub async fn get_audit_events(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match get_audit_events_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

#[cfg_attr(feature = "salvo-oapi", endpoint(
    tags("failure"),
    operation_id = "get_failures",
    security(("bearer_auth" = [])),
    responses(
        (status_code = 200, description = "List of ingestion/processing failures")
    )
))]
#[cfg_attr(not(feature = "salvo-oapi"), handler)]
pub async fn get_failures(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match get_failures_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

#[cfg_attr(feature = "salvo-oapi", endpoint(
    tags("failure"),
    operation_id = "resolve_failure",
    security(("bearer_auth" = [])),
    responses(
        (status_code = 200, description = "Failure resolved", body = ResolveFailureResponse),
        (status_code = 404, description = "Failure event not found"),
        (status_code = 409, description = "Failure already resolved")
    )
))]
#[cfg_attr(not(feature = "salvo-oapi"), handler)]
pub async fn resolve_failure(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match resolve_failure_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

#[cfg_attr(feature = "salvo-oapi", endpoint(
    tags("etl"),
    operation_id = "get_delivery_outcomes",
    security(("bearer_auth" = [])),
    responses(
        (status_code = 200, description = "List of sink delivery outcome records")
    )
))]
#[cfg_attr(not(feature = "salvo-oapi"), handler)]
pub async fn get_delivery_outcomes(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match get_delivery_outcomes_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

#[cfg_attr(feature = "salvo-oapi", endpoint(
    tags("runtime"),
    operation_id = "get_runtime_status",
    security(("bearer_auth" = [])),
    responses(
        (status_code = 200, description = "Active runtime metrics and connection statuses", body = RuntimeStatusResponse)
    )
))]
#[cfg_attr(not(feature = "salvo-oapi"), handler)]
pub async fn get_runtime_status(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match get_runtime_status_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

#[cfg_attr(feature = "salvo-oapi", endpoint(
    tags("command"),
    operation_id = "post_execute_command",
    security(("bearer_auth" = [])),
    responses(
        (status_code = 202, description = "Command execution queued", body = ExecuteCommandResponse),
        (status_code = 400, description = "Invalid execution parameters"),
        (status_code = 404, description = "Command template not found")
    )
))]
#[cfg_attr(not(feature = "salvo-oapi"), handler)]
pub async fn post_execute_command(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match post_execute_command_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::ACCEPTED, &response),
        Err(error) => render_error(res, error),
    }
}

#[cfg_attr(feature = "salvo-oapi", endpoint(
    tags("runtime"),
    operation_id = "post_runtime_reload",
    security(("bearer_auth" = [])),
    responses(
        (status_code = 200, description = "Runtime configuration successfully reloaded", body = RuntimeReloadResponse)
    )
))]
#[cfg_attr(not(feature = "salvo-oapi"), handler)]
pub async fn post_runtime_reload(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match post_runtime_reload_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

async fn readiness_inner(depot: &Depot) -> Result<ReadinessResponse, ApiError> {
    let state = authorized_state(depot)?;
    let storage = match state.storage().health_check().await {
        Ok(()) => ReadinessCheckResponse::ready(),
        Err(error) => {
            tracing::warn!(error = %error, "management API storage readiness check failed");
            ReadinessCheckResponse::not_ready("storage check failed")
        }
    };
    let runtime = state.runtime().readiness().await?;

    Ok(ReadinessResponse::from_checks(
        MANAGEMENT_SERVICE_NAME,
        storage,
        runtime,
    ))
}

async fn get_project_config_inner(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<ProjectConfigResponse, ApiError> {
    let state = authorized_state(depot)?;
    let project_id = path_project_id(req)?;
    authorize_project(depot, &project_id, ManagementPermission::ProjectRead)?;
    let config = state
        .storage()
        .load_project_config(&project_id)
        .await?
        .ok_or_else(|| ApiError::NotFound {
            message: format!("project config '{project_id}' was not found"),
        })?;

    Ok(ProjectConfigResponse::from_domain(config))
}

async fn put_project_config_inner(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<ProjectConfigWriteResponse, ApiError> {
    let state = authorized_state(depot)?;
    let path_project_id = path_project_id(req)?;
    let auth = authorize_project(
        depot,
        &path_project_id,
        ManagementPermission::ProjectConfigWrite,
    )?
    .clone();
    ensure_declared_body_size(req, state.max_config_body_bytes())?;
    let body = req
        .parse_json_with_max_size::<UpdateProjectConfigRequest>(state.max_config_body_bytes())
        .await
        .map_err(|error| ApiError::BadRequest {
            message: format!("invalid config update request: {error}"),
        })?;

    if body.config.project_id != path_project_id {
        return Err(ApiError::BadRequest {
            message: format!(
                "path project_id '{path_project_id}' does not match body project_id '{}'",
                body.config.project_id
            ),
        });
    }

    let next_version = next_config_version(body.expected_version)?;
    let existing = state
        .storage()
        .load_project_config(&path_project_id)
        .await?
        .ok_or_else(|| ApiError::NotFound {
            message: format!("project config '{path_project_id}' was not found"),
        })?;
    let mut next_config = body.config.into_domain(next_version);

    merge_redacted_secrets(&existing, &mut next_config)?;
    next_config.validate()?;
    state
        .runtime()
        .validate_candidate_config(&path_project_id, &next_config)
        .await?;

    let write = state
        .storage()
        .update_project_config(
            &next_config,
            body.expected_version,
            audit_context(&auth, body.reason),
        )
        .await?;

    Ok(ProjectConfigWriteResponse {
        project_id: write.project_id,
        version: write.version,
        revision_id: write.revision_id,
        config_hash: write.config_hash,
        reload_required: true,
    })
}

async fn get_audit_events_inner(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<ListResponse<pipe_bolt_storage::model::AuditEventRecord>, ApiError> {
    let state = authorized_state(depot)?;
    let project_id = path_project_id(req)?;
    authorize_project(depot, &project_id, ManagementPermission::ProjectRead)?;
    let query = list_query(req)?;
    let items = state
        .storage()
        .list_audit_events(&project_id, query)
        .await?;
    let next_before = items.last().map(|item| item.occurred_at);

    Ok(ListResponse { items, next_before })
}

async fn get_failures_inner(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<ListResponse<pipe_bolt_storage::model::FailureEventRecord>, ApiError> {
    let state = authorized_state(depot)?;
    let project_id = path_project_id(req)?;
    authorize_project(depot, &project_id, ManagementPermission::ProjectRead)?;
    let list = list_query(req)?;
    let unresolved_only = optional_bool_query(req, "unresolved_only")?.unwrap_or(false);
    let query = FailureListQuery {
        limit: list.limit,
        before: list.before,
        unresolved_only,
    };
    let items = state.storage().list_failures(&project_id, query).await?;
    let next_before = items.last().map(|item| item.occurred_at);

    Ok(ListResponse { items, next_before })
}

async fn resolve_failure_inner(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<ResolveFailureResponse, ApiError> {
    let state = authorized_state(depot)?;
    let project_id = path_project_id(req)?;
    let auth = authorize_project(depot, &project_id, ManagementPermission::FailureResolve)?.clone();
    let failure_id = path_param(req, "failure_id")?;
    let body = req
        .parse_json_with_max_size::<ResolveFailureRequest>(16 * 1024)
        .await
        .map_err(|error| ApiError::BadRequest {
            message: format!("invalid failure resolution request: {error}"),
        })?;

    state
        .storage()
        .resolve_failure(
            &project_id,
            &failure_id,
            &body.resolution,
            audit_context(&auth, body.reason),
        )
        .await?;

    Ok(ResolveFailureResponse {
        failure_id,
        resolved: true,
    })
}

async fn get_delivery_outcomes_inner(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<ListResponse<pipe_bolt_storage::model::SinkDeliveryOutcomeRecord>, ApiError> {
    let state = authorized_state(depot)?;
    let project_id = path_project_id(req)?;
    authorize_project(depot, &project_id, ManagementPermission::ProjectRead)?;
    let query = list_query(req)?;
    let items = state
        .storage()
        .list_delivery_outcomes(&project_id, query)
        .await?;
    let next_before = items.last().map(|item| item.occurred_at);

    Ok(ListResponse { items, next_before })
}

async fn get_runtime_status_inner(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<crate::dto::RuntimeStatusResponse, ApiError> {
    let state = authorized_state(depot)?;
    let project_id = path_project_id(req)?;
    authorize_project(depot, &project_id, ManagementPermission::ProjectRead)?;
    Ok(state.runtime().status(&project_id).await?)
}

async fn post_runtime_reload_inner(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<crate::dto::RuntimeReloadResponse, ApiError> {
    let state = authorized_state(depot)?;
    let project_id = path_project_id(req)?;
    let auth = authorize_project(depot, &project_id, ManagementPermission::RuntimeReload)?.clone();
    ensure_declared_body_size(req, RUNTIME_RELOAD_MAX_BODY_BYTES)?;
    let body = parse_optional_json_with_max_size::<RuntimeReloadRequest>(
        req,
        RUNTIME_RELOAD_MAX_BODY_BYTES,
    )
    .await?
    .unwrap_or_default();

    Ok(state
        .runtime()
        .reload(&project_id, audit_context(&auth, body.reason))
        .await?)
}

async fn post_execute_command_inner(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<crate::dto::ExecuteCommandResponse, ApiError> {
    let state = authorized_state(depot)?;
    let project_id = path_project_id(req)?;
    let command_template_id = CommandTemplateId::new(path_param(req, "command_template_id")?)?;
    let auth = authorize_project(depot, &project_id, ManagementPermission::CommandExecute)?.clone();
    ensure_declared_body_size(req, COMMAND_EXECUTE_MAX_BODY_BYTES)?;
    let body = parse_optional_json_with_max_size::<ExecuteCommandRequest>(
        req,
        COMMAND_EXECUTE_MAX_BODY_BYTES,
    )
    .await?
    .unwrap_or(ExecuteCommandRequest {
        params: Default::default(),
        reason: None,
    });

    Ok(state
        .runtime()
        .execute_command(
            &project_id,
            &command_template_id,
            body.params,
            audit_context(&auth, body.reason),
        )
        .await?)
}

fn authorized_state(depot: &Depot) -> Result<ApiState, ApiError> {
    depot
        .obtain::<ApiState>()
        .cloned()
        .map_err(|_| ApiError::Internal {
            message: "API state missing".to_owned(),
        })
}

fn path_project_id(req: &Request) -> Result<ProjectId, ApiError> {
    ProjectId::new(path_param(req, "project_id")?).map_err(ApiError::from)
}

fn path_param(req: &Request, name: &'static str) -> Result<String, ApiError> {
    req.param::<String>(name)
        .ok_or_else(|| ApiError::BadRequest {
            message: format!("missing path parameter '{name}'"),
        })
}

fn auth_context(depot: &Depot) -> Result<&AuthContext, ApiError> {
    depot
        .get::<AuthContext>(AUTH_CONTEXT_KEY)
        .map_err(|_| ApiError::Unauthorized)
}

fn authorize_project<'a>(
    depot: &'a Depot,
    project_id: &ProjectId,
    permission: ManagementPermission,
) -> Result<&'a AuthContext, ApiError> {
    let auth = auth_context(depot)?;
    auth.authorize_project(project_id, permission)?;
    Ok(auth)
}

fn audit_context(auth: &AuthContext, reason: Option<String>) -> AuditContext {
    AuditContext {
        actor_id: Some(auth.actor_id().clone()),
        reason,
    }
}

fn next_config_version(expected_version: u64) -> Result<u64, ApiError> {
    expected_version
        .checked_add(1)
        .ok_or_else(|| ApiError::UnprocessableEntity {
            message: "project config version overflow".to_owned(),
            details: Some(serde_json::json!({ "expected_version": expected_version })),
        })
}

fn ensure_declared_body_size(req: &Request, max_bytes: usize) -> Result<(), ApiError> {
    let Some(actual_bytes) = req.header::<usize>("content-length") else {
        return Ok(());
    };

    if actual_bytes > max_bytes {
        return Err(ApiError::PayloadTooLarge {
            actual_bytes,
            max_bytes,
        });
    }

    Ok(())
}

async fn parse_optional_json_with_max_size<T>(
    req: &mut Request,
    max_bytes: usize,
) -> Result<Option<T>, ApiError>
where
    T: DeserializeOwned,
{
    let payload = req.payload().await.map_err(|error| ApiError::BadRequest {
        message: format!("failed to read request body: {error}"),
    })?;

    if payload.is_empty() {
        return Ok(None);
    }

    if payload.len() > max_bytes {
        return Err(ApiError::PayloadTooLarge {
            actual_bytes: payload.len(),
            max_bytes,
        });
    }

    serde_json::from_slice::<T>(payload)
        .map(Some)
        .map_err(|err| ApiError::BadRequest {
            message: format!("invalid JSON body: {err}"),
        })
}

fn list_query(req: &Request) -> Result<OperationalListQuery, ApiError> {
    let limit = optional_u32_query(req, "limit")?
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    let before = optional_rfc3339_query(req, "before")?;

    Ok(OperationalListQuery { limit, before })
}

fn optional_u32_query(req: &Request, name: &'static str) -> Result<Option<u32>, ApiError> {
    let Some(raw) = req.query::<String>(name) else {
        return Ok(None);
    };

    raw.parse::<u32>()
        .map(Some)
        .map_err(|_| ApiError::BadRequest {
            message: format!("query parameter '{name}' must be an unsigned integer"),
        })
}

fn optional_bool_query(req: &Request, name: &'static str) -> Result<Option<bool>, ApiError> {
    let Some(raw) = req.query::<String>(name) else {
        return Ok(None);
    };

    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => Err(ApiError::BadRequest {
            message: format!("query parameter '{name}' must be a boolean"),
        }),
    }
}

fn optional_rfc3339_query(
    req: &Request,
    name: &'static str,
) -> Result<Option<OffsetDateTime>, ApiError> {
    let Some(raw) = req.query::<String>(name) else {
        return Ok(None);
    };

    OffsetDateTime::parse(&raw, &Rfc3339)
        .map(Some)
        .map_err(|error| ApiError::BadRequest {
            message: format!("query parameter '{name}' must be RFC3339 timestamp: {error}"),
        })
}

fn merge_redacted_secrets(
    existing: &ProjectConfig,
    proposed: &mut ProjectConfig,
) -> Result<(), ApiError> {
    for broker in &mut proposed.brokers {
        let Some(credentials) = &mut broker.credentials else {
            continue;
        };

        if credentials.password.expose_secret() == REDACTED_SECRET {
            credentials.password = existing_broker_password(existing, &broker.id)?;
        }
    }

    for sink in &mut proposed.sinks {
        let SinkKind::Webhook { headers, .. } = &mut sink.kind else {
            continue;
        };

        for header in headers {
            if header.value.expose_secret() == REDACTED_SECRET {
                header.value = existing_sink_header(existing, &sink.id, &header.name)?;
            }
        }
    }

    Ok(())
}

fn existing_broker_password(
    existing: &ProjectConfig,
    broker_id: &BrokerId,
) -> Result<SecretString, ApiError> {
    existing
        .brokers
        .iter()
        .find(|broker| &broker.id == broker_id)
        .and_then(|broker| broker.credentials.as_ref())
        .map(|credentials| credentials.password.clone())
        .ok_or_else(|| ApiError::BadRequest {
            message: format!(
                "broker '{broker_id}' submitted a redacted password but no existing secret exists"
            ),
        })
}

fn existing_sink_header(
    existing: &ProjectConfig,
    sink_id: &SinkId,
    header_name: &str,
) -> Result<SecretString, ApiError> {
    existing
        .sinks
        .iter()
        .find(|sink| &sink.id == sink_id)
        .and_then(|sink| match &sink.kind {
            SinkKind::Webhook { headers, .. } => headers
                .iter()
                .find(|header| header.name.eq_ignore_ascii_case(header_name)),
            SinkKind::Database { .. } => None,
        })
        .map(|header| header.value.clone())
        .ok_or_else(|| ApiError::BadRequest {
            message: format!(
                "sink '{sink_id}' header '{header_name}' submitted a redacted value but no existing secret exists"
            ),
        })
}

fn render_json<T>(res: &mut Response, status: StatusCode, body: &T)
where
    T: Serialize + Send + Sync,
{
    res.status_code(status);
    res.render(Json(body));
}

fn render_error(res: &mut Response, error: ApiError) {
    write_error_response(res, &error);
}
