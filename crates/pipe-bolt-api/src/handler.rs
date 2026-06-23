use pipe_bolt_domain::{
    BrokerId, ProjectConfig, ProjectId, SecretString, SinkId, SinkKind, UserId,
};
use pipe_bolt_storage::model::{AuditContext, FailureListQuery, OperationalListQuery};
use salvo::prelude::*;
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::dto::{
    HealthResponse, ListResponse, ProjectConfigResponse, ProjectConfigWriteResponse,
    ResolveFailureRequest, ResolveFailureResponse, RuntimeReloadRequest,
    UpdateProjectConfigRequest,
};
use crate::error::ApiError;
use crate::state::ApiState;

const DEFAULT_LIST_LIMIT: u32 = 100;
const MAX_LIST_LIMIT: u32 = 500;
const REDACTED_SECRET: &str = "<redacted>";
const ACTOR_HEADER: &str = "x-pipe-bolt-actor-id";

#[handler]
pub async fn get_health(res: &mut Response) {
    render_json(
        res,
        StatusCode::OK,
        &HealthResponse {
            status: "ok",
            service: "pipe-bolt-management-api",
        },
    );
}

#[handler]
pub async fn get_project_config(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match get_project_config_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

#[handler]
pub async fn put_project_config(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match put_project_config_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

#[handler]
pub async fn get_audit_events(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match get_audit_events_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

#[handler]
pub async fn get_failures(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match get_failures_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

#[handler]
pub async fn resolve_failure(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match resolve_failure_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

#[handler]
pub async fn get_delivery_outcomes(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match get_delivery_outcomes_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

#[handler]
pub async fn get_runtime_status(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match get_runtime_status_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

#[handler]
pub async fn post_runtime_reload(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    match post_runtime_reload_inner(req, depot).await {
        Ok(response) => render_json(res, StatusCode::OK, &response),
        Err(error) => render_error(res, error),
    }
}

async fn get_project_config_inner(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<ProjectConfigResponse, ApiError> {
    let state = authorized_state(req, depot)?;
    let project_id = path_project_id(req)?;
    let config = state
        .storage()
        .load_project_config(&project_id)
        .await?
        .ok_or_else(|| ApiError::NotFound {
            message: format!("project config '{project_id}' was not found"),
        })?;

    Ok(ProjectConfigResponse { config })
}

async fn put_project_config_inner(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<ProjectConfigWriteResponse, ApiError> {
    let state = authorized_state(req, depot)?;
    let path_project_id = path_project_id(req)?;
    let body = req
        .parse_json_with_max_size::<UpdateProjectConfigRequest>(state.max_config_body_bytes())
        .await
        .map_err(|error| ApiError::BadRequest {
            message: format!("invalid config update request: {error}"),
        })?;

    if body.config.id != path_project_id {
        return Err(ApiError::BadRequest {
            message: format!(
                "path project_id '{path_project_id}' does not match body project_id '{}'",
                body.config.id
            ),
        });
    }

    let existing = state
        .storage()
        .load_project_config(&path_project_id)
        .await?
        .ok_or_else(|| ApiError::NotFound {
            message: format!("project config '{path_project_id}' was not found"),
        })?;
    let mut next_config = body.config;

    merge_redacted_secrets(&existing, &mut next_config)?;
    next_config.validate()?;

    let write = state
        .storage()
        .update_project_config(
            &next_config,
            body.expected_version,
            audit_context(req, body.reason)?,
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
    let state = authorized_state(req, depot)?;
    let project_id = path_project_id(req)?;
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
    let state = authorized_state(req, depot)?;
    let project_id = path_project_id(req)?;
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
    let state = authorized_state(req, depot)?;
    let project_id = path_project_id(req)?;
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
            audit_context(req, body.reason)?,
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
    let state = authorized_state(req, depot)?;
    let project_id = path_project_id(req)?;
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
    let state = authorized_state(req, depot)?;
    let project_id = path_project_id(req)?;
    Ok(state.runtime().status(&project_id).await?)
}

async fn post_runtime_reload_inner(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<crate::dto::RuntimeReloadResponse, ApiError> {
    let state = authorized_state(req, depot)?;
    let project_id = path_project_id(req)?;
    let body = req
        .parse_json_with_max_size::<RuntimeReloadRequest>(8 * 1024)
        .await
        .map_err(|error| ApiError::BadRequest {
            message: format!("invalid runtime reload request: {error}"),
        })?;

    Ok(state.runtime().reload(&project_id, body.reason).await?)
}

fn authorized_state(req: &Request, depot: &Depot) -> Result<ApiState, ApiError> {
    let state = depot
        .obtain::<ApiState>()
        .map_err(|_| ApiError::Internal {
            message: "API state missing".to_owned(),
        })?
        .clone();

    state.authorize(req.header::<String>("authorization").as_deref())?;
    Ok(state)
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

fn audit_context(req: &Request, reason: Option<String>) -> Result<AuditContext, ApiError> {
    let actor_id = req
        .header::<String>(ACTOR_HEADER)
        .map(UserId::new)
        .transpose()?;

    Ok(AuditContext { actor_id, reason })
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
    if matches!(error, ApiError::Internal { .. }) {
        tracing::error!(error = %error, "management API request failed");
    }

    res.status_code(error.status_code());
    res.render(Json(error.response_body()));
}
