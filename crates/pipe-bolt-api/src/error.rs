use pipe_bolt_storage::error::StorageError;
use salvo::http::header::WWW_AUTHENTICATE;
use salvo::http::{HeaderValue, StatusCode};
use salvo::prelude::*;
use serde_json::json;
use thiserror::Error;

use crate::dto::{ErrorPayload, ErrorResponse};
use crate::runtime_control::RuntimeControlError;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("unauthorized")]
    Unauthorized,

    #[error("forbidden: {message}")]
    Forbidden { message: String },

    #[error("bad request: {message}")]
    BadRequest { message: String },

    #[error("not found: {message}")]
    NotFound { message: String },

    #[error("conflict: {message}")]
    Conflict {
        message: String,
        details: Option<serde_json::Value>,
    },

    #[error("unprocessable entity: {message}")]
    UnprocessableEntity {
        message: String,
        details: Option<serde_json::Value>,
    },

    #[error("payload too large: {actual_bytes} bytes exceeds {max_bytes} bytes")]
    PayloadTooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },

    #[error("service unavailable: {message}")]
    ServiceUnavailable { message: String },

    #[error("internal server error: {message}")]
    Internal { message: String },
}

impl ApiError {
    pub const fn status_code(&self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden { .. } => StatusCode::FORBIDDEN,
            Self::BadRequest { .. } => StatusCode::BAD_REQUEST,
            Self::NotFound { .. } => StatusCode::NOT_FOUND,
            Self::Conflict { .. } => StatusCode::CONFLICT,
            Self::UnprocessableEntity { .. } => StatusCode::UNPROCESSABLE_ENTITY,
            Self::PayloadTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::ServiceUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn response_body(&self) -> ErrorResponse {
        ErrorResponse {
            error: ErrorPayload {
                code: self.code(),
                message: self.safe_message(),
                details: self.details(),
            },
        }
    }

    const fn code(&self) -> &'static str {
        match self {
            Self::Unauthorized => "unauthorized",
            Self::Forbidden { .. } => "forbidden",
            Self::BadRequest { .. } => "bad_request",
            Self::NotFound { .. } => "not_found",
            Self::Conflict { .. } => "conflict",
            Self::UnprocessableEntity { .. } => "unprocessable_entity",
            Self::PayloadTooLarge { .. } => "payload_too_large",
            Self::ServiceUnavailable { .. } => "service_unavailable",
            Self::Internal { .. } => "internal_server_error",
        }
    }

    fn safe_message(&self) -> String {
        match self {
            Self::Unauthorized => "valid bearer token is required".to_owned(),
            Self::Forbidden { message }
            | Self::BadRequest { message }
            | Self::NotFound { message }
            | Self::Conflict { message, .. }
            | Self::UnprocessableEntity { message, .. }
            | Self::ServiceUnavailable { message } => message.clone(),
            Self::PayloadTooLarge {
                actual_bytes,
                max_bytes,
            } => {
                format!("request body is too large: {actual_bytes} bytes exceeds {max_bytes} bytes")
            }
            Self::Internal { .. } => "internal server error".to_owned(),
        }
    }

    fn details(&self) -> Option<serde_json::Value> {
        match self {
            Self::Conflict { details, .. } | Self::UnprocessableEntity { details, .. } => {
                details.clone()
            }
            Self::PayloadTooLarge {
                actual_bytes,
                max_bytes,
            } => Some(json!({
                "actual_bytes": actual_bytes,
                "max_bytes": max_bytes,
            })),
            _ => None,
        }
    }
}

#[async_trait]
impl Writer for ApiError {
    async fn write(self, _req: &mut Request, _depot: &mut Depot, res: &mut Response) {
        write_error_response(res, &self);
    }
}

pub fn write_error_response(res: &mut Response, error: &ApiError) {
    if matches!(error, ApiError::Internal { .. }) {
        tracing::error!(error = %error, "management API request failed");
    }

    if matches!(error, ApiError::Unauthorized) {
        res.headers_mut().insert(
            WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"pipe-bolt-management\""),
        );
    }

    res.status_code(error.status_code());
    res.render(Json(error.response_body()));
}

impl From<pipe_bolt_domain::DomainError> for ApiError {
    fn from(error: pipe_bolt_domain::DomainError) -> Self {
        Self::UnprocessableEntity {
            message: error.to_string(),
            details: None,
        }
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(error: serde_json::Error) -> Self {
        Self::BadRequest {
            message: format!("invalid JSON body: {error}"),
        }
    }
}

impl From<StorageError> for ApiError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::VersionConflict {
                project_id,
                expected_version,
                actual_version,
            } => Self::Conflict {
                message: "project config version conflict".to_owned(),
                details: Some(json!({
                    "project_id": project_id,
                    "expected_version": expected_version,
                    "actual_version": actual_version,
                })),
            },
            StorageError::ProjectConfigNotFound { project_id } => Self::NotFound {
                message: format!("project config '{project_id}' was not found"),
            },
            StorageError::FailureNotFound {
                project_id,
                failure_id,
            } => Self::NotFound {
                message: format!("failure '{failure_id}' was not found for project '{project_id}'"),
            },
            StorageError::FailureAlreadyResolved {
                project_id,
                failure_id,
            } => Self::Conflict {
                message: format!(
                    "failure '{failure_id}' for project '{project_id}' is already resolved"
                ),
                details: None,
            },
            StorageError::InvalidConfig { .. }
            | StorageError::InvalidField { .. }
            | StorageError::FieldTooLarge { .. }
            | StorageError::Domain(_)
            | StorageError::Json(_)
            | StorageError::VersionOverflow { .. }
            | StorageError::NumericOverflow { .. } => Self::UnprocessableEntity {
                message: error.to_string(),
                details: None,
            },
            StorageError::InvalidStoredState { .. } => Self::Internal {
                message: error.to_string(),
            },
            StorageError::InvalidSecretKey { .. }
            | StorageError::UnknownSecretKey { .. }
            | StorageError::InvalidSecretEncoding { .. }
            | StorageError::SecretCrypto { .. }
            | StorageError::Sqlx(_)
            | StorageError::Migration(_) => Self::Internal {
                message: error.to_string(),
            },
        }
    }
}

impl From<RuntimeControlError> for ApiError {
    fn from(error: RuntimeControlError) -> Self {
        match error {
            RuntimeControlError::ProjectNotManaged { project_id } => Self::NotFound {
                message: format!(
                    "runtime for project '{project_id}' is not managed by this daemon"
                ),
            },
            RuntimeControlError::ReloadInProgress => Self::Conflict {
                message: "runtime reload or shutdown is already in progress".to_owned(),
                details: None,
            },
            RuntimeControlError::InvalidConfig { reason } => Self::UnprocessableEntity {
                message: reason,
                details: None,
            },
            RuntimeControlError::CommandTemplateNotFound {
                command_template_id,
            } => Self::NotFound {
                message: format!("command template '{command_template_id}' was not found"),
            },
            RuntimeControlError::CommandRejected { reason } => Self::UnprocessableEntity {
                message: reason,
                details: None,
            },
            RuntimeControlError::UnsafeOldRuntimeShutdown { reason } => {
                Self::ServiceUnavailable { message: reason }
            }
            RuntimeControlError::RuntimeUnavailable { reason }
            | RuntimeControlError::StartFailed { reason }
            | RuntimeControlError::ShuttingDown { reason } => {
                Self::ServiceUnavailable { message: reason }
            }
            RuntimeControlError::Storage { reason } => Self::Internal { message: reason },
        }
    }
}
