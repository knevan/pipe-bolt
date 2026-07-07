use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("invalid storage config: {reason}")]
    InvalidConfig { reason: &'static str },

    #[error("invalid field '{field}': {reason}")]
    InvalidField {
        field: &'static str,
        reason: &'static str,
    },

    #[error("field '{field}' is too large: {actual_bytes} bytes exceeds {max_bytes} bytes")]
    FieldTooLarge {
        field: &'static str,
        actual_bytes: usize,
        max_bytes: usize,
    },

    #[error("invalid secret key: {reason}")]
    InvalidSecretKey { reason: &'static str },

    #[error("unknown secret key id '{key_id}'")]
    UnknownSecretKey { key_id: String },

    #[error("invalid secret encoding: {source}")]
    InvalidSecretEncoding {
        #[source]
        source: base64::DecodeError,
    },

    #[error("secret {operation} failed")]
    SecretCrypto { operation: &'static str },

    #[error("project config version {version} exceeds database bigint range")]
    VersionOverflow { version: u64 },

    #[error("numeric field '{field}' exceeds database range")]
    NumericOverflow { field: &'static str },

    #[error("project config '{project_id}' was not found")]
    ProjectConfigNotFound { project_id: String },

    #[error(
        "project config version conflict for '{project_id}': expected {expected_version:?}, actual {actual_version:?}"
    )]
    VersionConflict {
        project_id: String,
        expected_version: Option<u64>,
        actual_version: Option<u64>,
    },

    #[error("stored state is invalid: {reason}")]
    InvalidStoredState { reason: &'static str },

    #[error("failure '{failure_id}' was not found for project '{project_id}'")]
    FailureNotFound {
        project_id: String,
        failure_id: String,
    },

    #[error("failure '{failure_id}' for project '{project_id}' is already resolved")]
    FailureAlreadyResolved {
        project_id: String,
        failure_id: String,
    },

    #[error("domain validation failed: {0}")]
    Domain(#[from] pipe_bolt_domain::DomainError),

    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("database migration failed: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
