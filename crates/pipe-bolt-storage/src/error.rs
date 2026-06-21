use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("invalid storage config: {reason}")]
    InvalidConfig { reason: &'static str },

    #[error("invalid secret key: {reason}")]
    InvalidSecretKey { reason: &'static str },

    #[error("invalid secret encoding: {source}")]
    InvalidSecretEncoding {
        #[source]
        source: base64::DecodeError,
    },

    #[error("secret {operation} failed")]
    SecretCrypto { operation: &'static str },

    #[error("project config version {version} exceeds database bigint range")]
    VersionOverflow { version: u64 },

    #[error("numeric field '{field}' exceeds database bigint range")]
    NumericOverflow { field: &'static str },

    #[error("project config '{project_id}' was not found")]
    ProjectConfigNotFound { project_id: String },

    #[error("stored state is invalid: {reason}")]
    InvalidStoredState { reason: &'static str },

    #[error("domain validation failed: {0}")]
    Domain(#[from] pipe_bolt_domain::DomainError),

    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("database migration failed: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
