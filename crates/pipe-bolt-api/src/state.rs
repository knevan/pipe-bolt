use std::sync::Arc;

use pipe_bolt_storage::postgres::PostgresStorage;
use subtle::ConstantTimeEq;

use crate::error::ApiError;
use crate::runtime_control::RuntimeControl;

#[derive(Clone)]
pub struct ApiState {
    storage: Arc<PostgresStorage>,
    runtime: Arc<dyn RuntimeControl>,
    auth: ManagementAuth,
    max_config_body_bytes: usize,
}

impl ApiState {
    pub fn new(
        storage: Arc<PostgresStorage>,
        runtime: Arc<dyn RuntimeControl>,
        auth: ManagementAuth,
        max_config_body_bytes: usize,
    ) -> Self {
        Self {
            storage,
            runtime,
            auth,
            max_config_body_bytes,
        }
    }

    pub fn storage(&self) -> &Arc<PostgresStorage> {
        &self.storage
    }

    pub fn runtime(&self) -> &Arc<dyn RuntimeControl> {
        &self.runtime
    }

    pub const fn max_config_body_bytes(&self) -> usize {
        self.max_config_body_bytes
    }

    pub fn authorize(&self, authorization_header: Option<&str>) -> Result<(), ApiError> {
        self.auth.authorize(authorization_header)
    }
}

#[derive(Clone)]
pub struct ManagementAuth {
    bearer_token: String,
}

impl ManagementAuth {
    pub fn bearer(token: impl Into<String>) -> Result<Self, ApiError> {
        let token = token.into();
        if token.trim().is_empty() {
            return Err(ApiError::Internal {
                message: "management bearer token must not be empty".to_owned(),
            });
        }

        Ok(Self {
            bearer_token: token,
        })
    }

    fn authorize(&self, authorization_header: Option<&str>) -> Result<(), ApiError> {
        let Some(header) = authorization_header else {
            return Err(ApiError::Unauthorized);
        };

        let Some(token) = header.strip_prefix("Bearer ") else {
            return Err(ApiError::Unauthorized);
        };

        let matches = token.as_bytes().ct_eq(self.bearer_token.as_bytes()).into();

        if matches {
            Ok(())
        } else {
            Err(ApiError::Unauthorized)
        }
    }
}
