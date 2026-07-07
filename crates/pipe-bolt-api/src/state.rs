use std::sync::Arc;

use pipe_bolt_domain::UserId;
use subtle::ConstantTimeEq;

use crate::error::ApiError;
use crate::runtime_control::RuntimeControl;
use crate::storage::ManagementStorage;

pub const AUTH_CONTEXT_KEY: &str = "pipe_bolt.auth_context";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthContext {
    actor_id: UserId,
}

impl AuthContext {
    pub fn new(actor_id: UserId) -> Self {
        Self { actor_id }
    }

    pub const fn actor_id(&self) -> &UserId {
        &self.actor_id
    }
}

#[derive(Clone)]
pub struct ApiState {
    storage: Arc<dyn ManagementStorage>,
    runtime: Arc<dyn RuntimeControl>,
    auth: ManagementAuth,
    max_config_body_bytes: usize,
}

impl ApiState {
    pub fn new(
        storage: Arc<dyn ManagementStorage>,
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

    pub fn storage(&self) -> &Arc<dyn ManagementStorage> {
        &self.storage
    }

    pub fn runtime(&self) -> &Arc<dyn RuntimeControl> {
        &self.runtime
    }

    pub const fn max_config_body_bytes(&self) -> usize {
        self.max_config_body_bytes
    }

    pub fn authenticate(
        &self,
        authorization_header: Option<&str>,
    ) -> Result<AuthContext, ApiError> {
        self.auth.authenticate(authorization_header)
    }
}

#[derive(Clone)]
pub struct ManagementAuth {
    bearer_token: String,
    actor_id: UserId,
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
            actor_id: UserId::new("system:bootstrap-token")?,
        })
    }

    fn authenticate(&self, authorization_header: Option<&str>) -> Result<AuthContext, ApiError> {
        let Some(header) = authorization_header else {
            return Err(ApiError::Unauthorized);
        };

        let Some(token) = header.strip_prefix("Bearer ") else {
            return Err(ApiError::Unauthorized);
        };

        let matches = token.as_bytes().ct_eq(self.bearer_token.as_bytes()).into();
        if matches {
            Ok(AuthContext::new(self.actor_id.clone()))
        } else {
            Err(ApiError::Unauthorized)
        }
    }
}
