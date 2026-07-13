use std::sync::Arc;

use crate::auth::{AuthContext, ManagementAuth};
use crate::error::ApiError;
use crate::runtime_control::RuntimeControl;
use crate::storage::ManagementStorage;

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
