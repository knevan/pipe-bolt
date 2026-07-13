use std::fmt;
use std::sync::Arc;

use pipe_bolt_domain::{ProjectId, UserId};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::ApiError;

pub const AUTH_CONTEXT_KEY: &str = "pipe_bolt.auth_context";

const MIN_BEARER_TOKEN_BYTES: usize = 32;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ManagementPermission {
    ProjectRead,
    ProjectConfigWrite,
    CommandExecute,
    RuntimeReload,
    FailureResolve,
}

impl ManagementPermission {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProjectRead => "project:read",
            Self::ProjectConfigWrite => "project_config:write",
            Self::CommandExecute => "command:execute",
            Self::RuntimeReload => "runtime:reload",
            Self::FailureResolve => "failure:resolve",
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ManagementRole {
    Admin,
    Operator,
    Viewer,
}

impl ManagementRole {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "admin" => Some(Self::Admin),
            "operator" => Some(Self::Operator),
            "viewer" => Some(Self::Viewer),
            _ => None,
        }
    }

    pub const fn allows(self, permission: ManagementPermission) -> bool {
        match self {
            Self::Admin => true,
            Self::Operator => matches!(
                permission,
                ManagementPermission::ProjectRead
                    | ManagementPermission::CommandExecute
                    | ManagementPermission::RuntimeReload
                    | ManagementPermission::FailureResolve
            ),
            Self::Viewer => matches!(permission, ManagementPermission::ProjectRead),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Operator => "operator",
            Self::Viewer => "viewer",
        }
    }
}

impl fmt::Display for ManagementRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ManagementProjectScope {
    All,
    Projects(Arc<[ProjectId]>),
}

impl ManagementProjectScope {
    pub const fn all() -> Self {
        Self::All
    }

    pub fn projects(projects: Vec<ProjectId>) -> Self {
        Self::Projects(Arc::from(projects))
    }

    pub fn allows(&self, project_id: &ProjectId) -> bool {
        match self {
            Self::All => true,
            Self::Projects(projects) => projects.iter().any(|allowed| allowed == project_id),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthContext {
    actor_id: UserId,
    role: ManagementRole,
    project_scope: ManagementProjectScope,
}

impl AuthContext {
    pub fn new(
        actor_id: UserId,
        role: ManagementRole,
        project_scope: ManagementProjectScope,
    ) -> Self {
        Self {
            actor_id,
            role,
            project_scope,
        }
    }

    pub const fn actor_id(&self) -> &UserId {
        &self.actor_id
    }

    pub const fn role(&self) -> ManagementRole {
        self.role
    }

    pub fn authorize_project(
        &self,
        project_id: &ProjectId,
        permission: ManagementPermission,
    ) -> Result<(), ApiError> {
        if !self.role.allows(permission) {
            return Err(ApiError::Forbidden {
                message: format!(
                    "permission '{}' is required for role '{}'",
                    permission.as_str(),
                    self.role
                ),
            });
        }

        if !self.project_scope.allows(project_id) {
            return Err(ApiError::Forbidden {
                message: format!(
                    "actor '{}' is not authorized for project '{}'",
                    self.actor_id, project_id
                ),
            });
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct ManagementAuth {
    tokens: Arc<[ManagementToken]>,
}

impl fmt::Debug for ManagementAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagementAuth")
            .field("token_count", &self.tokens.len())
            .finish_non_exhaustive()
    }
}

impl ManagementAuth {
    pub fn bearer(token: impl Into<String>) -> Result<Self, ApiError> {
        Self::bearer_with_context(
            token,
            UserId::new("system:bootstrap-token")?,
            ManagementRole::Admin,
            ManagementProjectScope::all(),
        )
    }

    pub fn bearer_with_context(
        token: impl Into<String>,
        actor_id: UserId,
        role: ManagementRole,
        project_scope: ManagementProjectScope,
    ) -> Result<Self, ApiError> {
        let context = AuthContext::new(actor_id, role, project_scope);
        Ok(Self {
            tokens: Arc::from([ManagementToken::new(token.into(), context)?]),
        })
    }

    pub fn authenticate(
        &self,
        authorization_header: Option<&str>,
    ) -> Result<AuthContext, ApiError> {
        let token = parse_bearer_token(authorization_header)?;
        let presented_digest = bearer_token_digest(token);
        let mut matched = None;

        for configured in self.tokens.iter() {
            let token_matches: bool = configured.digest.ct_eq(&presented_digest).into();
            if token_matches {
                matched = Some(configured.context.clone());
            }
        }

        matched.ok_or(ApiError::Unauthorized)
    }
}

#[derive(Clone)]
struct ManagementToken {
    digest: [u8; 32],
    context: AuthContext,
}

impl ManagementToken {
    fn new(token: String, context: AuthContext) -> Result<Self, ApiError> {
        validate_bearer_token_secret(&token)?;
        Ok(Self {
            digest: bearer_token_digest(&token),
            context,
        })
    }
}

fn parse_bearer_token(authorization_header: Option<&str>) -> Result<&str, ApiError> {
    let Some(header) = authorization_header else {
        return Err(ApiError::Unauthorized);
    };
    let Some((scheme, token)) = header.split_once(' ') else {
        return Err(ApiError::Unauthorized);
    };

    if !scheme.eq_ignore_ascii_case("Bearer") || !bearer_token_format_is_valid(token) {
        return Err(ApiError::Unauthorized);
    }

    Ok(token)
}

fn validate_bearer_token_secret(token: &str) -> Result<(), ApiError> {
    if !bearer_token_format_is_valid(token) || token.len() < MIN_BEARER_TOKEN_BYTES {
        return Err(ApiError::Internal {
            message: format!(
                "management bearer token must be at least {MIN_BEARER_TOKEN_BYTES} bytes and contain no whitespace"
            ),
        });
    }

    Ok(())
}

fn bearer_token_digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

fn bearer_token_format_is_valid(token: &str) -> bool {
    !token.is_empty() && !token.bytes().any(|byte| byte.is_ascii_whitespace())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_TOKEN: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn role_should_allow_read_when_viewer() {
        assert!(ManagementRole::Viewer.allows(ManagementPermission::ProjectRead));
    }

    #[test]
    fn role_should_reject_config_write_when_viewer() {
        assert!(!ManagementRole::Viewer.allows(ManagementPermission::ProjectConfigWrite));
    }

    #[test]
    fn auth_should_reject_short_bearer_token_when_configured() {
        let error = ManagementAuth::bearer("short").expect_err("short token rejected");

        assert!(matches!(error, ApiError::Internal { .. }));
    }

    #[test]
    fn auth_should_accept_case_insensitive_bearer_scheme() {
        let auth = ManagementAuth::bearer(TEST_TOKEN).expect("auth config");
        let context = auth
            .authenticate(Some("bearer 0123456789abcdef0123456789abcdef"))
            .expect("valid bearer token");

        assert_eq!(context.role(), ManagementRole::Admin);
    }
}
