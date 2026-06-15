use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::DomainError;

const MAX_ID_BYTES: usize = 128;

macro_rules! define_id {
    ($name:ident, $field:literal) => {
        #[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
                let value = value.into();
                validate_identifier($field, &value)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.0)
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = DomainError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = DomainError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl<'a> TryFrom<&'a str> for $name {
            type Error = DomainError;

            fn try_from(value: &'a str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

define_id!(TenantId, "tenant_id");
define_id!(UserId, "user_id");
define_id!(ProjectId, "project_id");
define_id!(BrokerId, "broker_id");
define_id!(RouteId, "route_id");
define_id!(SchemaMappingId, "schema_mapping_id");
define_id!(RuleId, "rule_id");
define_id!(SinkId, "sink_id");
define_id!(CommandTemplateId, "command_template_id");
define_id!(CommandExecutionId, "command_execution_id");
define_id!(EventId, "event_id");

pub(crate) fn validate_identifier(field: &'static str, value: &str) -> Result<(), DomainError> {
    validate_text(field, value, MAX_ID_BYTES)?;

    let valid = value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'));

    if !valid {
        return Err(DomainError::InvalidControlCharacters { field });
    }

    Ok(())
}

pub(crate) fn validate_text(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), DomainError> {
    if value.trim().is_empty() {
        return Err(DomainError::EmptyField { field });
    }

    if value.len() > max_bytes {
        return Err(DomainError::FieldTooLong {
            field,
            max: max_bytes,
        });
    }

    if value.chars().any(char::is_control) {
        return Err(DomainError::InvalidControlCharacters { field });
    }

    Ok(())
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FieldPath(String);

impl FieldPath {
    pub fn new(path: impl Into<String>) -> Result<Self, DomainError> {
        let path = path.into();
        validate_field_path(&path)?;
        Ok(Self(path))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('.')
    }
}

impl fmt::Display for FieldPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for FieldPath {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

pub(crate) fn validate_field_path(path: &str) -> Result<(), DomainError> {
    validate_text("field_path", path, 256)?;

    if path.starts_with('.') || path.ends_with('.') || path.contains("..") {
        return Err(DomainError::InvalidFieldPath {
            reason: "path must contain non-empty dot-separated segments",
        });
    }

    for segment in path.split('.') {
        let valid = segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));

        if !valid {
            return Err(DomainError::InvalidFieldPath {
                reason: "path segment contains unsupported characters",
            });
        }
    }

    Ok(())
}

pub(crate) fn borrowed_or_owned<'a>(value: &'a str) -> Cow<'a, str> {
    Cow::Borrowed(value)
}
