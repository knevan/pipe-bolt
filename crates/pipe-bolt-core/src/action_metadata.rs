const RESERVED_METADATA_PREFIXES: [&str; 2] = ["pipe_bolt.", "_pipe_bolt."];

/// Static limits for metadata mutations produced by actions.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) struct ActionMetadataLimits {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
}

impl ActionMetadataLimits {
    pub(crate) const fn new(max_key_bytes: usize, max_value_bytes: usize) -> Self {
        Self {
            max_key_bytes,
            max_value_bytes,
        }
    }
}

/// Error returned when action-controlled metadata is invalid.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum MetadataValidationError {
    InvalidKey { reason: &'static str },
    ValueTooLarge { actual: usize, max: usize },
}

/// Validates metadata mutations before they are applied to an event snapshot.
pub(crate) fn validate_action_metadata(
    key: &str,
    value: &str,
    limits: ActionMetadataLimits,
) -> Result<(), MetadataValidationError> {
    if key.is_empty() {
        return Err(MetadataValidationError::InvalidKey {
            reason: "key must not be empty",
        });
    }

    if key.len() > limits.max_key_bytes {
        return Err(MetadataValidationError::InvalidKey {
            reason: "key exceeds maximum length",
        });
    }

    if key.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(MetadataValidationError::InvalidKey {
            reason: "key must not contain control characters",
        });
    }

    if RESERVED_METADATA_PREFIXES
        .iter()
        .any(|prefix| key.starts_with(prefix))
    {
        return Err(MetadataValidationError::InvalidKey {
            reason: "key uses reserved prefix",
        });
    }

    if value.len() > limits.max_value_bytes {
        return Err(MetadataValidationError::ValueTooLarge {
            actual: value.len(),
            max: limits.max_value_bytes,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> ActionMetadataLimits {
        ActionMetadataLimits::new(16, 8)
    }

    #[test]
    fn accepts_valid_metadata() {
        assert_eq!(
            validate_action_metadata("severity", "hot", limits()),
            Ok(())
        );
    }

    #[test]
    fn rejects_empty_key() {
        assert!(matches!(
            validate_action_metadata("", "hot", limits()),
            Err(MetadataValidationError::InvalidKey { .. })
        ));
    }

    #[test]
    fn rejects_control_characters_in_key() {
        assert!(matches!(
            validate_action_metadata("bad\nkey", "hot", limits()),
            Err(MetadataValidationError::InvalidKey { .. })
        ));
    }

    #[test]
    fn rejects_reserved_prefix() {
        assert!(matches!(
            validate_action_metadata("pipe_bolt.internal", "hot", limits()),
            Err(MetadataValidationError::InvalidKey { .. })
        ));
    }

    #[test]
    fn rejects_large_value() {
        assert_eq!(
            validate_action_metadata("severity", "too-large", limits()),
            Err(MetadataValidationError::ValueTooLarge { actual: 9, max: 8 })
        );
    }
}
