use salvo::prelude::*;
use serde::Deserialize;

use crate::bus::TelemetryEvent;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct TelemetryFilter {
    pub device: Option<String>,
    pub topic: Option<String>,
    pub topic_prefix: Option<String>,
    pub event_type: Option<String>,
}

impl TelemetryFilter {
    pub(crate) fn normalized(self) -> Self {
        Self {
            device: normalize_optional(self.device),
            topic: normalize_optional(self.topic),
            topic_prefix: normalize_optional(self.topic_prefix),
            event_type: normalize_optional(self.event_type),
        }
    }

    /// Matches telemetry events using exact values and segment-aware prefixes only.
    ///
    /// This intentionally does not support MQTT wildcards from client input.
    pub(crate) fn matches(&self, event: &TelemetryEvent) -> bool {
        if let Some(expected) = self.device.as_deref()
            && telemetry_device(&event.topic) != Some(expected)
        {
            return false;
        }

        if let Some(expected) = self.topic.as_deref()
            && event.topic != expected
        {
            return false;
        }

        if let Some(expected) = self.topic_prefix.as_deref()
            && !topic_matches_prefix(&event.topic, expected)
        {
            return false;
        }

        if let Some(expected) = self.event_type.as_deref()
            && telemetry_event_type(&event.topic) != Some(expected)
        {
            return false;
        }

        true
    }
}

pub(crate) fn parse_filter(req: &mut Request) -> Result<TelemetryFilter, StatusError> {
    let filter = TelemetryFilter {
        device: req.query::<String>("device"),
        topic: req.query::<String>("topic"),
        topic_prefix: req.query::<String>("topic_prefix"),
        event_type: req.query::<String>("event_type"),
    };

    validate_filter(&filter)?;
    Ok(filter)
}

pub(crate) fn topic_matches_prefix(topic: &str, prefix: &str) -> bool {
    // Use segment-aware prefix matching to avoid matching `devices/a` against `devices/abc`.
    topic == prefix
        || topic
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

pub(crate) fn telemetry_device(topic: &str) -> Option<&str> {
    let mut levels = topic.split('/');
    let namespace = levels.next()?;

    if namespace == "devices" || namespace == "device" {
        levels.next().filter(|device| !device.is_empty())
    } else {
        None
    }
}

pub(crate) fn telemetry_event_type(topic: &str) -> Option<&str> {
    let levels: Vec<&str> = topic.split('/').collect();

    match levels.as_slice() {
        ["devices" | "device", _, "telemetry", ..] => Some("telemetry"),
        ["devices" | "device", _, "status", ..] => Some("status"),
        ["devices" | "device", _, "event", event_type, ..] if !event_type.is_empty() => {
            Some(event_type)
        }
        _ => None,
    }
}

fn validate_filter(filter: &TelemetryFilter) -> Result<(), StatusError> {
    if let Some(topic) = filter.topic.as_deref() {
        validate_topic_like("topic", topic)?;
    }

    if let Some(topic_prefix) = filter.topic_prefix.as_deref() {
        validate_topic_like("topic_prefix", topic_prefix)?;
    }

    if let Some(device) = filter.device.as_deref() {
        validate_simple_filter_value("device", device)?;
    }

    if let Some(event_type) = filter.event_type.as_deref() {
        validate_simple_filter_value("event_type", event_type)?;
    }

    Ok(())
}

fn validate_topic_like(name: &'static str, value: &str) -> Result<(), StatusError> {
    if value.trim().is_empty() || value.contains('+') || value.contains('#') {
        return Err(StatusError::bad_request().brief(format!(
            "{name} must not be empty and must not contain MQTT wildcards"
        )));
    }

    Ok(())
}

fn validate_simple_filter_value(name: &'static str, value: &str) -> Result<(), StatusError> {
    if value.trim().is_empty() || value.contains('/') || value.contains('+') || value.contains('#')
    {
        return Err(StatusError::bad_request()
            .brief(format!("{name} must be a non-empty single topic segment")));
    }

    Ok(())
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_owned();
        if value.is_empty() { None } else { Some(value) }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_prefix_matches_topic_segments_only() {
        // Match exact topic
        assert!(topic_matches_prefix("devices/a", "devices/a"));
        // Match subtree with slash
        assert!(topic_matches_prefix("devices/a/telemetry", "devices/a"));
        // Reject partial segment match
        assert!(!topic_matches_prefix("devices/abc/telemetry", "devices/a"));
    }
}
