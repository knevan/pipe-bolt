use std::collections::BTreeMap;
use std::time::SystemTime;

use pipe_bolt_domain::{
    DecodedPayload, DeviceIdExtraction, EventId, FieldMapping, FieldValue, NormalizedEvent,
    PayloadCodecKind, PayloadSchemaMapping, ProjectId, RawPayloadRef, TopicName, TopicRouteConfig,
};
use salvo::http::cookie::time::OffsetDateTime;
use serde_json::Value;
use uuid::Uuid;

use crate::error::MqttEngineError;
use crate::message::envelope::MqttMessage;
use crate::router::matcher::TopicParams;
use crate::web::realtime::error::PipelineError;

#[derive(Debug, Clone)]
pub struct NormalizationContext {
    pub project_id: ProjectId,
    pub route: TopicRouteConfig,
    pub params: TopicParams,
    pub schema_mapping: Option<PayloadSchemaMapping>,
}

#[derive(Debug, Clone)]
pub struct EventNormalizer {
    limits: NormalizerLimits,
}

impl Default for EventNormalizer {
    fn default() -> Self {
        Self::new(NormalizerLimits::default())
    }
}

impl EventNormalizer {
    pub fn new(limits: NormalizerLimits) -> Self {
        Self { limits }
    }

    pub fn normalize(
        &self,
        message: &MqttMessage,
        context: NormalizationContext,
    ) -> Result<NormalizedEvent, MqttEngineError> {
        context.route.validate()?;
        validate_payload_size(message.payload(), self.limits.max_payload_size_bytes)?;

        let decoded_payload = decode_payload(
            context.route.codec,
            message.payload(),
            self.limits.max_json_depth,
        )?;
        let device_id = extract_device_id(&context.route, &context.params, &decoded_payload)?;
        let fields = extract_fields(
            context.schema_mapping.as_ref(),
            &decoded_payload,
            self.limits.max_extracted_fields,
        )?;
        let topic = TopicName::new(message.topic().to_owned())?;
        let raw = build_raw_payload_ref(
            message.payload(),
            context.route.codec,
            self.limits.retain_raw_payload,
            self.limits.max_raw_payload_size_bytes,
        )?;
        let event_id = EventId::new(format!("evt_{}", Uuid::now_v7()))?;

        Ok(NormalizedEvent {
            correlation_id: event_id.as_str().to_owned(),
            id: event_id,
            project_id: context.project_id,
            broker_id: context.route.broker_id.clone(),
            route_id: context.route.id.clone(),
            schema_mapping_id: context
                .schema_mapping
                .as_ref()
                .map(|mapping| mapping.id.clone()),
            topic,
            device_id,
            event_type: context.route.event_type,
            received_at: system_time_to_offset(message.received_at()),
            payload_size_bytes: message.payload().len(),
            payload: decoded_payload,
            fields,
            raw,
            normalization_errors: Vec::new(),
            metadata: BTreeMap::new(),
        })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct NormalizerLimits {
    pub max_payload_size_bytes: usize,
    pub max_raw_payload_size_bytes: usize,
    pub max_json_depth: usize,
    pub max_extracted_fields: usize,
    pub retain_raw_payload: bool,
}

impl Default for NormalizerLimits {
    fn default() -> Self {
        Self {
            max_payload_size_bytes: 256 * 1024,
            max_raw_payload_size_bytes: 64 * 1024,
            max_json_depth: 32,
            max_extracted_fields: 128,
            retain_raw_payload: true,
        }
    }
}

fn validate_payload_size(payload: &[u8], max: usize) -> Result<(), MqttEngineError> {
    if payload.len() > max {
        return Err(PipelineError::PayloadTooLarge {
            actual: payload.len(),
            max,
        }
        .into());
    }

    Ok(())
}

fn decode_payload(
    codec: PayloadCodecKind,
    payload: &[u8],
    max_json_depth: usize,
) -> Result<DecodedPayload, MqttEngineError> {
    match codec {
        PayloadCodecKind::Json => {
            let value = serde_json::from_slice::<Value>(payload).map_err(|error| {
                MqttEngineError::from(PipelineError::InvalidJson {
                    message: error.to_string(),
                })
            })?;

            let depth = json_depth(&value);
            if depth > max_json_depth {
                return Err(PipelineError::JsonTooDeep {
                    actual: depth,
                    max: max_json_depth,
                }
                .into());
            }

            Ok(DecodedPayload::Json(value))
        }
        PayloadCodecKind::Raw => Ok(DecodedPayload::Raw(payload.to_vec())),
    }
}

fn json_depth(value: &Value) -> usize {
    match value {
        Value::Array(values) => values.iter().map(json_depth).max().unwrap_or(0) + 1,
        Value::Object(values) => values.values().map(json_depth).max().unwrap_or(0) + 1,
        _ => 1,
    }
}

fn build_raw_payload_ref(
    payload: &[u8],
    codec: PayloadCodecKind,
    retain_raw_payload: bool,
    max_raw_payload_size_bytes: usize,
) -> Result<Option<RawPayloadRef>, MqttEngineError> {
    if !retain_raw_payload {
        return Ok(None);
    }

    if payload.len() > max_raw_payload_size_bytes {
        return Err(PipelineError::RawPayloadTooLarge {
            actual: payload.len(),
            max: max_raw_payload_size_bytes,
        }
        .into());
    }

    Ok(Some(RawPayloadRef {
        byte_len: payload.len(),
        content_type: content_type_for_codec(codec).map(str::to_owned),
        bytes: payload.to_vec(),
    }))
}

fn extract_device_id(
    route: &TopicRouteConfig,
    params: &TopicParams,
    payload: &DecodedPayload,
) -> Result<Option<String>, MqttEngineError> {
    match &route.device_id {
        DeviceIdExtraction::None => Ok(None),
        DeviceIdExtraction::Static { value } => Ok(Some(value.clone())),
        DeviceIdExtraction::TopicWildcardIndex { index } => {
            Ok(params.single(*index).map(str::to_owned))
        }
        DeviceIdExtraction::PayloadField { path } => match payload {
            DecodedPayload::Json(value) => match get_json_path(value, path.segments()) {
                Some(Value::String(value)) => Ok(Some(value.clone())),
                Some(value) if value.is_number() || value.is_boolean() => {
                    Ok(Some(value.to_string()))
                }
                Some(_) => Err(PipelineError::InvalidDeviceIdFieldType.into()),
                None => Ok(None),
            },
            DecodedPayload::Raw(_) => Err(PipelineError::DeviceIdRequiresJson.into()),
        },
    }
}

fn extract_fields(
    mapping: Option<&PayloadSchemaMapping>,
    payload: &DecodedPayload,
    max_extracted_fields: usize,
) -> Result<BTreeMap<String, FieldValue>, MqttEngineError> {
    let Some(mapping) = mapping else {
        return Ok(BTreeMap::new());
    };

    mapping.validate()?;

    if mapping.fields.len() > max_extracted_fields {
        return Err(PipelineError::TooManyExtractedFields {
            actual: mapping.fields.len(),
            max: max_extracted_fields,
        }
        .into());
    }

    let DecodedPayload::Json(value) = payload else {
        return Err(PipelineError::MappingRequiresJson.into());
    };

    if !value.is_object() {
        return Err(PipelineError::MappingRequiresJsonObject.into());
    }

    let mut fields = BTreeMap::new();

    for field in &mapping.fields {
        extract_field(field, value, &mut fields)?;
    }

    Ok(fields)
}

fn extract_field(
    field: &FieldMapping,
    payload: &Value,
    fields: &mut BTreeMap<String, FieldValue>,
) -> Result<(), MqttEngineError> {
    match get_json_path(payload, field.source.segments()) {
        Some(value) => {
            let value = coerce_field_value(&field.target, value.clone(), field.value_type)?;
            fields.insert(field.target.clone(), value);
            Ok(())
        }
        None if field.required => Err(PipelineError::MissingRequiredField {
            target: field.target.clone(),
            source_path: field.source.to_string(),
        }
        .into()),
        None => {
            if let Some(default) = &field.default {
                let value = coerce_field_value(&field.target, default.clone(), field.value_type)?;
                fields.insert(field.target.clone(), value);
            }

            Ok(())
        }
    }
}

fn coerce_field_value(
    target: &str,
    value: Value,
    expected: pipe_bolt_domain::FieldValueType,
) -> Result<FieldValue, MqttEngineError> {
    let actual = json_type_name(&value);

    match expected {
        pipe_bolt_domain::FieldValueType::String if value.is_string() => {
            Ok(FieldValue::from_json(value))
        }
        pipe_bolt_domain::FieldValueType::Number if value.is_number() => {
            Ok(FieldValue::from_json(value))
        }
        pipe_bolt_domain::FieldValueType::Boolean if value.is_boolean() => {
            Ok(FieldValue::from_json(value))
        }
        pipe_bolt_domain::FieldValueType::Object if value.is_object() => {
            Ok(FieldValue::from_json(value))
        }
        pipe_bolt_domain::FieldValueType::Array if value.is_array() => {
            Ok(FieldValue::from_json(value))
        }
        expected => Err(PipelineError::TypeMismatch {
            target: target.to_owned(),
            expected: field_value_type_name(expected),
            actual,
        }
        .into()),
    }
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn field_value_type_name(value_type: pipe_bolt_domain::FieldValueType) -> &'static str {
    match value_type {
        pipe_bolt_domain::FieldValueType::String => "string",
        pipe_bolt_domain::FieldValueType::Number => "number",
        pipe_bolt_domain::FieldValueType::Boolean => "boolean",
        pipe_bolt_domain::FieldValueType::Object => "object",
        pipe_bolt_domain::FieldValueType::Array => "array",
    }
}

fn get_json_path<I>(value: &Value, segments: I) -> Option<&Value>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut current = value;

    for segment in segments {
        current = current.get(segment.as_ref())?;
    }

    Some(current)
}

fn system_time_to_offset(value: SystemTime) -> OffsetDateTime {
    OffsetDateTime::from(value)
}

fn content_type_for_codec(codec: PayloadCodecKind) -> Option<&'static str> {
    match codec {
        PayloadCodecKind::Json => Some("application/json"),
        PayloadCodecKind::Raw => None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use pipe_bolt_domain::{
        BackpressurePolicy, BrokerId, DeviceIdExtraction, FieldMapping, FieldPath, FieldValueType,
        MqttQos, PayloadCodecKind, PayloadSchemaMapping, ProjectId, RouteId, SchemaMappingId,
        TopicFilter, TopicRouteConfig,
    };
    use rumqttc::QoS;

    use super::*;
    use crate::message::envelope::MqttMessage;
    use crate::router::matcher::TopicParams;

    fn route(device_id: DeviceIdExtraction) -> TopicRouteConfig {
        TopicRouteConfig {
            id: RouteId::new("route-1").unwrap(),
            broker_id: BrokerId::new("broker-1").unwrap(),
            name: "telemetry".to_owned(),
            topic_filter: TopicFilter::new("devices/+/telemetry").unwrap(),
            codec: PayloadCodecKind::Json,
            schema_mapping_id: Some(SchemaMappingId::new("mapping-1").unwrap()),
            device_id,
            event_type: "telemetry".to_owned(),
            qos: MqttQos::AtLeastOnce,
            enabled: true,
            backpressure: BackpressurePolicy::DropOldest,
        }
    }

    fn mapping(required: bool) -> PayloadSchemaMapping {
        PayloadSchemaMapping {
            id: SchemaMappingId::new("mapping-1").unwrap(),
            name: "telemetry".to_owned(),
            fields: vec![FieldMapping {
                source: FieldPath::new("temperature").unwrap(),
                target: "temperature".to_owned(),
                value_type: FieldValueType::Number,
                required,
                default: None,
            }],
        }
    }

    fn message(payload: &[u8]) -> MqttMessage {
        MqttMessage::new(
            "devices/device-1/telemetry",
            QoS::AtLeastOnce,
            false,
            payload.to_vec(),
            SystemTime::now(),
        )
        .unwrap()
    }

    #[test]
    fn json_decode_success_extracts_fields() {
        let mut params = TopicParams::default();
        params.push_single("device-1");

        let event = EventNormalizer::default()
            .normalize(
                &message(br#"{"temperature":25.5}"#),
                NormalizationContext {
                    project_id: ProjectId::new("project-1").unwrap(),
                    route: route(DeviceIdExtraction::TopicWildcardIndex { index: 0 }),
                    params,
                    schema_mapping: Some(mapping(true)),
                },
            )
            .unwrap();

        assert_eq!(event.device_id.as_deref(), Some("device-1"));
        assert!(event.field("temperature").is_some());
        assert_eq!(
            event.schema_mapping_id.as_ref().unwrap().as_str(),
            "mapping-1"
        );
    }

    #[test]
    fn invalid_json_fails_before_rule_engine() {
        let error = EventNormalizer::default()
            .normalize(
                &message(b"not-json"),
                NormalizationContext {
                    project_id: ProjectId::new("project-1").unwrap(),
                    route: route(DeviceIdExtraction::None),
                    params: TopicParams::default(),
                    schema_mapping: Some(mapping(true)),
                },
            )
            .unwrap_err();

        assert!(matches!(
            error,
            MqttEngineError::Pipeline(PipelineError::InvalidJson { .. })
        ));
    }

    #[test]
    fn required_mapping_missing_fails() {
        let error = EventNormalizer::default()
            .normalize(
                &message(br#"{"humidity":70}"#),
                NormalizationContext {
                    project_id: ProjectId::new("project-1").unwrap(),
                    route: route(DeviceIdExtraction::None),
                    params: TopicParams::default(),
                    schema_mapping: Some(mapping(true)),
                },
            )
            .unwrap_err();

        assert!(matches!(
            error,
            MqttEngineError::Pipeline(PipelineError::MissingRequiredField { .. })
        ));
    }

    #[test]
    fn optional_mapping_missing_is_skipped() {
        let event = EventNormalizer::default()
            .normalize(
                &message(br#"{"humidity":70}"#),
                NormalizationContext {
                    project_id: ProjectId::new("project-1").unwrap(),
                    route: route(DeviceIdExtraction::None),
                    params: TopicParams::default(),
                    schema_mapping: Some(mapping(false)),
                },
            )
            .unwrap();

        assert!(event.field("temperature").is_none());
    }

    #[test]
    fn payload_size_limit_is_enforced() {
        let normalizer = EventNormalizer::new(NormalizerLimits {
            max_payload_size_bytes: 4,
            ..NormalizerLimits::default()
        });

        let error = normalizer
            .normalize(
                &message(br#"{"x":1}"#),
                NormalizationContext {
                    project_id: ProjectId::new("project-1").unwrap(),
                    route: route(DeviceIdExtraction::None),
                    params: TopicParams::default(),
                    schema_mapping: None,
                },
            )
            .unwrap_err();

        assert!(matches!(
            error,
            MqttEngineError::Pipeline(PipelineError::PayloadTooLarge { .. })
        ));
    }
}
