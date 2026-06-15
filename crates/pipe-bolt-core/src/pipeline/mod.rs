use pipe_bolt_domain::{NormalizedEvent, PayloadSchemaMapping};

use crate::error::MqttEngineError;
use crate::message::envelope::MqttMessage;
use crate::pipeline::normalizer::{EventNormalizer, NormalizationContext};
use crate::pipeline::router::ConfigRouteMatcher;

pub mod normalizer;
pub mod router;

pub fn normalize_routed_message(
    matcher: &ConfigRouteMatcher,
    normalizer: &EventNormalizer,
    schema_mappings: &[PayloadSchemaMapping],
    message: &MqttMessage,
) -> Result<Option<NormalizedEvent>, MqttEngineError> {
    let Some(route_match) = matcher.match_message(message) else {
        return Ok(None);
    };

    let schema_mapping =
        route_match
            .route
            .schema_mapping_id
            .as_ref()
            .and_then(|schema_mapping_id| {
                schema_mappings
                    .iter()
                    .find(|mapping| &mapping.id == schema_mapping_id)
                    .cloned()
            });

    normalizer
        .normalize(
            message,
            NormalizationContext {
                project_id: route_match.project_id,
                route: (*route_match.route).clone(),
                params: route_match.params,
                schema_mapping,
            },
        )
        .map(Some)
}
