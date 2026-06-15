use std::sync::Arc;

use pipe_bolt_domain::{ProjectId, TopicRouteConfig};

use crate::error::MqttEngineError;
use crate::message::envelope::MqttMessage;
use crate::router::matcher::TopicParams;

#[derive(Debug, Clone)]
pub struct ConfigRouteMatch {
    pub project_id: ProjectId,
    pub route: Arc<TopicRouteConfig>,
    pub params: TopicParams,
}

#[derive(Debug, Clone)]
pub struct ConfigRouteMatcher {
    project_id: ProjectId,
    routes: Arc<Vec<CompiledConfigRoute>>,
}

impl ConfigRouteMatcher {
    pub fn new(
        project_id: ProjectId,
        routes: Vec<TopicRouteConfig>,
    ) -> Result<Self, MqttEngineError> {
        let mut compiled = Vec::with_capacity(routes.len());
        let mut route_ids = std::collections::HashSet::with_capacity(routes.len());

        for route in routes {
            if !route_ids.insert(route.id.clone()) {
                return Err(MqttEngineError::InvalidConfig("duplicate route id"));
            }

            if !route.enabled {
                continue;
            }

            route.validate()?;
            compiled.push(CompiledConfigRoute::new(route)?);
        }

        Ok(Self {
            project_id,
            routes: Arc::new(compiled),
        })
    }

    /// Matches a message against enabled compiled routes.
    ///
    /// Matching is deterministic: routes are evaluated in config order and the first match wins.
    /// Disabled routes are ignored at compile time. Invalid filters fail in `new`.
    pub fn match_message(&self, message: &MqttMessage) -> Option<ConfigRouteMatch> {
        for route in self.routes.iter() {
            if let Some(params) = route.matcher.matches(message.topic()) {
                return Some(ConfigRouteMatch {
                    project_id: self.project_id.clone(),
                    route: Arc::clone(&route.config),
                    params,
                });
            }
        }

        None
    }

    pub fn len(&self) -> usize {
        self.routes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

#[derive(Debug, Clone)]
struct CompiledConfigRoute {
    config: Arc<TopicRouteConfig>,
    matcher: TopicFilterMatcher,
}

impl CompiledConfigRoute {
    fn new(config: TopicRouteConfig) -> Result<Self, MqttEngineError> {
        let matcher = TopicFilterMatcher::parse(config.topic_filter.as_str())?;

        Ok(Self {
            config: Arc::new(config),
            matcher,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct TopicFilterMatcher {
    levels: Vec<TopicFilterLevel>,
}

impl TopicFilterMatcher {
    fn parse(filter: &str) -> Result<Self, MqttEngineError> {
        pipe_bolt_domain::TopicFilter::new(filter.to_owned())?;

        let levels = filter
            .split('/')
            .map(|level| match level {
                "+" => TopicFilterLevel::SingleWildCard,
                "#" => TopicFilterLevel::MultiWildCard,
                value => TopicFilterLevel::Exact(value.to_owned()),
            })
            .collect();

        Ok(Self { levels })
    }

    fn matches(&self, topic: &str) -> Option<TopicParams> {
        if topic.is_empty() || topic.contains('+') || topic.contains('#') {
            return None;
        }

        let topic_levels: Vec<&str> = topic.split('/').collect();
        let mut params = TopicParams::default();
        let mut topic_index = 0;

        for filter_level in &self.levels {
            match filter_level {
                TopicFilterLevel::Exact(expected) => {
                    let actual = topic_levels.get(topic_index)?;

                    if expected != actual {
                        return None;
                    }

                    topic_index += 1;
                }
                TopicFilterLevel::SingleWildCard => {
                    let actual = topic_levels.get(topic_index)?;
                    params.push_single(actual);
                    topic_index += 1;
                }
                TopicFilterLevel::MultiWildCard => {
                    params.set_multi(topic_levels[topic_index..].iter().copied());
                    return Some(params);
                }
            }
        }

        if topic_index == topic_levels.len() {
            Some(params)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum TopicFilterLevel {
    Exact(String),
    SingleWildCard,
    MultiWildCard,
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use pipe_bolt_domain::{
        BackpressurePolicy, BrokerId, DeviceIdExtraction, MqttQos, PayloadCodecKind, ProjectId,
        RouteId, TopicFilter, TopicRouteConfig,
    };
    use rumqttc::QoS;

    use super::*;
    use crate::message::envelope::MqttMessage;

    fn route(id: &str, filter: &str, enabled: bool) -> TopicRouteConfig {
        TopicRouteConfig {
            id: RouteId::new(id).unwrap(),
            broker_id: BrokerId::new("broker-1").unwrap(),
            name: id.to_owned(),
            topic_filter: TopicFilter::new(filter).unwrap(),
            codec: PayloadCodecKind::Json,
            schema_mapping_id: None,
            device_id: DeviceIdExtraction::None,
            event_type: "telemetry".to_owned(),
            qos: MqttQos::AtLeastOnce,
            enabled,
            backpressure: BackpressurePolicy::DropOldest,
        }
    }

    fn message(topic: &str) -> MqttMessage {
        MqttMessage::new(
            topic,
            QoS::AtLeastOnce,
            false,
            b"{}".to_vec(),
            SystemTime::now(),
        )
        .unwrap()
    }

    #[test]
    fn exact_match_works() {
        let matcher = ConfigRouteMatcher::new(
            ProjectId::new("project-1").unwrap(),
            vec![route("route-1", "devices/a/telemetry", true)],
        )
        .unwrap();

        let matched = matcher
            .match_message(&message("devices/a/telemetry"))
            .unwrap();

        assert_eq!(matched.route.id.as_str(), "route-1");
    }

    #[test]
    fn single_wildcard_match_consumes_one_level_and_continues() {
        let matcher = ConfigRouteMatcher::new(
            ProjectId::new("project-1").unwrap(),
            vec![route("route-1", "devices/+/telemetry", true)],
        )
        .unwrap();

        assert!(
            matcher
                .match_message(&message("devices/a/telemetry"))
                .is_some()
        );
        assert!(
            matcher
                .match_message(&message("devices/a/status"))
                .is_none()
        );
    }

    #[test]
    fn multi_wildcard_match_works() {
        let matcher = ConfigRouteMatcher::new(
            ProjectId::new("project-1").unwrap(),
            vec![route("route-1", "devices/+/events/#", true)],
        )
        .unwrap();

        let matched = matcher
            .match_message(&message("devices/a/events/x/y"))
            .unwrap();

        assert_eq!(matched.params.single(0), Some("a"));
        assert_eq!(matched.params.multi_as_topic().as_deref(), Some("x/y"));
    }

    #[test]
    fn disabled_route_is_ignored() {
        let matcher = ConfigRouteMatcher::new(
            ProjectId::new("project-1").unwrap(),
            vec![route("route-1", "devices/+/telemetry", false)],
        )
        .unwrap();

        assert!(
            matcher
                .match_message(&message("devices/a/telemetry"))
                .is_none()
        );
    }

    #[test]
    fn first_match_wins() {
        let matcher = ConfigRouteMatcher::new(
            ProjectId::new("project-1").unwrap(),
            vec![
                route("route-1", "devices/+/telemetry", true),
                route("route-2", "devices/a/telemetry", true),
            ],
        )
        .unwrap();

        let matched = matcher
            .match_message(&message("devices/a/telemetry"))
            .unwrap();

        assert_eq!(matched.route.id.as_str(), "route-1");
    }
}
