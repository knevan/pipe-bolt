use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use crate::codec::{JsonPayload, PayloadCodec, RawPayload};
use crate::error::MqttEngineError;
use crate::message::envelope::MqttMessage;

pub type MqttRouteFuture = Pin<Box<dyn Future<Output = Result<(), MqttEngineError>> + Send>>;

/// Async route handler abstraction without requiring async-trait
pub trait MqttRouteHandler: Send + Sync + 'static {
    fn codec(&self) -> PayloadCodec;
    fn call(&self, message: MqttMessage, params: TopicParams) -> MqttRouteFuture;
}

pub struct RawRouteHandler<F> {
    handler: F,
}

impl<F> RawRouteHandler<F> {
    fn new(handler: F) -> Self {
        Self { handler }
    }
}

impl<F, Fut> MqttRouteHandler for F
where
    F: Fn(MqttMessage, TopicParams) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), MqttEngineError>> + Send + 'static,
{
    fn codec(&self) -> PayloadCodec {
        PayloadCodec::Raw
    }
    fn call(&self, message: MqttMessage, params: TopicParams) -> MqttRouteFuture {
        Box::pin(self(message, params))
    }
}

impl<F, Fut> MqttRouteHandler for RawRouteHandler<F>
where
    F: Fn(MqttMessage, RawPayload, TopicParams) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), MqttEngineError>> + Send + 'static,
{
    fn codec(&self) -> PayloadCodec {
        PayloadCodec::Raw
    }

    fn call(&self, message: MqttMessage, params: TopicParams) -> MqttRouteFuture {
        let payload = RawPayload::new(PayloadCodec::decode_raw(message.payload()));
        Box::pin((self.handler)(message, payload, params))
    }
}

pub struct JsonRouteHandler<F, T> {
    handler: F,
    _payload: PhantomData<fn() -> T>,
}

impl<F, T> JsonRouteHandler<F, T> {
    fn new(handler: F) -> Self {
        Self {
            handler,
            _payload: PhantomData,
        }
    }
}

impl<F, Fut, T> MqttRouteHandler for JsonRouteHandler<F, T>
where
    F: Fn(MqttMessage, JsonPayload<T>, TopicParams) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), MqttEngineError>> + Send + 'static,
    T: DeserializeOwned + Send + 'static,
{
    fn codec(&self) -> PayloadCodec {
        PayloadCodec::Json
    }

    fn call(&self, message: MqttMessage, params: TopicParams) -> MqttRouteFuture {
        match PayloadCodec::decode_json::<T>(message.payload()) {
            Ok(decode) => Box::pin((self.handler)(message, JsonPayload::new(decode), params)),
            Err(err) => Box::pin(async move { Err(err) }),
        }
    }
}

/// Captured MQTT wildcard values from a matched topic filter.
///
/// `single_level` preserves the order of `+` wildcards. `multi_level` contains the remaining
/// levels captured by a trailing `#` wildcard, if the route uses one.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct TopicParams {
    single_level: Vec<String>,
    multi_level: Vec<String>,
}

impl TopicParams {
    pub fn single(&self, index: usize) -> Option<&str> {
        self.single_level.get(index).map(String::as_str)
    }

    pub fn singles(&self) -> &[String] {
        &self.single_level
    }

    pub fn multi(&self) -> &[String] {
        &self.multi_level
    }

    pub fn multi_as_topic(&self) -> Option<String> {
        if self.multi_level.is_empty() {
            None
        } else {
            Some(self.multi_level.join("/"))
        }
    }

    pub(crate) fn push_single(&mut self, value: &str) {
        self.single_level.push(value.to_owned());
    }

    pub(crate) fn set_multi<I>(&mut self, values: I)
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        self.multi_level = values.into_iter().map(Into::into).collect();
    }
}

#[derive(Clone)]
struct MqttRoute {
    filter: TopicFilter,
    handler: Arc<dyn MqttRouteHandler>,
}

#[derive(Clone, Default)]
pub struct MqttRouter {
    routes: Arc<Vec<MqttRoute>>,
}

impl MqttRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn raw_router<F, Fut>(
        mut self,
        filter: impl Into<String>,
        handler: F,
    ) -> Result<Self, MqttEngineError>
    where
        F: Fn(MqttMessage, RawPayload, TopicParams) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), MqttEngineError>> + Send + 'static,
    {
        self.push_route(filter.into(), RawRouteHandler::new(handler))?;
        Ok(self)
    }

    pub fn json_route<T, F, Fut>(
        mut self,
        filter: impl Into<String>,
        handler: F,
    ) -> Result<Self, MqttEngineError>
    where
        F: Fn(MqttMessage, JsonPayload<T>, TopicParams) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), MqttEngineError>> + Send + 'static,
        T: DeserializeOwned + Send + 'static,
    {
        self.push_route(filter.into(), JsonRouteHandler::new(handler))?;
        Ok(self)
    }

    pub fn route<H>(
        mut self,
        filter: impl Into<String>,
        handler: H,
    ) -> Result<Self, MqttEngineError>
    where
        H: MqttRouteHandler,
    {
        let filter = TopicFilter::parse(filter.into())?;
        let mut routes = Arc::unwrap_or_clone(self.routes);

        routes.push(MqttRoute {
            filter,
            handler: Arc::new(handler),
        });

        self.routes = Arc::new(routes);
        Ok(self)
    }

    /// Dispatches a message to the first matching route.
    ///
    /// Returns `Ok(true)` when a route handled the message and `Ok(false)` when no route matched.
    /// Handler errors are returned immediately and stop dispatch.
    pub async fn dispatch(&self, message: MqttMessage) -> Result<bool, MqttEngineError> {
        for route in self.routes.iter() {
            if let Some(params) = route.filter.matches(message.topic()) {
                route.handler.call(message, params).await?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.routes.len()
    }

    fn push_route<H>(&mut self, filter: String, handler: H) -> Result<(), MqttEngineError>
    where
        H: MqttRouteHandler,
    {
        let filter = TopicFilter::parse(filter)?;
        let mut routes = Arc::unwrap_or_clone(std::mem::take(&mut self.routes));

        routes.push(MqttRoute {
            filter,
            handler: Arc::new(handler),
        });

        self.routes = Arc::new(routes);
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct TopicFilter {
    raw: String,
    levels: Vec<TopicFilterLevel>,
}

impl TopicFilter {
    fn parse(raw: String) -> Result<Self, MqttEngineError> {
        validate_mqtt_topic_filter(&raw)?;

        let levels = raw
            .split('/')
            .map(|level| match level {
                "+" => TopicFilterLevel::SingleWildcard,
                "#" => TopicFilterLevel::MultiWildcard,
                value => TopicFilterLevel::Exact(value.to_owned()),
            })
            .collect();

        Ok(Self { raw, levels })
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
                TopicFilterLevel::SingleWildcard => {
                    let actual = topic_levels.get(topic_index)?;
                    params.push_single(actual);
                    topic_index += 1;
                }
                TopicFilterLevel::MultiWildcard => {
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
    SingleWildcard,
    MultiWildcard,
}

pub fn validate_mqtt_topic_filter(filter: &str) -> Result<(), MqttEngineError> {
    if filter.is_empty() {
        return Err(MqttEngineError::InvalidTopicFilter(
            "topic filter must not be empty".to_owned(),
        ));
    }

    let level_count = filter.split('/').count();

    for (index, level) in filter.split('/').enumerate() {
        if level.contains('#') {
            if level != "#" {
                return Err(MqttEngineError::InvalidTopicFilter(format!(
                    "multi-level wildcard must occupy an entire level in'{}'",
                    filter
                )));
            }

            if index != level_count - 1 {
                return Err(MqttEngineError::InvalidTopicFilter(format!(
                    "multi-level wildcard must be the last level in '{}'",
                    filter
                )));
            }
        }

        if level.contains('+') && level != "+" {
            return Err(MqttEngineError::InvalidTopicFilter(format!(
                "single-level wildcard must occupy and entire level in '{}'",
                filter
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_single_level_wildcard() {
        let filter = TopicFilter::parse("devices/+/telemetry".to_owned()).unwrap();
        let params = filter.matches("devices/device-1/telemetry").unwrap();

        assert_eq!(params.single(0), Some("device-1"));
        assert_eq!(params.multi(), &[] as &[String]);
    }

    #[test]
    fn matches_multi_level_wildcard() {
        let filter = TopicFilter::parse("devices/+/event/#".to_owned()).unwrap();
        let params = filter.matches("devices/device-1/event/a/b/c").unwrap();

        assert_eq!(params.single(0), Some("device-1"));
        assert_eq!(params.multi_as_topic().as_deref(), Some("a/b/c"));
    }

    #[test]
    fn rejects_invalid_wildcard_filters() {
        assert!(TopicFilter::parse("devices/#/event".to_owned()).is_err());
        assert!(TopicFilter::parse("devices/+status".to_owned()).is_err());
        assert!(TopicFilter::parse("devices/status#".to_owned()).is_err());
    }
}
