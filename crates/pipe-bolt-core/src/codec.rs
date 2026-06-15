use serde::de::DeserializeOwned;
use serde_json::from_slice;

use crate::error::MqttEngineError;

/// Route-selected payload codec.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PayloadCodec {
    Json,
    Raw,
}

impl PayloadCodec {
    pub fn decode_json<T>(payload: &[u8]) -> Result<T, MqttEngineError>
    where
        T: DeserializeOwned,
    {
        from_slice(payload).map_err(|err| MqttEngineError::Decode(err.to_string()))
    }

    pub fn decode_raw(payload: &[u8]) -> Vec<u8> {
        payload.to_vec()
    }
}

/// Typed message delivered to JSON route handlers.
#[derive(Debug, Clone)]
pub struct JsonPayload<T> {
    inner: T,
}

impl<T> JsonPayload<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn get(&self) -> &T {
        &self.inner
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

/// Raw message delivered to routes that intentionally avoid decoding.
#[derive(Debug, Clone)]
pub struct RawPayload {
    bytes: Vec<u8>,
}

impl RawPayload {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}
