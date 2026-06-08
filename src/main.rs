#![deny(
    clippy::panic,
    // clippy::unwrap_used,
    clippy::panicking_unwrap,
    clippy::redundant_clone,
    clippy::implicit_clone,
    clippy::perf
)]

use std::error::Error;
use std::time::Duration;

use config::MqttClientConfig;
use error::MqttEngineError;
use rumqttc::QoS;

use crate::message::envelope::MqttMessage;
use crate::mqtt::engine::MqttEngine;
use crate::router::matcher::{MqttRouter, TopicParams};

pub mod bus;
pub mod codec;
pub mod config;
pub mod error;
pub mod message;
mod mqtt;
pub mod router;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let router = MqttRouter::new()
        .route(
            "device/+/telemetry",
            |message: MqttMessage, params: TopicParams| async move {
                // Payload parsing is done after routing.
                let device_id = params.single(0).unwrap_or("unknown");
                eprintln!(
                    "telemetry received from device '{}' with {} bytes",
                    device_id,
                    message.payload().len()
                );
                Ok::<(), MqttEngineError>(())
            },
        )?
        .route(
            "devices/+/status",
            |message: MqttMessage, params: TopicParams| async move {
                // Status handling can parse bytes based on the route domain contract.
                let device_id = params.single(0).unwrap_or("unknown");
                eprintln!(
                    "status received from device '{}' on topic '{}'",
                    device_id,
                    message.topic()
                );
                Ok::<(), MqttEngineError>(())
            },
        )?
        .route(
            "devices/+/event/#",
            |message: MqttMessage, params: TopicParams| async move {
                let device_id = params.single(0).unwrap_or("unknown");
                let event_path = params.multi_as_topic().unwrap_or_default();
                eprintln!(
                    "event received from device '{}' at path '{}' with {} bytes",
                    device_id,
                    event_path,
                    message.payload().len()
                );
                Ok::<(), MqttEngineError>(())
            },
        )?;

    let config = MqttClientConfig::new("pipe-bolt-local", "localhost", 1883)
        .with_keep_alive(Duration::from_secs(30))
        .with_clean_session(false)
        .with_subscription("devices/+/telemetry", QoS::AtLeastOnce)
        .with_subscription("devices/+/status", QoS::AtLeastOnce)
        .with_subscription("devices/+/event/#", QoS::AtLeastOnce);

    let engine = MqttEngine::spawn(config, router)?;
    let mqtt = engine.handle();

    mqtt.publish(
        "devices/local/status",
        QoS::AtLeastOnce,
        false,
        br#"{"status":"online"}"#.to_vec(),
    )
    .await?;

    tokio::signal::ctrl_c().await?;
    engine.shutdown().await?;

    Ok(())
}
