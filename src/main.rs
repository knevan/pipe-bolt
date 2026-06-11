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
use tokio::sync::watch;

use crate::message::envelope::MqttMessage;
use crate::mqtt::engine::MqttEngine;
use crate::router::matcher::{MqttRouter, TopicParams};
use crate::web::realtime::router::{default_bind_addr, serve_realtime_bridge_with_state};
use crate::web::realtime::state::RealtimeBridgeState;

pub mod bus;
pub mod codec;
pub mod command;
pub mod config;
pub mod error;
pub mod message;
mod mqtt;
pub mod router;
pub mod web;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let router = MqttRouter::new()
        .route(
            "devices/+/telemetry",
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
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let realtime_state = RealtimeBridgeState::new(mqtt.clone())
        .with_websocket_client_buffer(64)
        .with_websocket_ping_interval(Duration::from_secs(30))
        .with_sse_keep_alive_interval(Duration::from_secs(15));
    let realtime_bind_addr = default_bind_addr();

    let realtime_worker = tokio::spawn(async move {
        serve_realtime_bridge_with_state(realtime_bind_addr, realtime_state, shutdown_rx).await
    });

    mqtt.publish(
        "devices/local/status",
        QoS::AtLeastOnce,
        false,
        br#"{"status":"online"}"#.to_vec(),
    )
    .await?;

    tokio::signal::ctrl_c().await?;

    // Ignore send failure because it only means the bridge already stopped.
    let _ = shutdown_tx.send(true);

    match realtime_worker.await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => eprintln!("Realtime bridge stopped with error: {err}"),
        Err(err) => eprintln!("Realtime bridge task join error: {err}"),
    }

    engine.shutdown().await?;

    Ok(())
}
