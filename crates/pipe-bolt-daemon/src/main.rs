#![deny(
    clippy::panic,
    clippy::panicking_unwrap,
    clippy::redundant_clone,
    clippy::implicit_clone,
    clippy::perf,
    clippy::large_types_passed_by_value,
    clippy::large_futures,
    clippy::trivially_copy_pass_by_ref,
    clippy::clone_on_ref_ptr,
    // clippy::unwrap_used,
    // clippy::missing_const_for_fn,
)]

use std::error::Error;
use std::time::Duration;

use pipe_bolt_core::config::MqttClientConfig;
use pipe_bolt_core::error::MqttEngineError;
use pipe_bolt_core::message::envelope::MqttMessage;
use pipe_bolt_core::mqtt::engine::MqttEngine;
use pipe_bolt_core::router::matcher::{MqttRouter, TopicParams};
use pipe_bolt_core::web::realtime::router::{default_bind_addr, serve_realtime_bridge_with_state};
use pipe_bolt_core::web::realtime::state::RealtimeBridgeState;
use rumqttc::QoS;
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let router = MqttRouter::new()
        .route(
            "devices/+/telemetry",
            |message: MqttMessage, params: TopicParams| async move {
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
    let _ = shutdown_tx.send(true);
    match realtime_worker.await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => eprintln!("Realtime bridge stopped with error: {err}"),
        Err(err) => eprintln!("Realtime bridge task join error: {err}"),
    }
    engine.shutdown().await?;
    Ok(())
}
