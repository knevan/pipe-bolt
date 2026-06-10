use std::sync::Arc;
use std::time::Duration;

use rumqttc::{AsyncClient, Event, EventLoop, Packet, QoS};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

use crate::bus::{BusConfig, InternalBus, InternalBusHandle, MqttCommand, TelemetryEvent};
use crate::config::MqttClientConfig;
use crate::error::MqttEngineError;
use crate::message::envelope::MqttMessage;
use crate::mqtt::backoff::ExponentialBackoff;
use crate::router::matcher::MqttRouter;

/// Cloneable handle intended to be stored in axum/salvo application state
#[derive(Clone)]
pub struct MqttHandle {
    client: AsyncClient,
    bus: InternalBusHandle,
}

impl MqttHandle {
    pub async fn publish(
        &self,
        topic: impl Into<String>,
        qos: QoS,
        retain: bool,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), MqttEngineError> {
        self.client
            .publish(topic.into(), qos, retain, payload.into())
            .await
            .map_err(|err| MqttEngineError::Client(err.to_string()))
    }

    pub fn try_enqueue_command(
        &self,
        topic: impl Into<String>,
        qos: QoS,
        retain: bool,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), MqttEngineError> {
        self.bus
            .try_enqueue_command(MqttCommand::publish(topic, qos, retain, payload))
    }

    pub fn subscribe_telemetry(&self) -> broadcast::Receiver<TelemetryEvent> {
        self.bus.subscribe_telemetry()
    }

    pub async fn subscribe(
        &self,
        topic: impl Into<String>,
        qos: QoS,
    ) -> Result<(), MqttEngineError> {
        self.client
            .subscribe(topic.into(), qos)
            .await
            .map_err(|err| MqttEngineError::Client(err.to_string()))
    }

    pub async fn unsubscribe(&self, topic: impl Into<String>) -> Result<(), MqttEngineError> {
        self.client
            .unsubscribe(topic.into())
            .await
            .map_err(|err| MqttEngineError::Client(err.to_string()))
    }
}

/// Owns the MQTT worker lifecycle and routes incoming publishes.
pub struct MqttEngine {
    handle: MqttHandle,
    shutdown_tx: watch::Sender<bool>,
    workers: Vec<JoinHandle<()>>,
}

impl MqttEngine {
    pub fn spawn(config: MqttClientConfig, router: MqttRouter) -> Result<Self, MqttEngineError> {
        Self::spawn_with_bus(config, router, BusConfig::default())
    }

    pub fn spawn_with_bus(
        config: MqttClientConfig,
        router: MqttRouter,
        bus_config: BusConfig,
    ) -> Result<Self, MqttEngineError> {
        config.validate()?;

        let options = config.build_option()?;
        let (client, event_loop) = AsyncClient::new(options, config.request_channel_capacity);
        let bus = InternalBus::new(bus_config);
        let (bus_handle, ingress_rx, command_rx) = bus.split();
        let handle = MqttHandle {
            client: client.clone(),
            bus: bus_handle.clone(),
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let router = Arc::new(router);

        let mqtt_worker = tokio::spawn(run_mqtt_worker(
            client.clone(),
            event_loop,
            config,
            bus_handle.clone(),
            shutdown_rx.clone(),
        ));
        let router_worker = tokio::spawn(run_router_worker(
            router,
            bus_handle,
            ingress_rx,
            shutdown_rx.clone(),
        ));
        let command_worker = tokio::spawn(run_command_worker(client, command_rx, shutdown_rx));

        Ok(Self {
            handle,
            shutdown_tx,
            workers: vec![mqtt_worker, router_worker, command_worker],
        })
    }

    pub fn handle(&self) -> MqttHandle {
        self.handle.clone()
    }

    pub async fn shutdown(self) -> Result<(), MqttEngineError> {
        // Ignore send failure because it only means the worker already stopped
        let _ = self.shutdown_tx.send(true);

        for worker in self.workers {
            worker
                .await
                .map_err(|err| MqttEngineError::WorkerJoin(err.to_string()))?;
        }

        Ok(())
    }
}

async fn run_mqtt_worker(
    client: AsyncClient,
    mut event_loop: EventLoop,
    config: MqttClientConfig,
    bus: InternalBusHandle,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut backoff =
        ExponentialBackoff::new(config.reconnect.min_delay, config.reconnect.max_delay);

    loop {
        tokio::select! {
            biased;

            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow()  {
                    break;
                }
            }

            event = event_loop.poll() => {
                match event {
                    Ok(event) => {
                        backoff.reset();
                        handle_mqtt_event(&client, &config, &bus, event).await;
                    }
                    Err(err) => {
                        let delay = backoff.next_delay();
                        eprintln!("MQTT event loop error: {}; retrying in {:?}", err, delay);

                        if wait_or_shutdown(delay, &mut shutdown_rx).await {
                            break;
                        }
                    }
                }
            }
        }
    }
}

async fn handle_mqtt_event(
    client: &AsyncClient,
    config: &MqttClientConfig,
    bus: &InternalBusHandle,
    event: Event,
) {
    match event {
        Event::Incoming(Packet::ConnAck(_)) => {
            for subscription in &config.subscriptions {
                if let Err(err) = client
                    .subscribe(&subscription.topic, subscription.qos)
                    .await
                {
                    eprintln!(
                        "MQTT subscribe error for topic: '{}': {}",
                        subscription.topic, err
                    );
                }
            }
        }
        Event::Incoming(Packet::Publish(publish)) => match MqttMessage::from_publish(publish) {
            Ok(message) => {
                if let Err(err) = bus.try_enqueue_ingress(message).await {
                    eprintln!("MQTT ingress enqueue error: {}", err);
                }
            }
            Err(err) => {
                eprintln!("MQTT message conversion error: {}", err);
            }
        },
        _ => {
            // Other packet intentionally consumed to keep the event loop healthy
        }
    }
}

async fn run_router_worker(
    router: Arc<MqttRouter>,
    bus: InternalBusHandle,
    mut ingress_rx: mpsc::Receiver<MqttMessage>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            biased;

            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow()  {
                    break;
                }
            }

            message = ingress_rx.recv() => {
                let Some(message) = message else {
                    break;
                };

                let telemetry = TelemetryEvent::from_message(&message);

                if let Err(err) = router.dispatch(message).await {
                    eprintln!("MQTT route dispatch error: {}", err);
                }

                if let Err(err) = bus.publish_telemetry(telemetry) {
                    eprintln!("MQTT telemetry publish error: {}", err);
                }
            }
        }
    }
}

async fn run_command_worker(
    client: AsyncClient,
    mut command_rx: mpsc::Receiver<MqttCommand>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            biased;

            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow()  {
                    break;
                }
            }

            command = command_rx.recv() => {
                let Some(command) = command else {
                    break;
                };

                if let Err(err) = client.publish(
                    command.topic, command.qos, command.retain, command.payload
                ).await
                {
                    eprintln!("MQTT command publish error: {}", err);
                }
            }
        }
    }
}

async fn wait_or_shutdown(delay: Duration, shutdown_rx: &mut watch::Receiver<bool>) -> bool {
    let sleep_until = Instant::now() + delay;

    tokio::select! {
        _ = sleep(sleep_until.saturating_duration_since(Instant::now())) => false,
        changed = shutdown_rx.changed() => changed.is_err() || *shutdown_rx.borrow(),
    }
}
