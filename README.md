# Pipe Bolt

Pipe Bolt is a **Broker-to-UI MQTT Backend Platform / Control Plane** that manages telemetry data streams from MQTT brokers, performs data normalization, evaluates business rules (via a rule engine), forwards events to various external targets (ETL forwarding), and provides real-time bidirectional communication to the User Interface (UI) and command execution (command gateway).

---

## Architecture & Main Data Flow

The platform is designed with a **pipeline-centric** principle, where incoming messages are processed through several stages:
1. **MQTT Ingestion & Connection**: Manages connections to one or more MQTT brokers dynamically at runtime.
2. **Topic Routing**: Routes incoming MQTT topics based on topic filter wildcard matching rules.
3. **Payload Decoding**: Decodes raw message payloads based on defined schema mapping configurations.
4. **Event Normalization**: Transforms decoded payloads into a standardized internal data format (*Normalized Event*) carrying the project context.
5. **Rule AST/IR Engine**: Safely evaluates business conditions based on AST (Abstract Syntax Tree) rule definitions without running arbitrary user scripts.
6. **Action Dispatcher**: Produces action intents (*Action Intents*) to be executed asynchronously (e.g., sending webhooks, storing data, streaming to UI, or publishing command templates back to devices).

---

## Codebase Structure

The codebase is split into several main modules and applications:

### 1. Applications & Interfaces
*   **Web Dashboard (`apps/web`)**: The frontend admin dashboard for visual management of projects, broker connections, topic routes, the visual Rule Builder, and the Command Gateway. This module integrates with the backend through an API client generated automatically from the API schema.
*   **Daemon Entrypoint (`crates/pipe-bolt-daemon`)**: The main execution binary that loads initial configurations, manages project runtime lifecycles, handles graceful shutdowns, orchestrates secure hot-reloading of configurations (using atomic pointer swapping), and runs background asynchronous workers for persistence logging (audit logs, executions, and outcomes) to keep the main telemetry ingestion pipeline unblocked.

### 2. Core Backend Modules (`crates/`)
*   **Domain Model (`crates/pipe-bolt-domain`)**: Defines core domain data types, strongly-typed identifiers, project configurations, rule AST definitions, action intents, and the `NormalizedEvent` structure. This module is extremely lightweight and has no runtime or external database dependencies.
*   **Data-Plane Runtime (`crates/pipe-bolt-core`)**: Implements core data-plane features including broker connection runtimes, bounded message buses with explicit backpressure policies, topic routing matchers, payload codecs, rule evaluation engines, action dispatchers, and realtime telemetry streams (WebSocket/SSE) for UI bridging.
*   **Persistence Layer (`crates/pipe-bolt-storage`)**: Manages read/write operations to the relational database. It is responsible for Optimistic Concurrency Control (OCC) on project configuration updates, transactional audit logging, operation outcome storage, and encryption of broker credentials at rest via an encryption keyring.
*   **API Layer (`crates/pipe-bolt-api`)**: An HTTP REST API gateway that handles user authentication and authorization (using JWT tokens or basic authentication), input data validation (DTOs), configuration CRUD handlers, realtime UI stream endpoints, dynamic configuration reload triggers, and automated OpenAPI documentation generation.

---

## Key Features & Capabilities

*   **Multi-Project Isolation**: Every configuration, event, data stream, and command carries a project-specific identifier to guarantee tenant isolation.
*   **Dynamic Configuration Hot-Reload**: Broker connection parameters, topic routes, schema mappings, and rule definitions can be updated dynamically via API. Draft configurations are validated before being atomically swapped in memory without stopping or recompiling the application.
*   **Rule Engine (Trigger -> Condition -> Action)**: Evaluates telemetry data fields using type-safe, declarative AST rules, producing deterministic action intents that can be isolated and unit-tested.
*   **ETL Sink & Forwarding**: Forwarding services (such as HTTP webhooks) with bounded concurrency, execution timeouts, and automatic retry policies with backoff.
*   **Command Gateway**: Publishing templates to control devices via MQTT, with execution IDs for command correlation, response tracking, and audit logging.
*   **Realtime Streaming**: Real-time telemetry streaming to UI dashboard clients via WebSocket/SSE protocols scoped per project.
*   **Credential Security**: Encryption of MQTT broker credentials at rest in the database and automatic secrets redaction in application logs and API responses.
*   **Observability**: Structured tracing with correlation IDs tracked across ingestion, routing, rule evaluation, and dispatch phases, alongside dedicated health and readiness check endpoints (`/healthz` and `/readyz`).
