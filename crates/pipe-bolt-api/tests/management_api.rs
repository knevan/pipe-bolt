use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use pipe_bolt_api::dto::{
    CommandExecutionStatusResponse, ExecuteCommandResponse, ForwarderCountersResponse,
    ProjectConfigDocumentV1, ReadinessStatus, RuntimeCountersResponse, RuntimeLifecycleState,
    RuntimePipelineCountersResponse, RuntimeReadinessResponse, RuntimeReloadResponse,
    RuntimeStatusResponse,
};
use pipe_bolt_api::{
    ApiState, ManagementAuth, ManagementProjectScope, ManagementRole, ManagementStorage,
    RuntimeControl, RuntimeControlError, management_router,
};
use pipe_bolt_domain::{
    BackpressurePolicy, BrokerConnectionConfig, BrokerId, CommandExecutionId, CommandTemplateId,
    DeviceIdExtraction, MqttCredentials, MqttQos, NormalizedEvent, PayloadCodecKind, ProjectConfig,
    ProjectId, ReconnectPolicy, SecretString, TlsMode, TopicFilter, TopicRouteConfig, UserId,
};
use pipe_bolt_storage::error::StorageError;
use pipe_bolt_storage::model::{
    AuditContext, AuditEventRecord, FailureEventRecord, FailureListQuery, OperationalListQuery,
    ProjectConfigWriteResult, SinkDeliveryOutcomeRecord,
};
use salvo::async_trait;
use salvo::http::StatusCode;
use salvo::prelude::*;
use salvo::test::{ResponseExt, TestClient};
use serde_json::{Value, json};
use time::OffsetDateTime;
use tokio::sync::{Mutex, broadcast};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const TEST_TOKEN: &str = "test-management-token-0123456789abcdef";
const TEST_PROJECT_ID: &str = "project-test";

#[tokio::test]
async fn management_health_is_public() -> TestResult {
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        TestRuntime::default(),
    )?;

    let response = TestClient::get("http://127.0.0.1:8080/health")
        .send(&service)
        .await;

    assert_eq!(response.status_code, Some(StatusCode::OK));
    Ok(())
}

#[tokio::test]
async fn management_config_requires_bearer_token() -> TestResult {
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        TestRuntime::default(),
    )?;

    let response = TestClient::get("http://127.0.0.1:8080/projects/project-test/config")
        .send(&service)
        .await;

    assert_eq!(response.status_code, Some(StatusCode::UNAUTHORIZED));
    Ok(())
}

#[tokio::test]
async fn management_config_rejects_invalid_token() -> TestResult {
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        TestRuntime::default(),
    )?;

    let response = TestClient::get("http://127.0.0.1:8080/projects/project-test/config")
        .bearer_auth("wrong-token")
        .send(&service)
        .await;

    assert_eq!(response.status_code, Some(StatusCode::UNAUTHORIZED));
    Ok(())
}

#[tokio::test]
async fn management_readyz_is_public_and_reports_ready() -> TestResult {
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        TestRuntime::default(),
    )?;

    let response = TestClient::get("http://127.0.0.1:8080/readyz")
        .send(&service)
        .await;

    assert_eq!(response.status_code, Some(StatusCode::OK));
    Ok(())
}

#[tokio::test]
async fn realtime_sse_requires_bearer_token() -> TestResult {
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        TestRuntime::default(),
    )?;

    let response = TestClient::get("http://127.0.0.1:8080/projects/project-test/realtime/sse")
        .send(&service)
        .await;

    assert_eq!(response.status_code, Some(StatusCode::UNAUTHORIZED));
    Ok(())
}

#[tokio::test]
async fn management_write_rejects_viewer_role() -> TestResult {
    let service = test_service_with_auth(
        TestStorage::with_config(runtime_supported_config(1)?),
        TestRuntime::default(),
        viewer_auth()?,
    )?;
    let body = update_body(1, runtime_supported_config(1)?, Some("viewer update"))?;

    let response = TestClient::put("http://127.0.0.1:8080/projects/project-test/config")
        .bearer_auth(TEST_TOKEN)
        .json(&body)
        .send(&service)
        .await;

    assert_eq!(response.status_code, Some(StatusCode::FORBIDDEN));
    Ok(())
}

#[tokio::test]
async fn management_read_rejects_disallowed_project_scope() -> TestResult {
    let auth = ManagementAuth::bearer_with_context(
        TEST_TOKEN,
        UserId::new("user:scoped")?,
        ManagementRole::Admin,
        ManagementProjectScope::projects(vec![ProjectId::new("project-allowed")?]),
    )?;
    let service = test_service_with_auth(
        TestStorage::with_config(runtime_supported_config(1)?),
        TestRuntime::default(),
        auth,
    )?;

    let response = TestClient::get("http://127.0.0.1:8080/projects/project-test/config")
        .bearer_auth(TEST_TOKEN)
        .send(&service)
        .await;

    assert_eq!(response.status_code, Some(StatusCode::FORBIDDEN));
    Ok(())
}

#[tokio::test]
async fn management_auth_inserts_token_bound_actor() -> TestResult {
    let storage = TestStorage::with_config(runtime_supported_config(1)?);
    let service = test_service(storage.clone(), TestRuntime::default())?;
    let mut proposed = runtime_supported_config(1)?;
    proposed.name = "Updated Project".to_owned();
    let body = update_body(1, proposed, Some("operator update"))?;

    let _response = TestClient::put("http://127.0.0.1:8080/projects/project-test/config")
        .bearer_auth(TEST_TOKEN)
        .add_header("x-pipe-bolt-actor-id", "attacker", true)
        .json(&body)
        .send(&service)
        .await;

    assert_eq!(
        storage.last_actor_id().await.as_deref(),
        Some("system:bootstrap-token")
    );
    Ok(())
}

#[tokio::test]
async fn config_put_rejects_stale_expected_version() -> TestResult {
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(2)?),
        TestRuntime::default(),
    )?;
    let body = update_body(1, runtime_supported_config(2)?, Some("stale update"))?;

    let response = TestClient::put("http://127.0.0.1:8080/projects/project-test/config")
        .bearer_auth(TEST_TOKEN)
        .json(&body)
        .send(&service)
        .await;

    assert_eq!(response.status_code, Some(StatusCode::CONFLICT));
    Ok(())
}

#[tokio::test]
async fn config_put_rejects_runtime_unsupported_config_before_storage_write() -> TestResult {
    let storage = TestStorage::with_config(runtime_supported_config(1)?);
    let runtime = TestRuntime::default()
        .reject_candidate_config("runtime unsupported config")
        .await;
    let service = test_service(storage.clone(), runtime)?;
    let body = update_body(1, runtime_supported_config(1)?, Some("runtime invalid"))?;

    let _response = TestClient::put("http://127.0.0.1:8080/projects/project-test/config")
        .bearer_auth(TEST_TOKEN)
        .json(&body)
        .send(&service)
        .await;

    assert_eq!(storage.update_calls().await, 0);
    Ok(())
}

#[tokio::test]
async fn config_put_preserves_existing_secret_on_redacted_placeholder() -> TestResult {
    let storage = TestStorage::with_config(runtime_supported_config(1)?);
    let service = test_service(storage.clone(), TestRuntime::default())?;
    let mut proposed = runtime_supported_config(1)?;
    proposed.name = "Secret Preserve".to_owned();
    let body = update_body(1, proposed, Some("round trip update"))?;

    let _response = TestClient::put("http://127.0.0.1:8080/projects/project-test/config")
        .bearer_auth(TEST_TOKEN)
        .json(&body)
        .send(&service)
        .await;

    assert_eq!(
        storage.broker_password("broker-main").await.as_deref(),
        Some("old-password")
    );
    Ok(())
}

#[tokio::test]
async fn config_put_rejects_redacted_secret_for_new_secret() -> TestResult {
    let storage = TestStorage::with_config(runtime_supported_config(1)?);
    let service = test_service(storage, TestRuntime::default())?;
    let mut proposed = runtime_supported_config(1)?;
    proposed
        .brokers
        .push(test_broker("broker-extra", false, "new-password")?);
    let mut body = update_body(1, proposed, Some("new redacted secret"))?;
    body["config"]["brokers"][1]["credentials"]["password"] = json!("<redacted>");

    let response = TestClient::put("http://127.0.0.1:8080/projects/project-test/config")
        .bearer_auth(TEST_TOKEN)
        .json(&body)
        .send(&service)
        .await;

    assert_eq!(response.status_code, Some(StatusCode::BAD_REQUEST));
    Ok(())
}

#[tokio::test]
async fn failure_resolve_is_project_scoped() -> TestResult {
    let storage = TestStorage::with_config(runtime_supported_config(1)?);
    storage
        .insert_failure("project-other", "failure-1", false)
        .await;
    let service = test_service(storage, TestRuntime::default())?;

    let response =
        TestClient::post("http://127.0.0.1:8080/projects/project-test/failures/failure-1/resolve")
            .bearer_auth(TEST_TOKEN)
            .json(&json!({ "resolution": "ignore", "reason": "scope test" }))
            .send(&service)
            .await;

    assert_eq!(response.status_code, Some(StatusCode::NOT_FOUND));
    Ok(())
}

#[tokio::test]
async fn failure_resolve_returns_404_for_missing_failure() -> TestResult {
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        TestRuntime::default(),
    )?;

    let response =
        TestClient::post("http://127.0.0.1:8080/projects/project-test/failures/missing/resolve")
            .bearer_auth(TEST_TOKEN)
            .json(&json!({ "resolution": "not found" }))
            .send(&service)
            .await;

    assert_eq!(response.status_code, Some(StatusCode::NOT_FOUND));
    Ok(())
}

#[tokio::test]
async fn failure_resolve_returns_409_for_already_resolved_failure() -> TestResult {
    let storage = TestStorage::with_config(runtime_supported_config(1)?);
    storage
        .insert_failure(TEST_PROJECT_ID, "failure-1", true)
        .await;
    let service = test_service(storage, TestRuntime::default())?;

    let response =
        TestClient::post("http://127.0.0.1:8080/projects/project-test/failures/failure-1/resolve")
            .bearer_auth(TEST_TOKEN)
            .json(&json!({ "resolution": "already done" }))
            .send(&service)
            .await;

    assert_eq!(response.status_code, Some(StatusCode::CONFLICT));
    Ok(())
}

#[tokio::test]
async fn runtime_reload_rejects_concurrent_reload() -> TestResult {
    let runtime = TestRuntime::default()
        .with_reload_mode(ReloadMode::ReloadInProgress)
        .await;
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        runtime,
    )?;

    let response = TestClient::post("http://127.0.0.1:8080/projects/project-test/runtime/reload")
        .bearer_auth(TEST_TOKEN)
        .send(&service)
        .await;

    assert_eq!(response.status_code, Some(StatusCode::CONFLICT));
    Ok(())
}

#[tokio::test]
async fn runtime_reload_does_not_start_after_shutdown_begins() -> TestResult {
    let runtime = TestRuntime::default()
        .with_reload_mode(ReloadMode::ShuttingDown)
        .await;
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        runtime,
    )?;

    let response = TestClient::post("http://127.0.0.1:8080/projects/project-test/runtime/reload")
        .bearer_auth(TEST_TOKEN)
        .send(&service)
        .await;

    assert_eq!(response.status_code, Some(StatusCode::SERVICE_UNAVAILABLE));
    Ok(())
}

#[tokio::test]
async fn runtime_reload_blocks_when_old_runtime_shutdown_is_uncertain() -> TestResult {
    let runtime = TestRuntime::default()
        .with_reload_mode(ReloadMode::UnsafeOldRuntimeShutdown)
        .await;
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        runtime,
    )?;

    let response = TestClient::post("http://127.0.0.1:8080/projects/project-test/runtime/reload")
        .bearer_auth(TEST_TOKEN)
        .send(&service)
        .await;

    assert_eq!(response.status_code, Some(StatusCode::SERVICE_UNAVAILABLE));
    Ok(())
}

#[tokio::test]
async fn runtime_reload_audits_success_and_failure() -> TestResult {
    let success_runtime = TestRuntime::default();
    let success_service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        success_runtime.clone(),
    )?;
    let failure_runtime = TestRuntime::default()
        .with_reload_mode(ReloadMode::StartFailed)
        .await;
    let failure_service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        failure_runtime.clone(),
    )?;

    let _success = TestClient::post("http://127.0.0.1:8080/projects/project-test/runtime/reload")
        .bearer_auth(TEST_TOKEN)
        .json(&json!({ "reason": "success reload" }))
        .send(&success_service)
        .await;
    let _failure = TestClient::post("http://127.0.0.1:8080/projects/project-test/runtime/reload")
        .bearer_auth(TEST_TOKEN)
        .json(&json!({ "reason": "failed reload" }))
        .send(&failure_service)
        .await;

    assert_eq!(
        (
            success_runtime.reload_audit_reasons().await,
            failure_runtime.reload_audit_reasons().await,
        ),
        (
            vec![Some("success reload".to_owned())],
            vec![Some("failed reload".to_owned())],
        ),
    );
    Ok(())
}

#[tokio::test]
async fn command_execute_requires_operator_permission() -> TestResult {
    let service = test_service_with_auth(
        TestStorage::with_config(runtime_supported_config(1)?),
        TestRuntime::default(),
        viewer_auth()?,
    )?;

    let response = TestClient::post(
        "http://127.0.0.1:8080/projects/project-test/commands/command-main/execute",
    )
    .bearer_auth(TEST_TOKEN)
    .json(&json!({ "params": { "device_id": "device-1" } }))
    .send(&service)
    .await;

    assert_eq!(response.status_code, Some(StatusCode::FORBIDDEN));
    Ok(())
}

#[tokio::test]
async fn command_execute_returns_accepted_when_runtime_queues_command() -> TestResult {
    let runtime = TestRuntime::default();
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        runtime.clone(),
    )?;

    let mut response = TestClient::post(
        "http://127.0.0.1:8080/projects/project-test/commands/command-main/execute",
    )
    .bearer_auth(TEST_TOKEN)
    .json(&json!({
        "params": { "device_id": "device-1", "state": true },
        "reason": "test command"
    }))
    .send(&service)
    .await;
    let body = response.take_json::<Value>().await?;

    assert_eq!(response.status_code, Some(StatusCode::ACCEPTED));
    assert_eq!(
        body.pointer("/status").and_then(Value::as_str),
        Some("queued")
    );
    assert_eq!(
        runtime.execute_audit_reasons().await,
        vec![Some("test command".to_owned())]
    );
    Ok(())
}

#[tokio::test]
async fn list_endpoints_enforce_limit_and_cursor() -> TestResult {
    let storage = TestStorage::with_config(runtime_supported_config(1)?);
    let service = test_service(storage.clone(), TestRuntime::default())?;

    let _response = TestClient::get(
        "http://127.0.0.1:8080/projects/project-test/audit-events?limit=999&before=2026-01-02T03:04:05Z",
    )
        .bearer_auth(TEST_TOKEN)
        .send(&service)
        .await;

    assert_eq!(storage.last_operational_limit().await, Some(500));
    Ok(())
}

#[cfg(feature = "salvo-oapi")]
#[tokio::test]
async fn openapi_spec_contains_bearer_scheme() -> TestResult {
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        TestRuntime::default(),
    )?;

    let mut response = TestClient::get("http://127.0.0.1:8080/api-doc/openapi.json")
        .send(&service)
        .await;
    let body = response.take_json::<Value>().await?;

    assert_eq!(
        body.pointer("/components/securitySchemes/bearer_auth/scheme")
            .and_then(Value::as_str),
        Some("bearer"),
    );
    Ok(())
}

#[cfg(feature = "salvo-oapi")]
#[tokio::test]
async fn openapi_spec_contains_phase9_endpoints() -> TestResult {
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        TestRuntime::default(),
    )?;

    let mut response = TestClient::get("http://127.0.0.1:8080/api-doc/openapi.json")
        .send(&service)
        .await;
    let body = response.take_json::<Value>().await?;

    assert!(
        body.pointer("/paths/~1readyz/get")
            .is_some_and(Value::is_object)
    );
    Ok(())
}

#[cfg(feature = "salvo-oapi")]
#[tokio::test]
async fn openapi_spec_contains_unified_realtime_sse_endpoint() -> TestResult {
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        TestRuntime::default(),
    )?;

    let mut response = TestClient::get("http://127.0.0.1:8080/api-doc/openapi.json")
        .send(&service)
        .await;
    let body = response.take_json::<Value>().await?;

    assert!(
        body.pointer("/paths/~1projects~1{project_id}~1realtime~1sse/get")
            .is_some_and(Value::is_object)
    );
    Ok(())
}

#[cfg(feature = "salvo-oapi")]
#[tokio::test]
async fn openapi_spec_should_use_unique_health_operation_ids() -> TestResult {
    let body = openapi_json().await?;

    assert_eq!(
        body.pointer("/paths/~1healthz/get/operationId")
            .and_then(Value::as_str),
        Some("get_healthz"),
    );
    Ok(())
}

#[cfg(feature = "salvo-oapi")]
#[tokio::test]
async fn openapi_spec_should_document_project_path_parameter() -> TestResult {
    let body = openapi_json().await?;

    assert!(
        body.pointer("/paths/~1projects~1{project_id}~1config/get/parameters")
            .and_then(Value::as_array)
            .is_some_and(|parameters| parameters.iter().any(|parameter| {
                parameter.get("name").and_then(Value::as_str) == Some("project_id")
                    && parameter.get("in").and_then(Value::as_str) == Some("path")
            }))
    );
    Ok(())
}

#[cfg(feature = "salvo-oapi")]
#[tokio::test]
async fn openapi_spec_should_document_config_write_body() -> TestResult {
    let body = openapi_json().await?;

    assert!(body
        .pointer("/paths/~1projects~1{project_id}~1config/put/requestBody/content/application~1json/schema")
        .is_some());
    Ok(())
}

#[cfg(feature = "salvo-oapi")]
#[tokio::test]
async fn openapi_spec_should_document_command_execute_body() -> TestResult {
    let body = openapi_json().await?;

    assert!(body
        .pointer("/paths/~1projects~1{project_id}~1commands~1{command_template_id}~1execute/post/requestBody/content/application~1json/schema")
        .is_some());
    Ok(())
}

#[cfg(feature = "salvo-oapi")]
#[tokio::test]
async fn openapi_spec_should_document_audit_list_response_schema() -> TestResult {
    let body = openapi_json().await?;

    assert!(body
        .pointer("/paths/~1projects~1{project_id}~1audit-events/get/responses/200/content/application~1json/schema")
        .is_some());
    Ok(())
}

#[cfg(feature = "salvo-oapi")]
async fn openapi_json() -> TestResult<Value> {
    let service = test_service(
        TestStorage::with_config(runtime_supported_config(1)?),
        TestRuntime::default(),
    )?;

    let mut response = TestClient::get("http://127.0.0.1:8080/api-doc/openapi.json")
        .send(&service)
        .await;

    Ok(response.take_json::<Value>().await?)
}

fn test_service(storage: TestStorage, runtime: TestRuntime) -> TestResult<Service> {
    test_service_with_auth(storage, runtime, ManagementAuth::bearer(TEST_TOKEN)?)
}

fn test_service_with_auth(
    storage: TestStorage,
    runtime: TestRuntime,
    auth: ManagementAuth,
) -> TestResult<Service> {
    let storage: Arc<dyn ManagementStorage> = Arc::new(storage);
    let runtime: Arc<dyn RuntimeControl> = Arc::new(runtime);
    let state = ApiState::new(storage, runtime, auth, 1024 * 1024);
    Ok(Service::new(management_router(state)))
}

fn viewer_auth() -> TestResult<ManagementAuth> {
    Ok(ManagementAuth::bearer_with_context(
        TEST_TOKEN,
        UserId::new("user:viewer")?,
        ManagementRole::Viewer,
        ManagementProjectScope::all(),
    )?)
}

fn runtime_supported_config(version: u64) -> TestResult<ProjectConfig> {
    let broker_id = BrokerId::new("broker-main")?;

    Ok(ProjectConfig {
        id: ProjectId::new(TEST_PROJECT_ID)?,
        tenant_id: None,
        name: "Test Project".to_owned(),
        description: None,
        enabled: true,
        version,
        brokers: vec![test_broker_with_id(
            broker_id.clone(),
            true,
            "old-password",
        )?],
        routes: vec![test_route(broker_id)?],
        schema_mappings: Vec::new(),
        rules: Vec::new(),
        command_templates: Vec::new(),
        sinks: Vec::new(),
    })
}

fn test_broker(id: &str, enabled: bool, password: &str) -> TestResult<BrokerConnectionConfig> {
    test_broker_with_id(BrokerId::new(id)?, enabled, password)
}

fn test_broker_with_id(
    id: BrokerId,
    enabled: bool,
    password: &str,
) -> TestResult<BrokerConnectionConfig> {
    Ok(BrokerConnectionConfig {
        id,
        name: "Test Broker".to_owned(),
        host: "127.0.0.1".to_owned(),
        port: 1883,
        tls: TlsMode::Disabled,
        credentials: Some(MqttCredentials {
            username: "user".to_owned(),
            password: SecretString::new(password)?,
        }),
        keep_alive: Duration::from_secs(30),
        clean_session: true,
        client_id: "pipe-bolt-test".to_owned(),
        reconnect: ReconnectPolicy::default(),
        enabled,
    })
}

fn test_route(broker_id: BrokerId) -> TestResult<TopicRouteConfig> {
    Ok(TopicRouteConfig {
        id: pipe_bolt_domain::RouteId::new("route-main")?,
        broker_id,
        name: "Main Route".to_owned(),
        topic_filter: TopicFilter::new("devices/+/telemetry")?,
        codec: PayloadCodecKind::Json,
        schema_mapping_id: None,
        device_id: DeviceIdExtraction::TopicWildcardIndex { index: 1 },
        event_type: "telemetry".to_owned(),
        qos: MqttQos::AtLeastOnce,
        enabled: true,
        backpressure: BackpressurePolicy::Reject,
    })
}

fn update_body(
    expected_version: u64,
    config: ProjectConfig,
    reason: Option<&str>,
) -> TestResult<Value> {
    Ok(json!({
        "expected_version": expected_version,
        "config": serde_json::to_value(ProjectConfigDocumentV1::from_domain(config))?,
        "reason": reason,
    }))
}

#[derive(Clone, Default)]
struct TestStorage {
    inner: Arc<Mutex<TestStorageState>>,
}

impl TestStorage {
    fn with_config(config: ProjectConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TestStorageState {
                config: Some(config),
                ..TestStorageState::default()
            })),
        }
    }

    async fn insert_failure(&self, project_id: &str, failure_id: &str, resolved: bool) {
        let mut state = self.inner.lock().await;
        state.failures.insert(
            (project_id.to_owned(), failure_id.to_owned()),
            FailureState { resolved },
        );
    }

    async fn update_calls(&self) -> usize {
        self.inner.lock().await.update_calls
    }

    async fn last_actor_id(&self) -> Option<String> {
        self.inner
            .lock()
            .await
            .last_audit
            .as_ref()
            .and_then(|audit| audit.actor_id.as_ref())
            .map(ToString::to_string)
    }

    async fn broker_password(&self, broker_id: &str) -> Option<String> {
        self.inner
            .lock()
            .await
            .config
            .as_ref()?
            .brokers
            .iter()
            .find(|broker| broker.id.as_str() == broker_id)?
            .credentials
            .as_ref()
            .map(|credentials| credentials.password.expose_secret().to_owned())
    }

    async fn last_operational_limit(&self) -> Option<u32> {
        self.inner
            .lock()
            .await
            .last_operational_query
            .as_ref()
            .map(|query| query.limit)
    }
}

#[derive(Default)]
struct TestStorageState {
    config: Option<ProjectConfig>,
    update_calls: usize,
    last_audit: Option<AuditContext>,
    last_operational_query: Option<OperationalListQuery>,
    failures: HashMap<(String, String), FailureState>,
}

struct FailureState {
    resolved: bool,
}

#[async_trait]
impl ManagementStorage for TestStorage {
    async fn health_check(&self) -> Result<(), StorageError> {
        Ok(())
    }

    async fn load_project_config(
        &self,
        project_id: &ProjectId,
    ) -> Result<Option<ProjectConfig>, StorageError> {
        let state = self.inner.lock().await;
        Ok(state
            .config
            .as_ref()
            .filter(|config| config.id == *project_id)
            .cloned())
    }

    async fn update_project_config(
        &self,
        config: &ProjectConfig,
        expected_version: u64,
        audit: AuditContext,
    ) -> Result<ProjectConfigWriteResult, StorageError> {
        let mut state = self.inner.lock().await;
        let current_version = state.config.as_ref().map(|config| config.version);

        if current_version != Some(expected_version) {
            return Err(StorageError::VersionConflict {
                project_id: config.id.to_string(),
                expected_version: Some(expected_version),
                actual_version: current_version,
            });
        }

        state.update_calls += 1;
        state.last_audit = Some(audit);
        state.config = Some(config.clone());

        Ok(ProjectConfigWriteResult {
            project_id: config.id.clone(),
            version: config.version,
            revision_id: "revision-test".to_owned(),
            config_hash: "hash-test".to_owned(),
        })
    }

    async fn list_audit_events(
        &self,
        _project_id: &ProjectId,
        query: OperationalListQuery,
    ) -> Result<Vec<AuditEventRecord>, StorageError> {
        self.inner.lock().await.last_operational_query = Some(query);
        Ok(Vec::new())
    }

    async fn list_failures(
        &self,
        _project_id: &ProjectId,
        query: FailureListQuery,
    ) -> Result<Vec<FailureEventRecord>, StorageError> {
        let _query = query;
        Ok(Vec::new())
    }

    async fn resolve_failure(
        &self,
        project_id: &ProjectId,
        failure_id: &str,
        _resolution: &str,
        audit: AuditContext,
    ) -> Result<(), StorageError> {
        let mut state = self.inner.lock().await;
        let key = (project_id.to_string(), failure_id.to_owned());
        let Some(failure) = state.failures.get_mut(&key) else {
            return Err(StorageError::FailureNotFound {
                project_id: project_id.to_string(),
                failure_id: failure_id.to_owned(),
            });
        };

        if failure.resolved {
            return Err(StorageError::FailureAlreadyResolved {
                project_id: project_id.to_string(),
                failure_id: failure_id.to_owned(),
            });
        }

        failure.resolved = true;
        state.last_audit = Some(audit);
        Ok(())
    }

    async fn list_delivery_outcomes(
        &self,
        _project_id: &ProjectId,
        query: OperationalListQuery,
    ) -> Result<Vec<SinkDeliveryOutcomeRecord>, StorageError> {
        self.inner.lock().await.last_operational_query = Some(query);
        Ok(Vec::new())
    }
}

#[derive(Clone)]
struct TestRuntime {
    inner: Arc<Mutex<TestRuntimeState>>,
}

impl Default for TestRuntime {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TestRuntimeState::default())),
        }
    }
}

impl TestRuntime {
    async fn reject_candidate_config(self, reason: &str) -> Self {
        self.inner.lock().await.validate_mode = ValidateMode::Reject(reason.to_owned());
        self
    }

    async fn with_reload_mode(self, reload_mode: ReloadMode) -> Self {
        self.inner.lock().await.reload_mode = reload_mode;
        self
    }

    async fn reload_audit_reasons(&self) -> Vec<Option<String>> {
        self.inner
            .lock()
            .await
            .reload_audits
            .iter()
            .map(|audit| audit.reason.clone())
            .collect()
    }

    async fn execute_audit_reasons(&self) -> Vec<Option<String>> {
        self.inner
            .lock()
            .await
            .execute_audits
            .iter()
            .map(|audit| audit.reason.clone())
            .collect()
    }
}

#[derive(Default)]
struct TestRuntimeState {
    validate_mode: ValidateMode,
    reload_mode: ReloadMode,
    reload_audits: Vec<AuditContext>,
    execute_audits: Vec<AuditContext>,
}

#[derive(Default)]
enum ValidateMode {
    #[default]
    Accept,
    Reject(String),
}

#[derive(Copy, Clone, Default)]
enum ReloadMode {
    #[default]
    Success,
    ReloadInProgress,
    ShuttingDown,
    UnsafeOldRuntimeShutdown,
    StartFailed,
}

#[async_trait]
impl RuntimeControl for TestRuntime {
    async fn readiness(&self) -> Result<RuntimeReadinessResponse, RuntimeControlError> {
        Ok(RuntimeReadinessResponse {
            status: ReadinessStatus::Ready,
            project_id: TEST_PROJECT_ID.to_owned(),
            lifecycle: RuntimeLifecycleState::Running,
            active_version: Some(1),
            message: None,
        })
    }

    async fn status(
        &self,
        project_id: &ProjectId,
    ) -> Result<RuntimeStatusResponse, RuntimeControlError> {
        Ok(RuntimeStatusResponse {
            project_id: project_id.clone(),
            state: RuntimeLifecycleState::Running,
            active_version: Some(1),
            started_at: None,
            last_reload_at: None,
            last_reload_error: None,
            counters: RuntimeCountersResponse {
                pipeline: RuntimePipelineCountersResponse::default(),
                forwarder: ForwarderCountersResponse::default(),
                persistence_writer: None,
            },
        })
    }

    async fn subscribe_realtime_events(
        &self,
        _project_id: &ProjectId,
    ) -> Result<broadcast::Receiver<NormalizedEvent>, RuntimeControlError> {
        let (_tx, rx) = broadcast::channel(16);
        Ok(rx)
    }

    async fn validate_candidate_config(
        &self,
        _project_id: &ProjectId,
        _config: &ProjectConfig,
    ) -> Result<(), RuntimeControlError> {
        match &self.inner.lock().await.validate_mode {
            ValidateMode::Accept => Ok(()),
            ValidateMode::Reject(reason) => Err(RuntimeControlError::InvalidConfig {
                reason: reason.clone(),
            }),
        }
    }

    async fn reload(
        &self,
        project_id: &ProjectId,
        audit: AuditContext,
    ) -> Result<RuntimeReloadResponse, RuntimeControlError> {
        let mut state = self.inner.lock().await;
        state.reload_audits.push(audit);

        match state.reload_mode {
            ReloadMode::Success => Ok(RuntimeReloadResponse {
                project_id: project_id.clone(),
                previous_version: 1,
                active_version: 2,
                reloaded_at: OffsetDateTime::now_utc(),
                old_runtime_shutdown_error: None,
                audit_event_id: "audit-runtime-success".to_owned(),
            }),
            ReloadMode::ReloadInProgress => Err(RuntimeControlError::ReloadInProgress),
            ReloadMode::ShuttingDown => Err(RuntimeControlError::ShuttingDown {
                reason: "daemon shutdown has started".to_owned(),
            }),
            ReloadMode::UnsafeOldRuntimeShutdown => {
                Err(RuntimeControlError::UnsafeOldRuntimeShutdown {
                    reason: "old runtime shutdown failed; reload aborted".to_owned(),
                })
            }
            ReloadMode::StartFailed => Err(RuntimeControlError::StartFailed {
                reason: "runtime start failed".to_owned(),
            }),
        }
    }

    async fn execute_command(
        &self,
        project_id: &ProjectId,
        command_template_id: &CommandTemplateId,
        _params: std::collections::BTreeMap<String, serde_json::Value>,
        audit: AuditContext,
    ) -> Result<ExecuteCommandResponse, RuntimeControlError> {
        self.inner.lock().await.execute_audits.push(audit);

        Ok(ExecuteCommandResponse {
            project_id: project_id.clone(),
            command_template_id: command_template_id.clone(),
            command_execution_id: CommandExecutionId::new("command-exec-test")
                .expect("command execution id"),
            status: CommandExecutionStatusResponse::Queued,
            broker_id: BrokerId::new("broker-main").expect("broker id"),
            topic: "devices/device-1/command".to_owned(),
            qos: MqttQos::AtLeastOnce,
            retain: false,
            payload_size_bytes: 14,
            queued_at: OffsetDateTime::now_utc(),
            audit_event_id: "audit-test".to_owned(),
        })
    }
}
