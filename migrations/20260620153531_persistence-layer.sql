-- Add migration script here

-- CORE CONFIGURATION TABLES
CREATE TABLE project_configs (
    project_id TEXT PRIMARY KEY,
    tenant_id TEXT NULL,
    name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    version BIGINT NOT NULL CHECK (version >= 0),
    config_hash TEXT NOT NULL CHECK (
        octet_length(config_hash) BETWEEN 1 AND 128
    ),
    config JSONB NOT NULL CHECK (
        jsonb_typeof (config) = 'object'
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX project_configs_tenant_id_idx ON project_configs (tenant_id)
WHERE
    tenant_id IS NOT NULL;

CREATE INDEX project_configs_enabled_idx ON project_configs (enabled);

CREATE TABLE project_config_revisions (
    revision_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES project_configs (project_id) ON DELETE RESTRICT,
    version BIGINT NOT NULL CHECK (version >= 0),
    config_hash TEXT NOT NULL CHECK (
        octet_length(config_hash) BETWEEN 1 AND 128
    ),
    config JSONB NOT NULL CHECK (
        jsonb_typeof (config) = 'object'
    ),
    actor_id TEXT NULL,
    reason TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, version)
);

CREATE INDEX project_config_revisions_project_time_idx ON project_config_revisions (project_id, created_at DESC);

CREATE TABLE storage_retention_policies (
    project_id TEXT PRIMARY KEY REFERENCES project_configs (project_id) ON DELETE RESTRICT,
    audit_retention_days INTEGER NOT NULL CHECK (
        audit_retention_days BETWEEN 1 AND 3650
    ),
    delivery_outcome_retention_days INTEGER NOT NULL CHECK (
        delivery_outcome_retention_days BETWEEN 1 AND 3650
    ),
    failure_retention_days INTEGER NOT NULL CHECK (
        failure_retention_days BETWEEN 1 AND 3650
    ),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- OPERATIONAL & EVENT TABLES
CREATE TABLE audit_events
(
    audit_event_id TEXT PRIMARY KEY,
    project_id     TEXT        NULL REFERENCES project_configs (project_id) ON DELETE RESTRICT,
    actor_id       TEXT        NULL,
    action         TEXT        NOT NULL CHECK (octet_length(action) BETWEEN 1 AND 96),
    target_type    TEXT        NOT NULL CHECK (octet_length(target_type) BETWEEN 1 AND 96),
    target_id      TEXT        NOT NULL CHECK (octet_length(target_id) BETWEEN 1 AND 256),
    status         TEXT        NOT NULL CHECK (status IN ('succeeded', 'failed')),
    reason         TEXT        NULL CHECK (reason IS NULL OR octet_length(reason) <= 1024),
    metadata       JSONB       NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object' AND octet_length(metadata::text) <= 16384),
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX audit_events_project_time_idx ON audit_events (project_id, occurred_at DESC)
WHERE
    project_id IS NOT NULL;

CREATE INDEX audit_events_target_idx ON audit_events (
    target_type,
    target_id,
    occurred_at DESC
);

CREATE TABLE sink_delivery_outcomes (
    delivery_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES project_configs (project_id) ON DELETE RESTRICT,
    event_id TEXT NOT NULL,
    sink_id TEXT NOT NULL,
    correlation_id TEXT NULL CHECK (
        correlation_id IS NULL
        OR octet_length(correlation_id) <= 256
    ),
    status TEXT NOT NULL CHECK (
        status IN (
            'delivered',
            'http_rejected',
            'timed_out',
            'response_too_large',
            'failed'
        )
    ),
    http_status INTEGER NULL CHECK (
        http_status IS NULL
        OR (
            http_status >= 100
            AND http_status <= 599
        )
    ),
    response_body_bytes BIGINT NULL CHECK (
        response_body_bytes IS NULL
        OR response_body_bytes >= 0
    ),
    failure_reason TEXT NULL,
    duration_ms BIGINT NULL CHECK (
        duration_ms IS NULL
        OR duration_ms >= 0
    ),
    attempt INTEGER NOT NULL DEFAULT 1 CHECK (attempt >= 1),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX sink_delivery_outcomes_project_time_idx ON sink_delivery_outcomes (project_id, occurred_at DESC);

CREATE INDEX sink_delivery_outcomes_event_idx ON sink_delivery_outcomes (event_id);

CREATE INDEX sink_delivery_outcomes_sink_time_idx ON sink_delivery_outcomes (sink_id, occurred_at DESC);

CREATE INDEX sink_delivery_outcomes_failure_idx ON sink_delivery_outcomes (project_id, occurred_at DESC)
WHERE
    status <> 'delivered';

CREATE TABLE command_executions (
    command_execution_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES project_configs (project_id) ON DELETE RESTRICT,
    command_template_id TEXT NOT NULL,
    broker_id TEXT NOT NULL,
    actor_id TEXT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'published', 'failed')),
    topic TEXT NOT NULL CHECK (octet_length(topic) BETWEEN 1 AND 1024),
    qos TEXT NOT NULL CHECK (qos IN ('at_most_once', 'at_least_once', 'exactly_once')),
    retain BOOLEAN NOT NULL,
    payload_size_bytes BIGINT NOT NULL CHECK (payload_size_bytes >= 0),
    failure_reason TEXT NULL CHECK (failure_reason IS NULL OR octet_length(failure_reason) <= 1024),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX command_executions_project_time_idx ON command_executions (project_id, occurred_at DESC);

CREATE INDEX command_executions_template_time_idx ON command_executions (project_id, command_template_id, occurred_at DESC);

CREATE INDEX command_executions_actor_time_idx ON command_executions (actor_id, occurred_at DESC)
WHERE
    actor_id IS NOT NULL;

CREATE TABLE failure_events
(
    failure_id   TEXT PRIMARY KEY,
    project_id   TEXT        NOT NULL REFERENCES project_configs (project_id) ON DELETE RESTRICT,
    event_id     TEXT        NULL,
    sink_id      TEXT        NULL,
    component    TEXT        NOT NULL CHECK (octet_length(component) BETWEEN 1 AND 96),
    failure_kind TEXT        NOT NULL CHECK (octet_length(failure_kind) BETWEEN 1 AND 96),
    severity     TEXT        NOT NULL CHECK (severity IN ('warning', 'error', 'critical')),
    message      TEXT        NOT NULL CHECK (octet_length(message) BETWEEN 1 AND 2048),
    details      JSONB       NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(details) = 'object' AND octet_length(details::text) <= 16384),
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at  TIMESTAMPTZ NULL,
    resolution   TEXT        NULL CHECK (resolution IS NULL OR octet_length(resolution) <= 2048)
);

CREATE INDEX failure_events_project_time_idx ON failure_events (project_id, occurred_at DESC);

CREATE INDEX failure_events_unresolved_idx ON failure_events (project_id, occurred_at DESC)
WHERE
    resolved_at IS NULL;

CREATE INDEX failure_events_component_idx ON failure_events (
    component,
    failure_kind,
    occurred_at DESC
);
