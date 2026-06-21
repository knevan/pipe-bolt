-- Add migration script here
CREATE TABLE project_configs
(
    project_id TEXT PRIMARY KEY,
    tenant_id  TEXT NULL,
    name       TEXT        NOT NULL,
    enabled    BOOLEAN     NOT NULL,
    version    BIGINT      NOT NULL CHECK (version >= 0),
    config     JSONB       NOT NULL CHECK (jsonb_typeof(config) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX project_configs_tenant_id_idx
    ON project_configs (tenant_id) WHERE tenant_id IS NOT NULL;

CREATE INDEX project_configs_enabled_idx
    ON project_configs (enabled);

CREATE TABLE audit_events
(
    audit_event_id TEXT PRIMARY KEY,
    project_id     TEXT NULL,
    actor_id       TEXT NULL,
    action         TEXT        NOT NULL,
    target_type    TEXT        NOT NULL,
    target_id      TEXT        NOT NULL,
    status         TEXT        NOT NULL CHECK (status IN ('succeeded', 'failed')),
    reason         TEXT NULL,
    metadata       JSONB       NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX audit_events_project_time_idx
    ON audit_events (project_id, occurred_at DESC) WHERE project_id IS NOT NULL;

CREATE INDEX audit_events_target_idx
    ON audit_events (target_type, target_id, occurred_at DESC);

CREATE TABLE sink_delivery_outcomes
(
    delivery_id         TEXT PRIMARY KEY,
    project_id          TEXT        NOT NULL,
    event_id            TEXT        NOT NULL,
    sink_id             TEXT        NOT NULL,
    status              TEXT        NOT NULL CHECK (status IN
                                                    ('delivered', 'http_rejected', 'timed_out', 'response_too_large',
                                                     'failed')),
    http_status         INTEGER NULL CHECK (http_status IS NULL OR (http_status >= 100 AND http_status <= 599)),
    response_body_bytes BIGINT NULL CHECK (response_body_bytes IS NULL OR response_body_bytes >= 0),
    failure_reason      TEXT NULL,
    occurred_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX sink_delivery_outcomes_project_time_idx
    ON sink_delivery_outcomes (project_id, occurred_at DESC);

CREATE INDEX sink_delivery_outcomes_event_idx
    ON sink_delivery_outcomes (event_id);

CREATE INDEX sink_delivery_outcomes_sink_time_idx
    ON sink_delivery_outcomes (sink_id, occurred_at DESC);

CREATE INDEX sink_delivery_outcomes_failure_idx
    ON sink_delivery_outcomes (project_id, occurred_at DESC) WHERE status <> 'delivered';

CREATE TABLE failure_events
(
    failure_id   TEXT PRIMARY KEY,
    project_id   TEXT        NOT NULL,
    event_id     TEXT NULL,
    sink_id      TEXT NULL,
    component    TEXT        NOT NULL,
    failure_kind TEXT        NOT NULL,
    severity     TEXT        NOT NULL CHECK (severity IN ('warning', 'error', 'critical')),
    message      TEXT        NOT NULL,
    details      JSONB       NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(details) = 'object'),
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at  TIMESTAMPTZ NULL,
    resolution   TEXT NULL
);

CREATE INDEX failure_events_project_time_idx
    ON failure_events (project_id, occurred_at DESC);

CREATE INDEX failure_events_unresolved_idx
    ON failure_events (project_id, occurred_at DESC) WHERE resolved_at IS NULL;

CREATE INDEX failure_events_component_idx
    ON failure_events (component, failure_kind, occurred_at DESC);