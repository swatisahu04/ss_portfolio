-- AlphaAgent: initial schemas + roles
-- Runs on first postgres container startup only

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS airflow;
CREATE SCHEMA IF NOT EXISTS metadata;

-- Read-only role the agent uses — cannot DDL/DML, only SELECT on marts
CREATE ROLE alphaagent_readonly NOLOGIN;
GRANT USAGE ON SCHEMA marts TO alphaagent_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO alphaagent_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO alphaagent_readonly;

CREATE USER agent_user WITH PASSWORD 'agent_readonly';
GRANT alphaagent_readonly TO agent_user;

-- Metadata table the agent uses to introspect mart schemas for prompt context
CREATE TABLE IF NOT EXISTS metadata.mart_descriptions (
    mart_name TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    grain TEXT,
    business_owner TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Query log for observability
CREATE TABLE IF NOT EXISTS metadata.agent_query_log (
    id BIGSERIAL PRIMARY KEY,
    question TEXT NOT NULL,
    generated_sql TEXT,
    row_count INT,
    latency_ms INT,
    cost_usd NUMERIC(10, 6),
    success BOOLEAN,
    error TEXT,
    llm_model TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_agent_query_log_created_at ON metadata.agent_query_log(created_at DESC);
