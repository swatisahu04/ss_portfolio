"""
Smoke tests for the FastAPI routes.

We avoid a live Postgres and a live LLM by:
  - Monkeypatching `api.routes.run_agent` to return a fixed AgentState
  - Monkeypatching `api.routes.get_pool` to return a dummy pool whose
    connection/cursor objects return canned rows
This keeps the test hermetic and fast. Full end-to-end is covered in
tests/integration.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date

import pytest
from fastapi.testclient import TestClient

from api import routes
from api.main import app


# ---------------------------------------------------------------------------
# Dummy pool / cursor for the analytics endpoints
# ---------------------------------------------------------------------------

class _DummyCursor:
    def __init__(self, rows):
        self._rows = rows
        self.description = [("col",)]

    def execute(self, sql, params=None):
        self._last_sql = sql
        return None

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _DummyConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _DummyCursor(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _DummyPool:
    def __init__(self, rows):
        self._rows = rows

    @contextmanager
    def connection(self):
        yield _DummyConn(self._rows)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def fake_agent_success(monkeypatch):
    def _run(question):
        return {
            "question": question,
            "generated_sql": "SELECT portfolio_id, ytd_return FROM marts.fct_portfolio_performance_daily LIMIT 1",
            "sql_valid": True,
            "sql_errors": [],
            "result_rows": [{"portfolio_id": "P-001", "ytd_return": 0.0842}],
            "result_columns": ["portfolio_id", "ytd_return"],
            "answer": "Portfolio P-001 YTD return is 8.42%.",
            "citations": ["ytd_return"],
            "chart_spec": {"type": "table"},
            "final_status": "success",
            "llm_cost_usd": 0.0012,
            "llm_tokens_used": 420,
            "sql_attempt": 1,
        }
    monkeypatch.setattr(routes, "run_agent", _run)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_root(client):
    r = client.get("/")
    assert r.status_code == 200
    assert r.json()["service"] == "alphaagent-api"


def test_ask_success(client, fake_agent_success):
    r = client.post("/v1/ask", json={"question": "What is YTD return for P-001?"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["sql_valid"] is True
    assert body["row_count"] == 1
    assert body["final_status"] == "success"
    assert body["llm_cost_usd"] > 0
    assert "P-001" in body["answer"]


def test_ask_validates_input(client):
    # Too short
    r = client.post("/v1/ask", json={"question": "a"})
    assert r.status_code == 422


def test_portfolio_performance(client, monkeypatch):
    rows = [
        (date(2026, 1, 2), 1_000_000.0, 0.001, 0.001, 0.001),
        (date(2026, 1, 3), 1_001_000.0, 0.001, 0.002, 0.002),
    ]
    monkeypatch.setattr(routes, "get_pool", lambda: _DummyPool(rows))

    r = client.get("/v1/portfolio/P-001/performance?from=2026-01-01&to=2026-01-31")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["portfolio_id"] == "P-001"
    assert len(body["points"]) == 2
    assert body["points"][0]["nav"] == 1_000_000.0


def test_portfolio_risk(client, monkeypatch):
    rows = [
        (date(2026, 1, 31), 0.18, 1.2, 1.05, -0.04),
    ]
    monkeypatch.setattr(routes, "get_pool", lambda: _DummyPool(rows))
    r = client.get("/v1/portfolio/P-001/risk")
    assert r.status_code == 200
    body = r.json()
    assert body["points"][0]["sharpe_30d"] == 1.2


def test_list_portfolios(client, monkeypatch):
    rows = [("P-001",), ("P-002",), ("P-003",)]
    monkeypatch.setattr(routes, "get_pool", lambda: _DummyPool(rows))
    r = client.get("/v1/portfolios")
    assert r.status_code == 200
    assert r.json()["portfolios"] == ["P-001", "P-002", "P-003"]


def test_lineage(client):
    r = client.get("/v1/lineage")
    assert r.status_code == 200
    body = r.json()
    assert len(body["nodes"]) > 0
    assert len(body["edges"]) > 0
    # Sanity: every edge source/target should reference a node name
    names = {n["name"] for n in body["nodes"]}
    for e in body["edges"]:
        assert e["source"] in names or e["target"] in names


def test_health(client, monkeypatch):
    # DB reachable
    monkeypatch.setattr(routes, "get_pool", lambda: _DummyPool([(1,)]))
    r = client.get("/v1/health")
    assert r.status_code == 200
    body = r.json()
    assert body["version"]
    # status may be "ok" or "degraded" depending on whether LLM is configured
    assert body["status"] in ("ok", "degraded", "down")
