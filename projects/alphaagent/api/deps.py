"""
FastAPI dependencies — a tiny connection pool for the analytics endpoints,
plus a simple per-IP rate limiter.

We use psycopg's connection_pool so the analytics endpoints don't pay connect
latency on every request. The agent path uses its own short-lived connections
inside safe_exec.run_safely() because each invocation sets a per-statement
timeout.
"""
from __future__ import annotations

import time
from collections import defaultdict, deque
from typing import Iterator

from fastapi import HTTPException, Request, status

from agent.config import get_agent_settings

_pool = None  # typed as ConnectionPool | None at runtime


def get_pool():
    """
    Lazily construct a psycopg connection pool on first call. Import of
    psycopg_pool is deferred so importing api.deps in a minimal test env
    (e.g., one without the binary pq wrapper) doesn't explode.
    """
    global _pool
    if _pool is None:
        from psycopg_pool import ConnectionPool  # lazy import

        s = get_agent_settings()
        _pool = ConnectionPool(
            conninfo=s.pg_dsn_readonly,
            min_size=1,
            max_size=8,
            timeout=5.0,
            kwargs={"autocommit": True},
        )
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


def get_db() -> Iterator:
    pool = get_pool()
    with pool.connection() as conn:
        yield conn


# ---------------------------------------------------------------------------
# Rate limiter — in-memory sliding window.
#
# This is a demo-grade guard, not a production limiter. In production we'd use
# Redis + token bucket. But for portfolio-project purposes it demonstrates
# awareness of cost-controls on LLM-backed endpoints.
# ---------------------------------------------------------------------------

_WINDOW_SEC = 60
_MAX_PER_WINDOW = 30  # 30 calls/minute/IP for the /ask endpoint
_hits: dict[str, deque[float]] = defaultdict(deque)


def rate_limit(request: Request) -> None:
    client_ip = request.client.host if request.client else "unknown"
    now = time.time()
    q = _hits[client_ip]
    # evict old
    cutoff = now - _WINDOW_SEC
    while q and q[0] < cutoff:
        q.popleft()
    if len(q) >= _MAX_PER_WINDOW:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit: {_MAX_PER_WINDOW} requests/{_WINDOW_SEC}s. Try again shortly.",
        )
    q.append(now)


# ---------------------------------------------------------------------------
# API-key auth stub
#
# If ALPHAAGENT_API_KEY is set in env, require X-API-Key header to match.
# If not set, allow everything (demo mode). Production would use OAuth2 / mTLS.
# ---------------------------------------------------------------------------

import os

def require_api_key(request: Request) -> None:
    expected = os.environ.get("ALPHAAGENT_API_KEY")
    if not expected:
        return  # demo mode — no auth
    provided = request.headers.get("X-API-Key")
    if provided != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key",
        )
