"""
AlphaAgent API — FastAPI entrypoint.

Run locally:
    uvicorn api.main:app --reload --port 8000

Then visit:
    http://localhost:8000/docs         (Swagger UI)
    http://localhost:8000/v1/health    (plain health check)

The API has three responsibilities:
  1. Expose the LangGraph agent as an HTTP endpoint (/v1/ask)
  2. Serve deterministic analytics from the dbt marts (/v1/portfolio/*)
  3. Surface observability data for the Streamlit UI (/v1/dq, /v1/agent/queries, /v1/lineage)

Every request is logged as a single JSON line with latency, status, and
agent-specific metadata. This is what ops looks like for a real LLM-backed
product — not just printf.
"""
from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.deps import close_pool, get_pool
from api.logging_config import api_log, configure_logging
from api.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    api_log.info("api_startup")
    # Eagerly open the pool so first request doesn't pay the cost.
    try:
        get_pool()
        api_log.info("db_pool_ready")
    except Exception as e:
        # Don't crash on startup — health will report db_reachable=False.
        api_log.warning("db_pool_init_failed", extra={"error": str(e)})
    yield
    close_pool()
    api_log.info("api_shutdown")


app = FastAPI(
    title="AlphaAgent API",
    description=(
        "Multi-agent natural-language query + portfolio analytics for a "
        "synthetic asset-management warehouse. See /docs for interactive Swagger."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

# CORS — permissive for local dev; tighten in production.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    req_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    t0 = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        response.headers["x-request-id"] = req_id
        return response
    finally:
        dur_ms = int((time.perf_counter() - t0) * 1000)
        api_log.info("http_request", extra={
            "req_id": req_id,
            "method": request.method,
            "path": request.url.path,
            "query": str(request.url.query) if request.url.query else None,
            "status": status_code,
            "duration_ms": dur_ms,
            "client_ip": request.client.host if request.client else None,
        })


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    api_log.exception("unhandled_exception", extra={
        "path": request.url.path,
        "method": request.method,
    })
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "type": type(exc).__name__},
    )


app.include_router(router)


@app.get("/", include_in_schema=False)
def root():
    return {
        "service": "alphaagent-api",
        "version": app.version,
        "docs": "/docs",
        "health": "/v1/health",
    }
