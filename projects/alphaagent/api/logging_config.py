"""
Structured JSON logging for the API.

Every request logs a single line at INFO containing method, path, status,
duration_ms, client_ip, and any request-id header. Every agent-backed call
additionally emits an event with generated_sql, row_count, llm_cost_usd.

This makes it trivial to ship logs to CloudWatch / Datadog / Loki without
any further transformation.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # any extras attached via logger.info(..., extra={...})
        for k, v in record.__dict__.items():
            if k in {"args", "msg", "levelname", "levelno", "pathname", "filename",
                     "module", "exc_info", "exc_text", "stack_info", "lineno",
                     "funcName", "created", "msecs", "relativeCreated", "thread",
                     "threadName", "processName", "process", "name", "asctime",
                     "taskName"}:
                continue
            payload[k] = v
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    # Clear any default handlers (uvicorn installs some)
    for h in list(root.handlers):
        root.removeHandler(h)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(JSONFormatter())
    root.addHandler(h)
    root.setLevel(level)
    # Quiet noisy libs
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


api_log = logging.getLogger("alphaagent.api")
