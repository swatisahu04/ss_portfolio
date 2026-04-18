"""Shared config for ingestion modules."""
from __future__ import annotations

import os
from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_db: str = "alphaagent"
    pg_user: str = "alphaagent"
    pg_password: str = "alphaagent"

    kafka_bootstrap: str = "localhost:9092"
    kafka_trades_topic: str = "trades.v1"

    class Config:
        env_file = ".env"
        extra = "ignore"

    @property
    def pg_dsn(self) -> str:
        return (
            f"postgresql://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


os.environ.setdefault("PYTHONUNBUFFERED", "1")
