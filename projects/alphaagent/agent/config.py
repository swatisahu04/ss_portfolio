"""Agent runtime configuration."""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class AgentSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # LLM
    anthropic_api_key: str | None = None
    openai_api_key: str | None = None
    llm_provider: str = "anthropic"   # anthropic | openai
    llm_model: str = "claude-sonnet-4-6"

    # Database (read-only for agent queries)
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_db: str = "alphaagent"
    pg_user: str = "agent_user"
    pg_password: str = "agent_readonly"

    # Safety + cost controls
    agent_query_timeout_s: int = 8
    agent_max_rows: int = 10_000
    agent_cache_enabled: bool = True
    agent_max_retries: int = 1

    @property
    def pg_dsn_readonly(self) -> str:
        return (
            f"postgresql://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )


@lru_cache(maxsize=1)
def get_agent_settings() -> AgentSettings:
    return AgentSettings()
