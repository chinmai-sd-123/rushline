from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RUSHLINE_",
        env_file=".env",
        extra="ignore",
    )

    database_url: str
    redis_url: str
    hold_ttl_seconds: int = Field(default=600, gt=0)
    max_seats_per_hold: int = Field(default=6, gt=0)
    db_pool_size: int = Field(default=10, gt=0)
    db_max_overflow: int = Field(default=5, ge=0)
    db_pool_timeout_seconds: float = Field(default=5, gt=0)
    jwt_secret: str = Field(min_length=32)  # HS256 key: at least 256 bits
    jwt_ttl_seconds: int = Field(default=3600, gt=0)
    enable_dev_login: bool = False  # only for local demos
    # How seats get claimed. Chosen by benchmark (ADR 002); "naive" is the unsafe baseline.
    hold_strategy: Literal[
        "naive", "pessimistic", "pessimistic_nowait", "optimistic", "atomic", "serializable"
    ] = "pessimistic"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    holds_per_minute: int = Field(default=20, gt=0)  # per user
    bookings_per_minute: int = Field(default=10, gt=0)
    seat_map_cache: bool = True  # off only to measure what the cache is worth
    seat_map_fresh_seconds: float = Field(default=1.0, ge=0)
    seat_map_stale_seconds: float = Field(default=10.0, gt=0)
    payment_url: str = "http://localhost:8001"
    payment_timeout_seconds: float = Field(default=2.0, gt=0)
    payment_window_seconds: int = Field(default=120, gt=0)  # paying holds settle after this
    worker_interval_seconds: float = Field(default=1.0, gt=0)
    worker_batch_size: int = Field(default=50, gt=0)
    capture_max_attempts: int = Field(default=5, gt=0)
    capture_retry_after_seconds: int = Field(default=10, ge=0)  # let the API capture first
    # Agent (phase 2). Without an OpenAI key the agent and embeddings use offline fakes.
    openai_api_key: SecretStr | None = None
    llm_models: list[str] = ["gpt-5-mini", "gpt-4.1-mini"]  # cheapest first, then fallbacks
    llm_timeout_seconds: float = Field(default=30, gt=0)
    llm_daily_tokens_per_user: int = Field(default=200_000, gt=0)
    conversation_token_budget: int = Field(default=50_000, gt=0)  # hard cap (design)
    run_max_steps: int = Field(default=12, gt=0)  # model calls per run
    agent_database_url: str | None = None  # LangGraph checkpoints; defaults to database_url
    agent_job_visibility_seconds: int = Field(default=300, gt=0)  # redeliver after this
    embedding_model: str = "text-embedding-3-small"
    api_url: str = "http://localhost:8000"  # the agent and MCP server call the REST API
    mcp_token: SecretStr | None = None  # per-user token for the MCP server
