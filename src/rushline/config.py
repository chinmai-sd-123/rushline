from typing import Literal

from pydantic import Field
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
