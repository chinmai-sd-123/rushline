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
