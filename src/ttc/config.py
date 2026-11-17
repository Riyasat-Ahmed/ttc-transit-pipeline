"""Settings loaded from environment variables (and .env when run locally)."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Database
    database_url: str = "postgresql://ttc:ttc@localhost:15432/ttc"

    # Broker
    kafka_bootstrap: str = "localhost:19092"
    topic_vehicle_positions: str = "vehicle_positions"
    topic_trip_updates: str = "trip_updates"
    topic_heartbeats: str = "heartbeats"
    topic_retention_ms: int = 24 * 60 * 60 * 1000
    topic_partitions: int = 3
    consumer_group: str = "ttc-processor"

    # Feeds
    vehicles_url: str = "https://bustime.ttc.ca/gtfsrt/vehicles"
    trips_url: str = "https://bustime.ttc.ca/gtfsrt/trips"
    # "Surface Routes and Schedules for Bustime": the static feed whose trip ids
    # match the bustime realtime feeds. The general "TTC Routes and Schedules"
    # dataset uses different trip ids (~0% match), so don't use that one.
    static_gtfs_url: str = (
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/"
        "bd4809dd-e289-4de8-bbde-c5c00dafbf4f/resource/"
        "28514055-d011-4ed7-8bb0-97961dfe2b66/download/surfacegtfs.zip"
    )
    feed_user_agent: str = "ttc-realtime-pipeline/0.1 (open-source data project)"
    poll_interval_s: float = 20.0
    http_timeout_s: float = 15.0
    max_backoff_s: float = 300.0

    # Processing
    timezone: str = "America/Toronto"
    # Only stop predictions at most this far in the future are written. Rows keep
    # being overwritten until the bus reaches the stop, so the final row is the
    # observed arrival. Far-future predictions mostly echo the schedule.
    prediction_horizon_s: int = 120
    max_abs_delay_s: int = 2 * 60 * 60
    batch_size: int = 2000

    # Jobs and alerts
    static_refresh_hours: float = 24.0
    discord_webhook_url: str = ""
    stale_alert_after_s: int = 300
    alert_check_interval_s: int = 60
    healthcheck_ping_url: str = ""

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
