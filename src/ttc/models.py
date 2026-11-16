"""Event schemas carried on the broker topics (JSON-encoded pydantic models)."""

from __future__ import annotations

from pydantic import BaseModel, Field


def vehicle_event_key(feed_ts: int, vehicle_id: str) -> str:
    return f"{feed_ts}:{vehicle_id}"


def trip_event_key(feed_ts: int, trip_id: str) -> str:
    return f"{feed_ts}:{trip_id}"


class VehiclePositionEvent(BaseModel):
    event_key: str
    feed_ts: int  # feed header timestamp (epoch seconds)
    ts: int  # vehicle's own report timestamp (epoch seconds)
    vehicle_id: str
    trip_id: str | None = None
    route_id: str | None = None
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    bearing: float | None = None
    speed: float | None = None
    stop_id: str | None = None
    current_status: str | None = None
    occupancy: str | None = None


class StopTimeEvent(BaseModel):
    stop_sequence: int
    stop_id: str | None = None
    arrival_time: int | None = None  # epoch seconds
    departure_time: int | None = None
    schedule_relationship: str = "SCHEDULED"

    @property
    def event_time(self) -> int | None:
        return self.arrival_time or self.departure_time


class TripUpdateEvent(BaseModel):
    event_key: str
    feed_ts: int
    trip_id: str
    route_id: str | None = None
    schedule_relationship: str = "SCHEDULED"
    start_date: str | None = None  # YYYYMMDD, if the feed provides it
    stops: list[StopTimeEvent] = []


class Heartbeat(BaseModel):
    event_key: str
    feed: str
    polled_at: float  # epoch seconds
    ok: bool
    entity_count: int = 0
    feed_ts: int | None = None
    new_snapshot: bool = False
    http_status: int | None = None
    latency_ms: int | None = None
    error: str | None = None
