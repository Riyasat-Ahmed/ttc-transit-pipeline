"""Consume -> validate -> compute delays -> idempotent write -> commit offsets.

Offsets are committed only after the database transaction commits, so a crash
anywhere replays the uncommitted messages (at-least-once). Every write is
an idempotent upsert keyed on natural keys, so replays never double-count.
"""

from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass, field
from typing import Protocol
from zoneinfo import ZoneInfo

import psycopg
from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError

from ttc import db
from ttc.config import Settings
from ttc.delay import ScheduledTrip, ServiceCalendar, StopDelay, compute_delays
from ttc.models import Heartbeat, TripUpdateEvent, VehiclePositionEvent

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RawMessage:
    topic: str
    partition: int
    offset: int
    value: bytes | None


class Schedule(Protocol):
    trip_ids: set[str]
    calendar: ServiceCalendar

    def get_trips(self, trip_ids) -> dict[str, ScheduledTrip]: ...


@dataclass
class Batch:
    vehicles: list[tuple[VehiclePositionEvent, bool]] = field(default_factory=list)
    stop_delays: list[StopDelay] = field(default_factory=list)
    heartbeats: list[Heartbeat] = field(default_factory=list)
    bad: list[tuple[str, int, int, str, str]] = field(default_factory=list)
    stats: dict[str, dict] = field(default_factory=dict)

    def stat(self, topic: str) -> dict:
        return self.stats.setdefault(topic, {"topic": topic, "received": 0, "malformed": 0})


def build_batch(messages: list[RawMessage], schedule: Schedule, settings: Settings) -> Batch:
    """Validate and transform a batch of raw messages. No database writes."""
    tz = ZoneInfo(settings.timezone)
    batch = Batch()
    trip_events: list[TripUpdateEvent] = []
    models = {
        settings.topic_vehicle_positions: VehiclePositionEvent,
        settings.topic_trip_updates: TripUpdateEvent,
        settings.topic_heartbeats: Heartbeat,
    }

    for m in messages:
        st = batch.stat(m.topic)
        st["received"] += 1
        model = models.get(m.topic)
        try:
            if model is None:
                raise ValueError(f"unexpected topic {m.topic}")
            if not m.value:
                raise ValueError("empty message")
            event = model.model_validate_json(m.value)
        except (ValidationError, ValueError) as exc:
            st["malformed"] += 1
            payload = (m.value or b"")[:2000].decode("utf-8", "replace")
            batch.bad.append((m.topic, m.partition, m.offset, str(exc)[:500], payload))
            continue
        if isinstance(event, VehiclePositionEvent):
            matched = bool(event.trip_id) and event.trip_id in schedule.trip_ids
            batch.vehicles.append((event, matched))
            if matched:
                st["matched_trips"] = st.get("matched_trips", 0) + 1
            else:
                st["unmatched_trips"] = st.get("unmatched_trips", 0) + 1
        elif isinstance(event, TripUpdateEvent):
            trip_events.append(event)
        else:
            batch.heartbeats.append(event)

    if trip_events:
        st = batch.stat(settings.topic_trip_updates)
        for k in ("matched_trips", "unmatched_trips", "added_trips", "outliers", "stop_mismatches"):
            st.setdefault(k, 0)
        trips = schedule.get_trips(e.trip_id for e in trip_events)
        for e in trip_events:
            if e.schedule_relationship != "SCHEDULED":
                st["added_trips"] += 1  # NEW/ADDED/CANCELED trips have no schedule to compare
                continue
            trip = trips.get(e.trip_id)
            if trip is None:
                st["unmatched_trips"] += 1
                continue
            st["matched_trips"] += 1
            res = compute_delays(
                e, trip, schedule.calendar, tz,
                settings.prediction_horizon_s, settings.max_abs_delay_s,
            )
            st["outliers"] += res.skipped_outliers
            st["stop_mismatches"] += res.stop_mismatches
            batch.stop_delays.extend(res.delays)
        batch.stop_delays = db.dedupe_stop_delays(batch.stop_delays)
    return batch


def write_batch(conn: psycopg.Connection, batch: Batch, settings: Settings) -> None:
    """Write everything in one transaction."""
    with conn.transaction(), conn.cursor() as cur:
        vp = batch.stat(settings.topic_vehicle_positions) if batch.vehicles else None
        if vp is not None:
            ins = db.insert_vehicle_positions(cur, batch.vehicles)
            vp["inserted"] = ins
            vp["duplicates"] = len(batch.vehicles) - ins
        if settings.topic_trip_updates in batch.stats:
            tu = batch.stats[settings.topic_trip_updates]
            ins, upd = db.upsert_stop_delays(cur, batch.stop_delays)
            tu["inserted"], tu["updated"] = ins, upd
            tu["duplicates"] = len(batch.stop_delays) - ins - upd
        if batch.heartbeats:
            db.insert_heartbeats(cur, batch.heartbeats)
        db.insert_bad_messages(cur, batch.bad)
        db.insert_stats(cur, list(batch.stats.values()))


class Processor:
    def __init__(self, settings: Settings, consumer: Consumer, conn: psycopg.Connection, schedule):
        self.settings = settings
        self.consumer = consumer
        self.conn = conn
        self.schedule = schedule
        self.running = True

    def stop(self, *_):
        self.running = False

    def run(self) -> None:
        s = self.settings
        topics = [s.topic_vehicle_positions, s.topic_trip_updates, s.topic_heartbeats]
        self.consumer.subscribe(topics)
        log.info("processor started", extra={"group": s.consumer_group})
        while self.running:
            self.schedule.refresh_if_changed()
            msgs = self.consumer.consume(num_messages=s.batch_size, timeout=1.0)
            raw = []
            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("consumer error", extra={"error": str(m.error())})
                    continue
                raw.append(RawMessage(m.topic(), m.partition(), m.offset(), m.value()))
            if not raw:
                continue
            batch = build_batch(raw, self.schedule, s)
            try:
                self._write_with_retry(batch)
            except psycopg.OperationalError:
                raise
            except psycopg.Error as exc:
                # A data error on one message must not wedge the pipeline: retry the
                # batch message by message and quarantine whatever still fails.
                log.error("batch rejected; isolating bad messages", extra={"error": str(exc)})
                self._write_one_by_one(raw)
                batch = Batch()
            # Only now is it safe to move the committed offsets forward.
            self.consumer.commit(asynchronous=False)
            log.info(
                "batch committed",
                extra={
                    "messages": len(raw),
                    "stop_rows": len(batch.stop_delays),
                    "malformed": len(batch.bad),
                },
            )
        self.consumer.close()

    def _write_one_by_one(self, raw: list[RawMessage]) -> None:
        for m in raw:
            try:
                self._write_with_retry(build_batch([m], self.schedule, self.settings))
            except psycopg.OperationalError:
                raise
            except psycopg.Error as exc:
                payload = (m.value or b"")[:2000].decode("utf-8", "replace")
                with self.conn.transaction(), self.conn.cursor() as cur:
                    db.insert_bad_messages(
                        cur, [(m.topic, m.partition, m.offset, f"db: {exc}"[:500], payload)]
                    )
                    db.insert_stats(cur, [{"topic": m.topic, "received": 1, "malformed": 1}])

    def _write_with_retry(self, batch: Batch, attempts: int = 5) -> None:
        for attempt in range(1, attempts + 1):
            try:
                write_batch(self.conn, batch, self.settings)
                return
            except psycopg.OperationalError as exc:
                log.error("db write failed", extra={"attempt": attempt, "error": str(exc)})
                if attempt == attempts:
                    raise  # exit; the container restarts and replays from the last commit
                time.sleep(2**attempt)
                if self.conn.closed or self.conn.broken:
                    self.conn = db.connect(self.settings.database_url)
                    self.schedule.conn = self.conn


def main(settings: Settings) -> None:
    from ttc.schedule import ScheduleCache

    conn = db.connect(settings.database_url)
    schedule = ScheduleCache(conn)
    schedule.refresh_if_changed(force=True)
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 600_000,
        }
    )
    proc = Processor(settings, consumer, conn, schedule)
    signal.signal(signal.SIGTERM, proc.stop)
    signal.signal(signal.SIGINT, proc.stop)
    proc.run()
