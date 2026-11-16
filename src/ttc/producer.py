"""Poll the TTC GTFS-RT feeds and publish one event per entity to Redpanda."""

from __future__ import annotations

import logging
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass

import httpx
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import BaseModel

from ttc import feeds
from ttc.config import Settings
from ttc.models import Heartbeat

log = logging.getLogger(__name__)


@dataclass
class FeedSpec:
    name: str
    url: str
    topic: str
    extract: Callable[..., list[BaseModel]]
    key_field: str = "event_key"


def ensure_topics(settings: Settings, timeout: float = 60) -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    names = [
        settings.topic_vehicle_positions, settings.topic_trip_updates, settings.topic_heartbeats,
    ]
    deadline = time.monotonic() + timeout
    while True:
        try:
            existing = set(admin.list_topics(timeout=10).topics)
            break
        except KafkaException as exc:
            if time.monotonic() > deadline:
                raise
            log.warning("broker not ready", extra={"error": str(exc)})
            time.sleep(3)
    new = [
        NewTopic(
            n,
            num_partitions=settings.topic_partitions,
            replication_factor=1,
            config={"retention.ms": str(settings.topic_retention_ms)},
        )
        for n in names
        if n not in existing
    ]
    for name, fut in (admin.create_topics(new).items() if new else []):
        try:
            fut.result()
            log.info("created topic", extra={"topic": name})
        except KafkaException as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


class FeedPoller:
    def __init__(self, settings: Settings, producer: Producer, client: httpx.Client):
        self.settings = settings
        self.producer = producer
        self.client = client
        self.last_feed_ts: dict[str, int] = {}
        self.delivery_failures = 0
        self.running = True
        self.specs = [
            FeedSpec("vehicles", settings.vehicles_url, settings.topic_vehicle_positions,
                     feeds.vehicle_events),
            FeedSpec("trips", settings.trips_url, settings.topic_trip_updates,
                     feeds.trip_update_events),
        ]

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.delivery_failures += 1
            log.error("delivery failed", extra={"topic": msg.topic(), "error": str(err)})

    def _publish(self, topic: str, key: str, value: BaseModel) -> None:
        while True:
            try:
                self.producer.produce(
                    topic, key=key.encode(), value=value.model_dump_json().encode(),
                    on_delivery=self._on_delivery,
                )
                return
            except BufferError:  # local queue full: let it drain
                self.producer.poll(0.5)

    def poll_feed(self, spec: FeedSpec) -> Heartbeat:
        polled_at = time.time()
        t0 = time.monotonic()
        status = None
        try:
            resp = feeds.fetch(self.client, spec.url)
            status = resp.status_code
            feed = feeds.decode(resp.content)
            feed_ts = int(feed.header.timestamp)
            new = feed_ts != self.last_feed_ts.get(spec.name)
            events = spec.extract(feed)
            if new:
                for e in events:
                    self._publish(spec.topic, e.event_key, e)
                self.last_feed_ts[spec.name] = feed_ts
            hb = Heartbeat(
                event_key=f"{spec.name}:{polled_at:.3f}", feed=spec.name, polled_at=polled_at,
                ok=True, entity_count=len(events), feed_ts=feed_ts, new_snapshot=new,
                http_status=status, latency_ms=int((time.monotonic() - t0) * 1000),
            )
        except (httpx.HTTPError, feeds.FeedError) as exc:
            if isinstance(exc, httpx.HTTPStatusError):
                status = exc.response.status_code
            hb = Heartbeat(
                event_key=f"{spec.name}:{polled_at:.3f}", feed=spec.name, polled_at=polled_at,
                ok=False, http_status=status, latency_ms=int((time.monotonic() - t0) * 1000),
                error=f"{type(exc).__name__}: {exc}"[:500],
            )
        self._publish(self.settings.topic_heartbeats, hb.event_key, hb)
        log.info("polled", extra=hb.model_dump(exclude={"event_key"}))
        return hb

    def run(self) -> None:
        s = self.settings
        failures = 0
        while self.running:
            start = time.monotonic()
            beats = [self.poll_feed(spec) for spec in self.specs]
            remaining = self.producer.flush(30)
            if remaining:
                log.error("messages still queued after flush", extra={"remaining": remaining})
            failures = 0 if all(b.ok for b in beats) else failures + 1
            # Back off exponentially on errors; never poll faster than poll_interval_s.
            interval = min(s.poll_interval_s * (2**failures), s.max_backoff_s)
            wait = max(s.poll_interval_s, interval) - (time.monotonic() - start)
            end = time.monotonic() + max(0.0, wait)
            while self.running and time.monotonic() < end:
                time.sleep(0.5)
        self.producer.flush(30)

    def stop(self, *_):
        self.running = False


def main(settings: Settings) -> None:
    ensure_topics(settings)
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            "compression.type": "zstd",
        }
    )
    client = httpx.Client(
        timeout=settings.http_timeout_s,
        headers={"User-Agent": settings.feed_user_agent},
        follow_redirects=True,
    )
    poller = FeedPoller(settings, producer, client)
    signal.signal(signal.SIGTERM, poller.stop)
    signal.signal(signal.SIGINT, poller.stop)
    log.info("producer started", extra={"interval_s": settings.poll_interval_s})
    poller.run()
