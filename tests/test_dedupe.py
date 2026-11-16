from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta

import pytest
from conftest import read_snapshot

from ttc import db, feeds
from ttc.delay import StopDelay
from ttc.processor import RawMessage, build_batch


def _raw(settings, n=1):
    msgs = []
    off = 0
    for kind, topic, extract in (
        ("vehicles", settings.topic_vehicle_positions, feeds.vehicle_events),
        ("trips", settings.topic_trip_updates, feeds.trip_update_events),
    ):
        for e in extract(feeds.decode(read_snapshot(kind, n))):
            msgs.append(RawMessage(topic, 0, off, e.model_dump_json().encode()))
            off += 1
    return msgs


def _row(feed_offset_s: int, delay: int) -> StopDelay:
    base = datetime(2026, 11, 16, 22, 0, tzinfo=UTC)
    return StopDelay(
        trip_id="T1", service_date=date(2026, 11, 16), stop_sequence=3, stop_id="C",
        route_id="504", scheduled_at=base, predicted_at=base + timedelta(seconds=delay),
        delay_s=delay, feed_ts=base + timedelta(seconds=feed_offset_s), observed=False,
    )


def test_dedupe_keeps_freshest_row():
    rows = [_row(0, 60), _row(40, 120), _row(20, 90)]
    out = db.dedupe_stop_delays(rows)
    assert len(out) == 1 and out[0].delay_s == 120


def test_replayed_batch_produces_identical_rows(schedule, settings):
    once = build_batch(_raw(settings), schedule, settings)
    twice = build_batch(_raw(settings) * 2, schedule, settings)
    assert len(once.stop_delays) > 0
    assert sorted(once.stop_delays, key=str) == sorted(twice.stop_delays, key=str)
    # vehicle duplicates are left for the DB unique key; both copies are identical
    assert len(twice.vehicles) == 2 * len(once.vehicles)


def test_malformed_message_is_quarantined_not_fatal(schedule, settings):
    msgs = _raw(settings)[:50] + [
        RawMessage(settings.topic_vehicle_positions, 0, 9998, b"{not json"),
        RawMessage(settings.topic_vehicle_positions, 0, 9999, b'{"event_key": "x", "lat": 999}'),
        RawMessage(settings.topic_trip_updates, 1, 5, None),
    ]
    batch = build_batch(msgs, schedule, settings)
    assert len(batch.bad) == 3
    assert batch.stats[settings.topic_vehicle_positions]["malformed"] == 2
    assert len(batch.vehicles) == 50


def test_match_rate_on_fixture(schedule, settings):
    batch = build_batch(_raw(settings), schedule, settings)
    tu = batch.stats[settings.topic_trip_updates]
    rate = tu["matched_trips"] / (tu["matched_trips"] + tu["unmatched_trips"])
    assert rate > 0.95


# ------------------------------------------------------------------ database
needs_db = pytest.mark.skipif(not os.environ.get("DATABASE_URL"), reason="DATABASE_URL not set")


@pytest.fixture
def conn():
    c = db.connect(os.environ["DATABASE_URL"], retries=3)
    db.migrate(c)
    c.execute("TRUNCATE stop_delays, vehicle_positions")
    c.commit()
    yield c
    c.rollback()
    c.execute("TRUNCATE stop_delays, vehicle_positions")
    c.commit()
    c.close()


@pytest.mark.db
@needs_db
def test_stop_delay_upsert_is_idempotent(conn):
    with conn.cursor() as cur:
        assert db.upsert_stop_delays(cur, [_row(0, 60)]) == (1, 0)
        assert db.upsert_stop_delays(cur, [_row(0, 60)]) == (0, 0), "replay must be a no-op"
        assert db.upsert_stop_delays(cur, [_row(30, 150)]) == (0, 1), "newer snapshot updates"
        assert db.upsert_stop_delays(cur, [_row(10, 999)]) == (0, 0), "older snapshot ignored"
        cur.execute("SELECT count(*), max(delay_s) FROM stop_delays")
        assert cur.fetchone() == (1, 150)


@pytest.mark.db
@needs_db
def test_vehicle_insert_is_idempotent(conn, settings):
    events = feeds.vehicle_events(feeds.decode(read_snapshot("vehicles")))
    rows = [(e, True) for e in events]
    with conn.cursor() as cur:
        first = db.insert_vehicle_positions(cur, rows)
        assert first > 0
        assert db.insert_vehicle_positions(cur, rows) == 0
        cur.execute("SELECT count(*) FROM vehicle_positions")
        assert cur.fetchone()[0] == first
