"""Connections, migrations and idempotent batch upserts."""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from ttc.delay import StopDelay
from ttc.models import Heartbeat, VehiclePositionEvent

log = logging.getLogger(__name__)

MIGRATIONS_DIR = Path("migrations")


def connect(url: str, retries: int = 30, autocommit: bool = False) -> psycopg.Connection:
    for attempt in range(1, retries + 1):
        try:
            return psycopg.connect(url, autocommit=autocommit)
        except psycopg.OperationalError as exc:
            if attempt == retries:
                raise
            log.warning("database not ready", extra={"attempt": attempt, "error": str(exc)})
            time.sleep(min(2 * attempt, 10))
    raise RuntimeError("unreachable")


def migrate(conn: psycopg.Connection, directory: Path = MIGRATIONS_DIR) -> list[str]:
    """Apply *.sql files in order, once each, recording them in schema_migrations."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations "
        "(version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
    )
    conn.commit()
    done = {r[0] for r in conn.execute("SELECT version FROM schema_migrations")}
    applied = []
    for path in sorted(directory.glob("*.sql")):
        if path.name in done:
            continue
        log.info("applying migration", extra={"migration": path.name})
        with conn.transaction():
            conn.execute(path.read_text())
            conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (path.name,))
        applied.append(path.name)
    conn.commit()
    return applied


def _ts(epoch: float | int | None) -> datetime | None:
    return None if epoch is None else datetime.fromtimestamp(epoch, UTC)


def insert_vehicle_positions(
    cur: psycopg.Cursor, rows: Sequence[tuple[VehiclePositionEvent, bool]]
) -> int:
    """Insert positions; returns rows actually inserted (the rest were duplicates)."""
    if not rows:
        return 0
    cols = list(zip(*[
        (
            _ts(e.ts), e.vehicle_id, e.trip_id, e.route_id, e.lat, e.lon, e.bearing, e.speed,
            e.stop_id, e.current_status, e.occupancy, _ts(e.feed_ts), matched,
        )
        for e, matched in rows
    ], strict=True))
    cur.execute(
        """
        INSERT INTO vehicle_positions (ts, vehicle_id, trip_id, route_id, lat, lon, bearing,
            speed, stop_id, current_status, occupancy, feed_ts, trip_matched)
        SELECT * FROM unnest(
            %s::timestamptz[], %s::text[], %s::text[], %s::text[], %s::float8[], %s::float8[],
            %s::real[], %s::real[], %s::text[], %s::text[], %s::text[], %s::timestamptz[],
            %s::boolean[])
        ON CONFLICT (vehicle_id, ts) DO NOTHING
        RETURNING 1
        """,
        [list(c) for c in cols],
    )
    return len(cur.fetchall())


def dedupe_stop_delays(rows: Sequence[StopDelay]) -> list[StopDelay]:
    """Keep the freshest row per key (Postgres can't upsert one key twice per statement)."""
    best: dict[tuple, StopDelay] = {}
    for r in rows:
        k = (r.trip_id, r.service_date, r.stop_sequence)
        cur = best.get(k)
        if cur is None or r.feed_ts > cur.feed_ts:
            best[k] = r
    return list(best.values())


def upsert_stop_delays(cur: psycopg.Cursor, rows: Sequence[StopDelay]) -> tuple[int, int]:
    """Upsert observed-arrival rows. Returns (inserted, updated); the rest were stale/duplicate.

    A row is only overwritten by a strictly newer feed snapshot, so replays and
    out-of-order delivery can never move a row backwards.
    """
    if not rows:
        return 0, 0
    cols = list(zip(*[
        (
            r.scheduled_at, r.trip_id, r.service_date, r.stop_sequence, r.stop_id, r.route_id,
            r.predicted_at, r.delay_s, r.observed, r.feed_ts,
        )
        for r in rows
    ], strict=True))
    cur.execute(
        """
        INSERT INTO stop_delays AS d (scheduled_at, trip_id, service_date, stop_sequence,
            stop_id, route_id, predicted_at, delay_s, observed, feed_ts)
        SELECT * FROM unnest(
            %s::timestamptz[], %s::text[], %s::date[], %s::int[], %s::text[], %s::text[],
            %s::timestamptz[], %s::int[], %s::boolean[], %s::timestamptz[])
        ON CONFLICT (trip_id, service_date, stop_sequence, scheduled_at) DO UPDATE SET
            predicted_at = EXCLUDED.predicted_at,
            delay_s = EXCLUDED.delay_s,
            observed = EXCLUDED.observed,
            feed_ts = EXCLUDED.feed_ts,
            updated_at = now()
        WHERE EXCLUDED.feed_ts > d.feed_ts
        RETURNING (xmax = 0) AS inserted
        """,
        [list(c) for c in cols],
    )
    results = [r[0] for r in cur.fetchall()]
    inserted = sum(results)
    return inserted, len(results) - inserted


def insert_heartbeats(cur: psycopg.Cursor, beats: Sequence[Heartbeat]) -> int:
    if not beats:
        return 0
    cur.executemany(
        """
        INSERT INTO feed_health (ts, feed, ok, entity_count, feed_ts, new_snapshot,
            http_status, latency_ms, error)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (feed, ts) DO NOTHING
        """,
        [
            (
                _ts(b.polled_at), b.feed, b.ok, b.entity_count, _ts(b.feed_ts), b.new_snapshot,
                b.http_status, b.latency_ms, b.error,
            )
            for b in beats
        ],
    )
    return len(beats)


def insert_bad_messages(cur: psycopg.Cursor, bad: Sequence[tuple[str, int, int, str, str]]) -> None:
    if not bad:
        return
    cur.executemany(
        """
        INSERT INTO bad_messages (topic, partition, "offset", error, payload)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        bad,
    )


def insert_stats(cur: psycopg.Cursor, stats: Sequence[dict]) -> None:
    for s in stats:
        cols = ", ".join(s)
        params = ", ".join(["%s"] * len(s))
        cur.execute(f"INSERT INTO processor_stats ({cols}) VALUES ({params})", list(s.values()))
