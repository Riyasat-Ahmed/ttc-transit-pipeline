"""Daily jobs: refresh the static schedule and compute on-time performance (F15)."""

from __future__ import annotations

import logging
import time
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg

from ttc import static_loader
from ttc.config import Settings

log = logging.getLogger(__name__)

# "On time" = between 1 minute early and 5 minutes late.
EARLY_S = -60
LATE_S = 300

OTP_SQL = """
INSERT INTO route_otp_daily (service_date, route_id, n, early, on_time, late, otp_pct, avg_delay_s)
SELECT
    service_date,
    route_id,
    count(*),
    count(*) FILTER (WHERE delay_s < %(early)s),
    count(*) FILTER (WHERE delay_s BETWEEN %(early)s AND %(late)s),
    count(*) FILTER (WHERE delay_s > %(late)s),
    100.0 * count(*) FILTER (WHERE delay_s BETWEEN %(early)s AND %(late)s) / count(*),
    avg(delay_s)
FROM stop_delays
WHERE service_date = %(day)s
  AND scheduled_at >= %(lo)s AND scheduled_at < %(hi)s  -- lets Timescale prune chunks
GROUP BY service_date, route_id
ON CONFLICT (service_date, route_id) DO UPDATE SET
    n = EXCLUDED.n, early = EXCLUDED.early, on_time = EXCLUDED.on_time, late = EXCLUDED.late,
    otp_pct = EXCLUDED.otp_pct, avg_delay_s = EXCLUDED.avg_delay_s, computed_at = now()
"""


def compute_otp(conn: psycopg.Connection, day: date) -> int:
    lo = datetime(day.year, day.month, day.day, tzinfo=UTC) - timedelta(hours=12)
    hi = lo + timedelta(days=2, hours=12)
    with conn.transaction():
        cur = conn.execute(
            OTP_SQL,
            {"early": EARLY_S, "late": LATE_S, "day": day, "lo": lo, "hi": hi},
        )
        return cur.rowcount


def run_daily(settings: Settings, conn: psycopg.Connection) -> None:
    try:
        static_loader.run(settings, conn)
    except Exception:
        log.exception("static GTFS refresh failed; keeping the current schedule")
        conn.rollback()
    today = datetime.now(ZoneInfo(settings.timezone)).date()
    # Recompute the last week (cheap, and it fills gaps after downtime).
    for back in range(1, 8):
        day = today - timedelta(days=back)
        n = compute_otp(conn, day)
        log.info("on-time performance computed", extra={"service_date": str(day), "routes": n})
    with conn.transaction():
        conn.execute("DELETE FROM bad_messages WHERE received_at < now() - interval '14 days'")


def main(settings: Settings) -> None:
    from ttc import db

    conn = db.connect(settings.database_url)
    while True:
        started = time.monotonic()
        try:
            run_daily(settings, conn)
        except psycopg.OperationalError:
            log.exception("database error in daily job; reconnecting and retrying in 60 s")
            conn = db.connect(settings.database_url)
            time.sleep(60)
            continue
        sleep_s = settings.static_refresh_hours * 3600 - (time.monotonic() - started)
        log.info("daily job done", extra={"next_run_in_s": int(sleep_s)})
        time.sleep(max(60, sleep_s))
