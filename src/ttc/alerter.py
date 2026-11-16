"""Discord alerts when a feed is stale or the pipeline stops (F16).

Alerts fire once on the transition to unhealthy and once on recovery. The
optional healthcheck ping is a dead-man's switch for when the whole server
(including this alerter) is down: point it at e.g. healthchecks.io.
"""

from __future__ import annotations

import contextlib
import logging
import time

import httpx
import psycopg

from ttc import db
from ttc.config import Settings

log = logging.getLogger(__name__)

CHECKS_SQL = """
SELECT 'feed:' || f.feed AS check,
       extract(epoch FROM now() - max(f.feed_ts))::int AS age_s
FROM feed_health f WHERE f.ts > now() - interval '1 day' AND f.ok
GROUP BY f.feed
UNION ALL
SELECT 'producer', extract(epoch FROM now() - max(ts))::int
FROM feed_health WHERE ts > now() - interval '1 day'
UNION ALL
SELECT 'processor', extract(epoch FROM now() - max(ts))::int
FROM processor_stats WHERE ts > now() - interval '1 day'
"""

EXPECTED = ("feed:vehicles", "feed:trips", "producer", "processor")

DESCRIPTIONS = {
    "feed:vehicles": "Vehicle positions feed has not published a new snapshot",
    "feed:trips": "Trip updates feed has not published a new snapshot",
    "producer": "Producer has not sent a heartbeat",
    "processor": "Processor has not written a batch",
}


def evaluate(rows: list[tuple[str, int | None]], threshold_s: int) -> dict[str, int | None]:
    """Return {check: age_s} for every unhealthy check (age None = never seen)."""
    ages = dict(rows)
    bad = {}
    for check in EXPECTED:
        age = ages.get(check)
        if age is None or age > threshold_s:
            bad[check] = age
    return bad


def post_discord(url: str, content: str) -> None:
    if not url:
        log.warning("alert (no DISCORD_WEBHOOK_URL set)", extra={"content": content})
        return
    try:
        httpx.post(url, json={"content": content}, timeout=10).raise_for_status()
    except httpx.HTTPError as exc:
        log.error("discord post failed", extra={"error": str(exc)})


def main(settings: Settings) -> None:
    conn = db.connect(settings.database_url, autocommit=True)
    firing: set[str] = set()
    # Give the pipeline time to produce its first data after a cold start.
    time.sleep(settings.stale_alert_after_s)
    while True:
        try:
            rows = conn.execute(CHECKS_SQL).fetchall()
        except psycopg.OperationalError as exc:
            log.error("alerter db error", extra={"error": str(exc)})
            if "database" not in firing:
                post_discord(
                    settings.discord_webhook_url,
                    f":rotating_light: **TTC pipeline**: database unreachable ({exc})",
                )
                firing.add("database")
            time.sleep(settings.alert_check_interval_s)
            conn = db.connect(settings.database_url, autocommit=True)
            continue
        bad = evaluate(rows, settings.stale_alert_after_s)
        for check in set(bad) - firing:
            age = bad[check]
            age_txt = "ever" if age is None else f"{age // 60} min"
            post_discord(
                settings.discord_webhook_url,
                f":rotating_light: **TTC pipeline**: {DESCRIPTIONS[check]} for {age_txt}.",
            )
        for check in firing - set(bad):
            post_discord(settings.discord_webhook_url,
                         f":white_check_mark: **TTC pipeline**: {check} recovered.")
        firing = set(bad)
        if not firing and settings.healthcheck_ping_url:
            with contextlib.suppress(httpx.HTTPError):
                httpx.get(settings.healthcheck_ping_url, timeout=10)
        time.sleep(settings.alert_check_interval_s)
