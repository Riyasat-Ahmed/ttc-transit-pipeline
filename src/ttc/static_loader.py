"""Download the static GTFS zip and load it into the schedule tables.

The whole swap runs in one transaction, so readers see either the old schedule
or the new one, never a half-loaded mix. Unchanged zips (same sha256) are skipped.
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import httpx
import psycopg

from ttc.config import Settings

log = logging.getLogger(__name__)

_SECS = (
    "(split_part({c}, ':', 1)::int * 3600 + split_part({c}, ':', 2)::int * 60"
    " + split_part({c}, ':', 3)::int)"
)
_DATE = "to_date({c}, 'YYYYMMDD')"

# target table -> (source file, [(target column, SQL expression over staging columns)])
# {name} in an expression is replaced with NULLIF(trim(stg."name"), '') or NULL if absent.
TABLES: dict[str, tuple[str, list[tuple[str, str]]]] = {
    "routes": ("routes.txt", [
        ("route_id", "{route_id}"),
        ("route_short_name", "{route_short_name}"),
        ("route_long_name", "{route_long_name}"),
        ("route_type", "{route_type}::int"),
        ("route_color", "{route_color}"),
        ("route_text_color", "{route_text_color}"),
    ]),
    "stops": ("stops.txt", [
        ("stop_id", "{stop_id}"),
        ("stop_code", "{stop_code}"),
        ("stop_name", "{stop_name}"),
        ("stop_lat", "{stop_lat}::float8"),
        ("stop_lon", "{stop_lon}::float8"),
    ]),
    "trips": ("trips.txt", [
        ("trip_id", "{trip_id}"),
        ("route_id", "{route_id}"),
        ("service_id", "{service_id}"),
        ("trip_headsign", "{trip_headsign}"),
        ("direction_id", "{direction_id}::int"),
        ("block_id", "{block_id}"),
        ("shape_id", "{shape_id}"),
    ]),
    "stop_times": ("stop_times.txt", [
        ("trip_id", "{trip_id}"),
        ("stop_sequence", "{stop_sequence}::int"),
        ("stop_id", "{stop_id}"),
        ("arrival_s", _SECS.format(c="coalesce({arrival_time}, {departure_time})")),
        ("departure_s", _SECS.format(c="coalesce({departure_time}, {arrival_time})")),
        ("timepoint", "{timepoint}::smallint"),
    ]),
    "calendar": ("calendar.txt", [
        ("service_id", "{service_id}"),
        *[(d, f"{{{d}}}::int::boolean") for d in
          ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")],
        ("start_date", _DATE.format(c="{start_date}")),
        ("end_date", _DATE.format(c="{end_date}")),
    ]),
    "calendar_dates": ("calendar_dates.txt", [
        ("service_id", "{service_id}"),
        ("date", _DATE.format(c="{date}")),
        ("exception_type", "{exception_type}::smallint"),
    ]),
}

# Rows that can't produce a valid key are skipped rather than failing the load.
_FILTERS = {
    "stop_times": "coalesce({arrival_time}, {departure_time}) IS NOT NULL",
}

OPTIONAL_FILES = {"calendar.txt", "calendar_dates.txt"}


def download(url: str, dest: Path, user_agent: str, timeout: float = 120) -> str:
    sha = hashlib.sha256()
    with httpx.stream("GET", url, timeout=timeout, follow_redirects=True,
                      headers={"User-Agent": user_agent}) as resp:
        resp.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in resp.iter_bytes(1 << 20):
                sha.update(chunk)
                fh.write(chunk)
    return sha.hexdigest()


def _clean_header(name: str) -> str:
    return name.strip().lstrip("﻿").strip('"')


def _expr(template: str, columns: set[str]) -> str:
    class Cols(dict):
        def __missing__(self, key):
            return f"NULLIF(trim(stg.\"{key}\"), '')" if key in columns else "NULL"
    return template.format_map(Cols())


def load_member(cur: psycopg.Cursor, zf: zipfile.ZipFile, member: str, table: str) -> int:
    _, spec = TABLES[table]
    with zf.open(member) as raw:
        header_line = io.TextIOWrapper(raw, encoding="utf-8-sig").readline()
    columns = [_clean_header(c) for c in next(csv.reader([header_line]))]
    colset = set(columns)
    cur.execute("DROP TABLE IF EXISTS stg")
    cur.execute(
        "CREATE TEMP TABLE stg (" + ", ".join(f'"{c}" text' for c in columns) + ") ON COMMIT DROP"
    )
    copy_sql = "COPY stg FROM STDIN WITH (FORMAT csv, HEADER true)"
    with zf.open(member) as raw, cur.copy(copy_sql) as cp:
        while chunk := raw.read(1 << 20):
            cp.write(chunk)
    targets = ", ".join(t for t, _ in spec)
    exprs = ", ".join(_expr(e, colset) for _, e in spec)
    where = _expr(_FILTERS[table], colset) if table in _FILTERS else "true"
    cur.execute(f"TRUNCATE {table}")
    cur.execute(
        f"INSERT INTO {table} ({targets}) SELECT {exprs} FROM stg WHERE {where} "
        "ON CONFLICT DO NOTHING"
    )
    n = cur.rowcount
    cur.execute("DROP TABLE stg")
    return n


def load_zip(conn: psycopg.Connection, zip_path: Path, sha256: str, source_url: str) -> dict:
    counts: dict[str, int] = {}
    with zipfile.ZipFile(zip_path) as zf:
        names = {Path(n).name: n for n in zf.namelist()}
        with conn.transaction(), conn.cursor() as cur:
            for table, (fname, _) in TABLES.items():
                if fname not in names:
                    if fname in OPTIONAL_FILES:
                        cur.execute(f"TRUNCATE {table}")
                        counts[table] = 0
                        continue
                    raise FileNotFoundError(f"{fname} missing from GTFS zip")
                counts[table] = load_member(cur, zf, names[fname], table)
                log.info("loaded table", extra={"table": table, "rows": counts[table]})
            cur.execute(
                """
                INSERT INTO gtfs_meta (id, sha256, source_url, loaded_at, n_trips, n_stop_times)
                VALUES (1, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET sha256 = EXCLUDED.sha256,
                    source_url = EXCLUDED.source_url, loaded_at = EXCLUDED.loaded_at,
                    n_trips = EXCLUDED.n_trips, n_stop_times = EXCLUDED.n_stop_times
                """,
                (sha256, source_url, datetime.now(UTC), counts["trips"], counts["stop_times"]),
            )
    conn.execute("ANALYZE trips; ANALYZE stop_times;")
    conn.commit()
    return counts


def run(settings: Settings, conn: psycopg.Connection, force: bool = False) -> bool:
    """Returns True if a new schedule was loaded."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "gtfs.zip"
        log.info("downloading static GTFS", extra={"url": settings.static_gtfs_url})
        sha = download(settings.static_gtfs_url, path, settings.feed_user_agent)
        row = conn.execute("SELECT sha256 FROM gtfs_meta").fetchone()
        conn.commit()
        if row and row[0] == sha and not force:
            log.info("static GTFS unchanged; skipping", extra={"sha256": sha})
            return False
        counts = load_zip(conn, path, sha, settings.static_gtfs_url)
        log.info("static GTFS loaded", extra={"sha256": sha, **counts})
        return True
