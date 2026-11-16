from __future__ import annotations

import csv
import gzip
import io
from datetime import datetime
from pathlib import Path

import pytest

from ttc.config import Settings
from ttc.delay import ScheduledTrip, ServiceCalendar, parse_gtfs_time

FIXTURES = Path(__file__).parent / "fixtures"
SNAPSHOTS = FIXTURES / "snapshots"
GTFS = FIXTURES / "gtfs_subset"


def read_snapshot(kind: str, n: int = 1) -> bytes:
    return gzip.decompress((SNAPSHOTS / f"{kind}_{n:02d}.pb.gz").read_bytes())


def _rows(name: str):
    with gzip.open(GTFS / f"{name}.txt.gz", "rt", newline="") as fh:
        yield from csv.DictReader(io.StringIO(fh.read()))


class FixtureSchedule:
    """In-memory stand-in for ScheduleCache built from the gtfs_subset fixture."""

    def __init__(self) -> None:
        trips = {r["trip_id"]: r for r in _rows("trips")}
        stops: dict[str, dict[int, tuple[str, int]]] = {}
        for r in _rows("stop_times"):
            stops.setdefault(r["trip_id"], {})[int(r["stop_sequence"])] = (
                r["stop_id"], parse_gtfs_time(r["arrival_time"] or r["departure_time"]),
            )
        self.trips = {
            t: ScheduledTrip(t, r["route_id"], r["service_id"], stops.get(t, {}))
            for t, r in trips.items()
        }
        self.trip_ids = set(self.trips)
        day = lambda s: datetime.strptime(s, "%Y%m%d").date()  # noqa: E731
        days = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
        self.calendar = ServiceCalendar.from_rows(
            ((r["service_id"], tuple(r[d] == "1" for d in days), day(r["start_date"]),
              day(r["end_date"])) for r in _rows("calendar")),
            ((r["service_id"], day(r["date"]), int(r["exception_type"]))
             for r in _rows("calendar_dates")),
        )

    def get_trips(self, trip_ids):
        return {t: self.trips[t] for t in trip_ids if t in self.trips}


@pytest.fixture(scope="session")
def schedule() -> FixtureSchedule:
    return FixtureSchedule()


@pytest.fixture
def settings() -> Settings:
    return Settings(_env_file=None)
