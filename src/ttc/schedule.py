"""In-memory view of the static schedule, refreshed when the loader swaps it."""

from __future__ import annotations

import logging
import time
from collections import OrderedDict
from collections.abc import Iterable

import psycopg

from ttc.delay import ScheduledTrip, ServiceCalendar

log = logging.getLogger(__name__)


class ScheduleCache:
    def __init__(
        self, conn: psycopg.Connection, max_trips: int = 20_000, check_every_s: float = 60
    ):
        self.conn = conn
        self.max_trips = max_trips
        self.check_every_s = check_every_s
        self.version: object = None
        self.trip_ids: set[str] = set()
        self.calendar = ServiceCalendar()
        self._trips: OrderedDict[str, ScheduledTrip] = OrderedDict()
        self._last_check = 0.0

    @property
    def loaded(self) -> bool:
        return bool(self.trip_ids)

    def refresh_if_changed(self, force: bool = False) -> bool:
        now = time.monotonic()
        if not force and now - self._last_check < self.check_every_s:
            return False
        self._last_check = now
        row = self.conn.execute("SELECT loaded_at FROM gtfs_meta").fetchone()
        self.conn.commit()
        version = row[0] if row else None
        if version == self.version:
            return False
        self.version = version
        self._trips.clear()
        self.trip_ids = {r[0] for r in self.conn.execute("SELECT trip_id FROM trips")}
        cal_rows = self.conn.execute(
            "SELECT service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, "
            "start_date, end_date FROM calendar"
        ).fetchall()
        date_rows = self.conn.execute(
            "SELECT service_id, date, exception_type FROM calendar_dates"
        ).fetchall()
        self.conn.commit()
        self.calendar = ServiceCalendar.from_rows(
            ((r[0], tuple(r[1:8]), r[8], r[9]) for r in cal_rows),
            ((r[0], r[1], r[2]) for r in date_rows),
        )
        log.info("schedule loaded", extra={"version": str(version), "trips": len(self.trip_ids)})
        return True

    def get_trips(self, trip_ids: Iterable[str]) -> dict[str, ScheduledTrip]:
        wanted = {t for t in trip_ids if t in self.trip_ids}
        missing = [t for t in wanted if t not in self._trips]
        if missing:
            rows = self.conn.execute(
                """
                SELECT t.trip_id, t.route_id, t.service_id, st.stop_sequence, st.stop_id,
                       st.arrival_s
                FROM trips t JOIN stop_times st USING (trip_id)
                WHERE t.trip_id = ANY(%s)
                """,
                (missing,),
            ).fetchall()
            self.conn.commit()
            grouped: dict[str, tuple[str, str, dict[int, tuple[str, int]]]] = {}
            for trip_id, route_id, service_id, seq, stop_id, arr in rows:
                grouped.setdefault(trip_id, (route_id, service_id, {}))[2][seq] = (stop_id, arr)
            for trip_id, (route_id, service_id, stops) in grouped.items():
                self._trips[trip_id] = ScheduledTrip(trip_id, route_id, service_id, stops)
        out = {}
        for t in wanted:
            trip = self._trips.get(t)
            if trip is not None:
                self._trips.move_to_end(t)
                out[t] = trip
        while len(self._trips) > self.max_trips:
            self._trips.popitem(last=False)
        return out
