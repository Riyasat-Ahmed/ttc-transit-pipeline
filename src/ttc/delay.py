"""Delay calculation: predicted/actual arrival minus scheduled arrival.

GTFS schedule times are seconds after "noon minus 12 hours" on the service day
(this is midnight except on DST-change days) and can exceed 24:00:00 for trips
that run past midnight. The TTC realtime feed does not send start_date, so the
service day is inferred: among yesterday and today (local time), pick the
active service day whose schedule is closest to the prediction.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from ttc.models import TripUpdateEvent


def parse_gtfs_time(value: str) -> int:
    """'25:10:30' -> seconds after the service day origin (may exceed 86400)."""
    h, m, s = value.strip().split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def service_day_origin(day: date, tz: ZoneInfo) -> datetime:
    noon = datetime(day.year, day.month, day.day, 12, tzinfo=tz)
    return (noon - timedelta(hours=12)).astimezone(UTC)


def scheduled_datetime(day: date, seconds: int, tz: ZoneInfo) -> datetime:
    return service_day_origin(day, tz) + timedelta(seconds=seconds)


@dataclass
class ServiceCalendar:
    """calendar.txt + calendar_dates.txt."""

    weekly: dict[str, tuple[tuple[bool, ...], date, date]] = field(default_factory=dict)
    added: set[tuple[str, date]] = field(default_factory=set)
    removed: set[tuple[str, date]] = field(default_factory=set)

    def is_active(self, service_id: str, day: date) -> bool:
        if (service_id, day) in self.removed:
            return False
        if (service_id, day) in self.added:
            return True
        rule = self.weekly.get(service_id)
        if rule is None:
            return False
        days, start, end = rule
        return start <= day <= end and days[day.weekday()]

    @classmethod
    def from_rows(
        cls,
        calendar: Iterable[tuple[str, tuple[bool, ...], date, date]],
        calendar_dates: Iterable[tuple[str, date, int]],
    ) -> ServiceCalendar:
        cal = cls()
        for service_id, days, start, end in calendar:
            cal.weekly[service_id] = (tuple(days), start, end)
        for service_id, day, exception_type in calendar_dates:
            (cal.added if exception_type == 1 else cal.removed).add((service_id, day))
        return cal


@dataclass(frozen=True)
class ScheduledTrip:
    trip_id: str
    route_id: str
    service_id: str
    # stop_sequence -> (stop_id, scheduled arrival seconds after service day origin)
    stops: dict[int, tuple[str, int]]


@dataclass(frozen=True)
class StopDelay:
    trip_id: str
    service_date: date
    stop_sequence: int
    stop_id: str
    route_id: str
    scheduled_at: datetime
    predicted_at: datetime
    delay_s: int
    feed_ts: datetime
    observed: bool


@dataclass
class DelayResult:
    delays: list[StopDelay]
    service_date: date | None
    skipped_outliers: int = 0
    stop_mismatches: int = 0


def choose_service_date(
    feed_ts: int,
    probe_time: int,
    probe_seconds: int,
    tz: ZoneInfo,
    is_active: Callable[[date], bool],
) -> date:
    """Pick the service day for a trip given one (predicted time, scheduled secs) pair."""
    local_day = datetime.fromtimestamp(feed_ts, tz).date()
    candidates = [local_day - timedelta(days=1), local_day]
    active = [d for d in candidates if is_active(d)] or candidates

    def gap(d: date) -> float:
        return abs(probe_time - scheduled_datetime(d, probe_seconds, tz).timestamp())

    return min(active, key=gap)


def compute_delays(
    event: TripUpdateEvent,
    trip: ScheduledTrip,
    calendar: ServiceCalendar,
    tz: ZoneInfo,
    horizon_s: int,
    max_abs_delay_s: int,
) -> DelayResult:
    pairs = []
    mismatches = 0
    for stu in event.stops:
        t = stu.event_time
        if t is None or stu.schedule_relationship in ("SKIPPED", "NO_DATA"):
            continue
        sched = trip.stops.get(stu.stop_sequence)
        if sched is None:
            mismatches += 1
            continue
        stop_id, seconds = sched
        if stu.stop_id and stu.stop_id != stop_id:
            mismatches += 1
            continue
        pairs.append((stu.stop_sequence, stop_id, seconds, t))

    if not pairs:
        return DelayResult([], None, stop_mismatches=mismatches)

    if event.start_date:
        service_date = datetime.strptime(event.start_date, "%Y%m%d").date()
    else:
        _, _, probe_secs, probe_t = pairs[0]
        service_date = choose_service_date(
            event.feed_ts,
            probe_t,
            probe_secs,
            tz,
            lambda d: calendar.is_active(trip.service_id, d),
        )

    origin = service_day_origin(service_date, tz)
    feed_dt = datetime.fromtimestamp(event.feed_ts, UTC)
    out: list[StopDelay] = []
    outliers = 0
    for seq, stop_id, seconds, t in pairs:
        if t > event.feed_ts + horizon_s:
            continue
        scheduled_at = origin + timedelta(seconds=seconds)
        delay = int(t - scheduled_at.timestamp())
        if abs(delay) > max_abs_delay_s:
            outliers += 1
            continue
        out.append(
            StopDelay(
                trip_id=trip.trip_id,
                service_date=service_date,
                stop_sequence=seq,
                stop_id=stop_id,
                route_id=trip.route_id,
                scheduled_at=scheduled_at,
                predicted_at=datetime.fromtimestamp(t, UTC),
                delay_s=delay,
                feed_ts=feed_dt,
                observed=t <= event.feed_ts,
            )
        )
    return DelayResult(out, service_date, skipped_outliers=outliers, stop_mismatches=mismatches)
