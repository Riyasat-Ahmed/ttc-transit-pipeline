from __future__ import annotations

import statistics
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from conftest import read_snapshot

from ttc import feeds
from ttc.delay import (
    ScheduledTrip,
    ServiceCalendar,
    choose_service_date,
    compute_delays,
    parse_gtfs_time,
    scheduled_datetime,
)
from ttc.models import StopTimeEvent, TripUpdateEvent

TZ = ZoneInfo("America/Toronto")
ALWAYS = ServiceCalendar.from_rows(
    [("wk", (True,) * 7, date(2026, 1, 1), date(2027, 12, 31))], []
)


def epoch(y, mo, d, h, mi, s=0) -> int:
    return int(datetime(y, mo, d, h, mi, s, tzinfo=TZ).timestamp())


def test_parse_gtfs_time_past_midnight():
    assert parse_gtfs_time("00:00:00") == 0
    assert parse_gtfs_time("07:43:37") == 7 * 3600 + 43 * 60 + 37
    assert parse_gtfs_time("25:10:00") == 25 * 3600 + 600


def test_scheduled_datetime_normal_day():
    got = scheduled_datetime(date(2026, 11, 16), parse_gtfs_time("17:00:00"), TZ)
    assert got == datetime(2026, 11, 16, 17, 0, tzinfo=TZ)


def test_scheduled_datetime_dst_end():
    # 2026-11-01: clocks go back at 02:00. GTFS times count from noon minus 12h,
    # which is 01:00 EDT == 05:00 UTC that day, so "17:00:00" lands at 16:00 local.
    got = scheduled_datetime(date(2026, 11, 1), parse_gtfs_time("17:00:00"), TZ)
    assert got.astimezone(UTC) == datetime(2026, 11, 1, 21, 0, tzinfo=UTC)


def test_after_midnight_trip_uses_previous_service_day():
    # 01:15 local on the 17th, trip scheduled at 25:10 on the 16th's service day.
    feed_ts = epoch(2026, 11, 17, 1, 15)
    predicted = epoch(2026, 11, 17, 1, 13)
    day = choose_service_date(feed_ts, predicted, parse_gtfs_time("25:10:00"), TZ, lambda d: True)
    assert day == date(2026, 11, 16)


def test_service_day_respects_calendar():
    # Weekday-only service; the candidate days are Sunday 15th and Monday 16th.
    cal = ServiceCalendar.from_rows(
        [("wkday", (True,) * 5 + (False, False), date(2026, 1, 1), date(2027, 1, 1))], []
    )
    feed_ts = epoch(2026, 11, 16, 0, 30)
    predicted = epoch(2026, 11, 16, 0, 35)
    day = choose_service_date(
        feed_ts, predicted, parse_gtfs_time("24:30:00"), TZ,
        lambda d: cal.is_active("wkday", d),
    )
    assert day == date(2026, 11, 16)  # Sunday has no weekday service


def test_calendar_exceptions():
    cal = ServiceCalendar.from_rows(
        [("s", (True,) * 7, date(2026, 11, 1), date(2026, 11, 30))],
        [("s", date(2026, 11, 11), 2), ("s", date(2026, 12, 1), 1)],
    )
    assert cal.is_active("s", date(2026, 11, 10))
    assert not cal.is_active("s", date(2026, 11, 11))
    assert cal.is_active("s", date(2026, 12, 1))
    assert not cal.is_active("s", date(2026, 12, 2))
    assert not cal.is_active("unknown", date(2026, 11, 10))


def _trip_event(feed_ts: int, stops: list[tuple[int, str, int]]) -> TripUpdateEvent:
    return TripUpdateEvent(
        event_key=f"{feed_ts}:T1", feed_ts=feed_ts, trip_id="T1", route_id="504",
        stops=[StopTimeEvent(stop_sequence=s, stop_id=sid, arrival_time=t) for s, sid, t in stops],
    )


TRIP = ScheduledTrip("T1", "504", "wk", {
    1: ("A", parse_gtfs_time("17:00:00")),
    2: ("B", parse_gtfs_time("17:05:00")),
    3: ("C", parse_gtfs_time("17:10:00")),
    4: ("D", parse_gtfs_time("17:40:00")),
})


def test_compute_delays_basic():
    feed_ts = epoch(2026, 11, 16, 17, 8)
    ev = _trip_event(feed_ts, [
        (1, "A", epoch(2026, 11, 16, 17, 3)),    # passed, 3 min late
        (2, "B", epoch(2026, 11, 16, 17, 7, 30)),  # passed, 2.5 min late
        (3, "C", epoch(2026, 11, 16, 17, 9)),    # due in 1 min (inside horizon), 1 min early
        (4, "D", epoch(2026, 11, 16, 17, 42)),   # far future: skipped
    ])
    res = compute_delays(ev, TRIP, ALWAYS, TZ, horizon_s=120, max_abs_delay_s=7200)
    assert res.service_date == date(2026, 11, 16)
    got = {d.stop_sequence: (d.delay_s, d.observed) for d in res.delays}
    assert got == {1: (180, True), 2: (150, True), 3: (-60, False)}


def test_compute_delays_skips_mismatched_stop_and_outliers():
    feed_ts = epoch(2026, 11, 16, 17, 8)
    ev = _trip_event(feed_ts, [
        (1, "A", epoch(2026, 11, 16, 17, 1)),
        (2, "WRONG", epoch(2026, 11, 16, 17, 6)),
        (9, "Z", epoch(2026, 11, 16, 17, 6)),
    ])
    res = compute_delays(ev, TRIP, ALWAYS, TZ, horizon_s=120, max_abs_delay_s=7200)
    assert [d.stop_sequence for d in res.delays] == [1]
    assert res.stop_mismatches == 2

    res = compute_delays(ev, TRIP, ALWAYS, TZ, horizon_s=120, max_abs_delay_s=30)
    assert res.delays == [] and res.skipped_outliers == 1


def test_real_snapshot_delays_are_plausible(schedule, settings):
    """End to end on a real feed snapshot + the matching static schedule subset."""
    events = feeds.trip_update_events(feeds.decode(read_snapshot("trips")))
    scheduled = [e for e in events if e.schedule_relationship == "SCHEDULED"]
    trips = schedule.get_trips(e.trip_id for e in scheduled)
    assert len(trips) / len(scheduled) > 0.95, "realtime trip ids should match the bustime GTFS"

    delays = []
    for e in scheduled:
        if e.trip_id in trips:
            res = compute_delays(e, trips[e.trip_id], schedule.calendar, TZ,
                                 settings.prediction_horizon_s, settings.max_abs_delay_s)
            delays += [d.delay_s for d in res.delays]
    assert len(delays) > 1000
    median = statistics.median(delays)
    assert -300 < median < 600, f"median delay {median}s is implausible"
    assert all(abs(d) <= settings.max_abs_delay_s for d in delays)


def test_horizon_controls_volume(schedule, settings):
    events = feeds.trip_update_events(feeds.decode(read_snapshot("trips")))
    e = next(
        e for e in events
        if e.trip_id in schedule.trip_ids and sum(s.event_time is not None for s in e.stops) > 10
    )
    trip = schedule.get_trips([e.trip_id])[e.trip_id]
    near = compute_delays(e, trip, schedule.calendar, TZ, 120, 7200).delays
    far = compute_delays(e, trip, schedule.calendar, TZ, 10**9, 7200).delays
    assert len(near) < len(far)
    assert all(d.predicted_at <= d.feed_ts + timedelta(seconds=120) for d in near)
