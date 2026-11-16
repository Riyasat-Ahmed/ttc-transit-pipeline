from __future__ import annotations

import pytest
from conftest import read_snapshot

from ttc import feeds
from ttc.models import TripUpdateEvent, VehiclePositionEvent


@pytest.mark.parametrize("n", range(1, 11))
def test_every_snapshot_decodes(n):
    for kind in ("vehicles", "trips"):
        feed = feeds.decode(read_snapshot(kind, n))
        assert feed.header.timestamp > 1_700_000_000
        assert len(feed.entity) > 100


def test_vehicle_events():
    events = feeds.vehicle_events(feeds.decode(read_snapshot("vehicles")))
    assert len(events) > 500
    keys = [e.event_key for e in events]
    assert len(keys) == len(set(keys)), "event keys must be unique within a snapshot"
    for e in events:
        assert 43.4 < e.lat < 44.1 and -79.9 < e.lon < -78.9, "vehicle outside Toronto"
        assert e.event_key == f"{e.feed_ts}:{e.vehicle_id}"
        assert e.ts <= e.feed_ts + 60
    # round-trips through the JSON wire format
    e = events[0]
    assert VehiclePositionEvent.model_validate_json(e.model_dump_json()) == e


def test_trip_update_events():
    events = feeds.trip_update_events(feeds.decode(read_snapshot("trips")))
    assert len(events) > 500
    scheduled = [e for e in events if e.schedule_relationship == "SCHEDULED"]
    assert len(scheduled) / len(events) > 0.9
    with_times = [e for e in scheduled if any(s.event_time for s in e.stops)]
    assert len(with_times) / len(scheduled) > 0.9, "most trips should carry predicted times"
    e = with_times[0]
    assert e.event_key == f"{e.feed_ts}:{e.trip_id}"
    assert TripUpdateEvent.model_validate_json(e.model_dump_json()) == e


def test_event_keys_stable_across_decodes():
    raw = read_snapshot("vehicles")
    a = [e.event_key for e in feeds.vehicle_events(feeds.decode(raw))]
    b = [e.event_key for e in feeds.vehicle_events(feeds.decode(raw))]
    assert a == b


@pytest.mark.parametrize("payload", [b"", b"not a protobuf \x00\xff\xfe", b"<html>502</html>"])
def test_garbage_raises_feed_error(payload):
    with pytest.raises(feeds.FeedError):
        feeds.decode(payload)
