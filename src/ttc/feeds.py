"""Fetch and decode GTFS-Realtime protobuf feeds into event models."""

from __future__ import annotations

import httpx
from google.transit import gtfs_realtime_pb2 as rt

from ttc.models import (
    StopTimeEvent,
    TripUpdateEvent,
    VehiclePositionEvent,
    trip_event_key,
    vehicle_event_key,
)

_TRIP_REL = rt.TripDescriptor.ScheduleRelationship
_STOP_REL = rt.TripUpdate.StopTimeUpdate.ScheduleRelationship
_VSTATUS = rt.VehiclePosition.VehicleStopStatus
_OCC = rt.VehiclePosition.OccupancyStatus


class FeedError(Exception):
    pass


def fetch(client: httpx.Client, url: str) -> httpx.Response:
    resp = client.get(url)
    resp.raise_for_status()
    return resp


def decode(payload: bytes) -> rt.FeedMessage:
    feed = rt.FeedMessage()
    try:
        feed.ParseFromString(payload)
    except Exception as exc:  # protobuf raises DecodeError, but be defensive
        raise FeedError(f"protobuf decode failed: {exc}") from exc
    if not feed.HasField("header") or not feed.header.timestamp:
        raise FeedError("feed has no header timestamp")
    return feed


def _enum_name(enum, value: int) -> str:
    try:
        return enum.Name(value)
    except ValueError:
        return str(value)


def vehicle_events(feed: rt.FeedMessage) -> list[VehiclePositionEvent]:
    feed_ts = int(feed.header.timestamp)
    events: list[VehiclePositionEvent] = []
    for ent in feed.entity:
        if not ent.HasField("vehicle"):
            continue
        v = ent.vehicle
        if not v.HasField("position"):
            continue
        vehicle_id = v.vehicle.id or ent.id
        p = v.position
        events.append(
            VehiclePositionEvent(
                event_key=vehicle_event_key(feed_ts, vehicle_id),
                feed_ts=feed_ts,
                ts=int(v.timestamp) or feed_ts,
                vehicle_id=vehicle_id,
                trip_id=v.trip.trip_id or None,
                route_id=v.trip.route_id or None,
                lat=p.latitude,
                lon=p.longitude,
                bearing=p.bearing if p.HasField("bearing") else None,
                speed=p.speed if p.HasField("speed") else None,
                stop_id=v.stop_id or None,
                current_status=_enum_name(_VSTATUS, v.current_status)
                if v.HasField("current_status")
                else None,
                occupancy=_enum_name(_OCC, v.occupancy_status)
                if v.HasField("occupancy_status")
                else None,
            )
        )
    return events


def trip_update_events(feed: rt.FeedMessage) -> list[TripUpdateEvent]:
    feed_ts = int(feed.header.timestamp)
    events: list[TripUpdateEvent] = []
    for ent in feed.entity:
        if not ent.HasField("trip_update"):
            continue
        tu = ent.trip_update
        trip_id = tu.trip.trip_id
        if not trip_id:
            continue
        stops = []
        for s in tu.stop_time_update:
            stops.append(
                StopTimeEvent(
                    stop_sequence=s.stop_sequence,
                    stop_id=s.stop_id or None,
                    arrival_time=int(s.arrival.time) or None if s.HasField("arrival") else None,
                    departure_time=int(s.departure.time) or None
                    if s.HasField("departure")
                    else None,
                    schedule_relationship=_enum_name(_STOP_REL, s.schedule_relationship),
                )
            )
        events.append(
            TripUpdateEvent(
                event_key=trip_event_key(feed_ts, trip_id),
                feed_ts=feed_ts,
                trip_id=trip_id,
                route_id=tu.trip.route_id or None,
                schedule_relationship=_enum_name(_TRIP_REL, tu.trip.schedule_relationship),
                start_date=tu.trip.start_date or None,
                stops=stops,
            )
        )
    return events
