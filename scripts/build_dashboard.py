"""Generate grafana/dashboards/ttc.json.

The dashboard is version-controlled as JSON (what Grafana provisions), but it's
written from this script so the SQL stays readable and reviewable.
Run: uv run python scripts/build_dashboard.py
"""

from __future__ import annotations

import json
from pathlib import Path

DS = {"type": "grafana-postgresql-datasource", "uid": "ttc-timescaledb"}
ATTRIBUTION = "Contains information licensed under the Open Government Licence – Toronto."
_id = 0


def target(sql: str, fmt: str = "table", ref: str = "A") -> dict:
    return {"datasource": DS, "refId": ref, "rawQuery": True, "editorMode": "code",
            "format": fmt, "rawSql": sql.strip()}


def panel(kind: str, title: str, grid: tuple[int, int, int, int], targets: list[dict],
          **extra) -> dict:
    global _id
    _id += 1
    x, y, w, h = grid
    p = {"id": _id, "type": kind, "title": title, "datasource": DS, "targets": targets,
         "gridPos": {"x": x, "y": y, "w": w, "h": h}}
    p.update(extra)
    return p


def delay_thresholds() -> dict:
    return {"mode": "absolute", "steps": [
        {"color": "green", "value": None}, {"color": "yellow", "value": 2},
        {"color": "orange", "value": 5}, {"color": "red", "value": 10}]}


def row(title: str, y: int) -> dict:
    global _id
    _id += 1
    return {"id": _id, "type": "row", "title": title, "collapsed": False,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


ROUTE_LABEL = "coalesce(r.route_short_name || ' ' || r.route_long_name, d.route_id)"

panels = [
    row("Live", 0),
    panel("geomap", "Vehicles now (colour = current delay, min)", (0, 1, 16, 16), [target("""
        SELECT v.lat, v.lon, v.vehicle_id, v.route_id,
               coalesce(r.route_short_name || ' ' || r.route_long_name, v.route_id) AS route,
               round(d.delay_s / 60.0, 1) AS delay_min,
               v.ts AS reported_at
        FROM (
            SELECT DISTINCT ON (vehicle_id) *
            FROM vehicle_positions
            WHERE ts > now() - interval '3 minutes'
            ORDER BY vehicle_id, ts DESC
        ) v
        LEFT JOIN routes r ON r.route_id = v.route_id
        LEFT JOIN LATERAL (
            SELECT delay_s FROM stop_delays sd
            WHERE sd.trip_id = v.trip_id AND sd.scheduled_at > now() - interval '4 hours'
            ORDER BY sd.feed_ts DESC, sd.stop_sequence DESC LIMIT 1
        ) d ON true
    """)],
        fieldConfig={"defaults": {"unit": "m", "decimals": 1, "color": {"mode": "thresholds"},
                                  "thresholds": delay_thresholds()}, "overrides": []},
        options={
            "view": {"id": "coords", "lat": 43.70, "lon": -79.40, "zoom": 10.5},
            "controls": {"showZoom": True, "mouseWheelZoom": True},
            "basemap": {"type": "osm-standard", "name": "OpenStreetMap"},
            "layers": [{
                "type": "markers", "name": "Vehicles",
                "location": {"mode": "coords", "latitude": "lat", "longitude": "lon"},
                "config": {"showLegend": True, "style": {
                    "size": {"fixed": 4}, "opacity": 0.8,
                    "color": {"field": "delay_min", "fixed": "dark-green"},
                    "symbol": {"mode": "fixed", "fixed": "img/icons/marker/circle.svg"}}},
                "tooltip": True}],
            "tooltip": {"mode": "details"}}),
    panel("stat", "Vehicles reporting", (16, 1, 4, 4), [target("""
        SELECT count(DISTINCT vehicle_id) AS vehicles FROM vehicle_positions
        WHERE ts > now() - interval '3 minutes'""")],
        fieldConfig={"defaults": {"color": {"mode": "fixed", "fixedColor": "text"}},
                     "overrides": []},
        options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value"}),
    panel("stat", "Network avg delay (last hour)", (20, 1, 4, 4), [target("""
        SELECT avg(delay_s) / 60.0 AS "avg delay" FROM stop_delays
        WHERE scheduled_at BETWEEN now() - interval '1 hour' AND now()""")],
        fieldConfig={"defaults": {"unit": "m", "decimals": 1, "color": {"mode": "thresholds"},
                                  "thresholds": delay_thresholds()}, "overrides": []},
        options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value"}),
    panel("bargauge", "10 latest-running routes (last hour, avg delay)", (16, 5, 8, 12), [target(f"""
        SELECT {ROUTE_LABEL} AS route,
               avg(d.delay_s) / 60.0 AS avg_delay_min
        FROM stop_delays d LEFT JOIN routes r USING (route_id)
        WHERE d.scheduled_at BETWEEN now() - interval '1 hour' AND now()
        GROUP BY 1 HAVING count(*) >= 20
        ORDER BY 2 DESC LIMIT 10""")],
        fieldConfig={"defaults": {"unit": "m", "decimals": 1, "min": 0,
                                  "color": {"mode": "thresholds"},
                                  "thresholds": delay_thresholds()}, "overrides": []},
        options={"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
                 "valueMode": "color", "namePlacement": "left",
                 "reduceOptions": {"calcs": [], "values": True, "fields": "/^avg_delay_min$/"}}),

    row("Routes", 17),
    panel("table", "Worst routes (last hour): average and p90 delay", (0, 18, 10, 11), [target(f"""
        SELECT {ROUTE_LABEL} AS "Route",
               round(avg(d.delay_s) / 60.0, 1) AS "Avg (min)",
               round((percentile_cont(0.9) WITHIN GROUP (ORDER BY d.delay_s) / 60.0)::numeric, 1)
                   AS "p90 (min)",
               count(*) AS "Stop arrivals"
        FROM stop_delays d LEFT JOIN routes r USING (route_id)
        WHERE d.scheduled_at BETWEEN now() - interval '1 hour' AND now()
        GROUP BY 1 HAVING count(*) >= 20
        ORDER BY 2 DESC LIMIT 25""")],
        fieldConfig={"defaults": {"custom": {"align": "auto"}}, "overrides": [
            {"matcher": {"id": "byRegexp", "options": "Avg.*|p90.*"}, "properties": [
                {"id": "custom.cellOptions", "value": {"type": "color-background"}},
                {"id": "decimals", "value": 1},
                {"id": "thresholds", "value": delay_thresholds()},
                {"id": "color", "value": {"mode": "thresholds"}}]}]}),
    panel("barchart", "Delay by hour of day: $route (past 7 days, Toronto time)", (10, 18, 14, 11),
          [target("""
        SELECT lpad(extract(hour FROM bucket AT TIME ZONE 'America/Toronto')::text, 2, '0')
                   || ':00' AS hour,
               sum(avg_delay_s * n) / sum(n) / 60.0 AS "avg delay",
               avg(p90_delay_s) / 60.0 AS "p90 delay (mean of hourly p90s)"
        FROM route_delay_1h
        WHERE route_id = '$route' AND bucket > now() - interval '7 days'
        GROUP BY 1 ORDER BY 1""")],
        fieldConfig={"defaults": {"unit": "m", "decimals": 1}, "overrides": []},
        options={"xField": "hour", "orientation": "vertical", "showValue": "never",
                 "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"}}),
    panel("timeseries", "Average delay over time: $route vs network (15 min buckets)",
          (0, 29, 24, 8), [
        target("""
        SELECT bucket AS time, sum(avg_delay_s * n) / sum(n) / 60.0 AS network
        FROM route_delay_15m WHERE $__timeFilter(bucket)
        GROUP BY 1 ORDER BY 1""", fmt="time_series", ref="A"),
        target("""
        SELECT bucket AS time, avg_delay_s / 60.0 AS "$route"
        FROM route_delay_15m WHERE route_id = '$route' AND $__timeFilter(bucket)
        ORDER BY 1""", fmt="time_series", ref="B")],
        fieldConfig={"defaults": {"unit": "m", "decimals": 1,
                                  "custom": {"lineWidth": 2, "fillOpacity": 10}}, "overrides": []}),
    panel("table", "On-time performance (daily job: −1 to +5 min)", (0, 37, 24, 8), [target(f"""
        SELECT o.service_date AS "Service day", {ROUTE_LABEL.replace('d.route_id', 'o.route_id')}
                   AS "Route",
               round(o.otp_pct::numeric, 1) AS "On time %", o.early AS "Early", o.on_time AS "On time",
               o.late AS "Late", round((o.avg_delay_s / 60.0)::numeric, 1) AS "Avg delay (min)"
        FROM route_otp_daily o LEFT JOIN routes r USING (route_id)
        WHERE o.service_date = (SELECT max(service_date) FROM route_otp_daily) AND o.n >= 100
        ORDER BY o.otp_pct ASC""")]),

    row("Data quality", 45),
    panel("stat", "Feed staleness (s since last new snapshot)", (0, 46, 6, 5), [target("""
        SELECT feed, extract(epoch FROM now() - max(feed_ts))::int AS staleness_s
        FROM feed_health WHERE ts > now() - interval '1 day' AND ok GROUP BY feed""")],
        fieldConfig={"defaults": {"unit": "s", "color": {"mode": "thresholds"},
                                  "thresholds": {"mode": "absolute", "steps": [
                                      {"color": "green", "value": None},
                                      {"color": "orange", "value": 90},
                                      {"color": "red", "value": 300}]}}, "overrides": []},
        options={"reduceOptions": {"calcs": ["lastNotNull"], "values": True,
                                   "fields": "/^staleness_s$/"},
                 "colorMode": "background", "textMode": "value_and_name"}),
    panel("stat", "Polls OK (last 24 h)", (6, 46, 4, 5), [target("""
        SELECT 100.0 * count(*) FILTER (WHERE ok) / nullif(count(*), 0) AS pct
        FROM feed_health WHERE ts > now() - interval '24 hours'""")],
        fieldConfig={"defaults": {"unit": "percent", "decimals": 2, "color": {"mode": "thresholds"},
                                  "thresholds": {"mode": "absolute", "steps": [
                                      {"color": "red", "value": None},
                                      {"color": "orange", "value": 98},
                                      {"color": "green", "value": 99}]}}, "overrides": []},
        options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background"}),
    panel("stat", "Trips matched to schedule (last hour)", (10, 46, 5, 5), [target("""
        SELECT 100.0 * sum(matched_trips) / nullif(sum(matched_trips + unmatched_trips), 0) AS pct
        FROM processor_stats WHERE topic = 'trip_updates' AND ts > now() - interval '1 hour'""")],
        fieldConfig={"defaults": {"unit": "percent", "decimals": 1, "color": {"mode": "thresholds"},
                                  "thresholds": {"mode": "absolute", "steps": [
                                      {"color": "red", "value": None},
                                      {"color": "orange", "value": 80},
                                      {"color": "green", "value": 95}]}}, "overrides": []},
        options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background"}),
    panel("stat", "Vehicles with no matching trip (last hour)", (15, 46, 5, 5), [target("""
        SELECT 100.0 * sum(unmatched_trips) / nullif(sum(matched_trips + unmatched_trips), 0)
                   AS pct
        FROM processor_stats WHERE topic = 'vehicle_positions'
          AND ts > now() - interval '1 hour'""")],
        fieldConfig={"defaults": {"unit": "percent", "decimals": 1}, "overrides": []},
        options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value"}),
    panel("stat", "End-to-end latency p95 (TTC publish → DB, last 15 min)", (20, 46, 4, 5),
          [target("""
        SELECT percentile_cont(0.95) WITHIN GROUP (
                   ORDER BY extract(epoch FROM ingested_at - feed_ts)) AS latency
        FROM vehicle_positions WHERE ts > now() - interval '15 minutes'""")],
        fieldConfig={"defaults": {"unit": "s", "decimals": 1, "color": {"mode": "thresholds"},
                                  "thresholds": {"mode": "absolute", "steps": [
                                      {"color": "green", "value": None},
                                      {"color": "red", "value": 60}]}}, "overrides": []},
        options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value"}),
    panel("timeseries", "Messages per minute: received / malformed / duplicates dropped",
          (0, 51, 12, 8), [target("""
        SELECT time_bucket('1 minute', ts) AS time,
               sum(received) AS received, sum(malformed) AS malformed,
               sum(duplicates) AS "duplicates dropped"
        FROM processor_stats WHERE $__timeFilter(ts)
        GROUP BY 1 ORDER BY 1""", fmt="time_series")],
        fieldConfig={"defaults": {"custom": {"drawStyle": "bars", "fillOpacity": 60}},
                     "overrides": []}),
    panel("timeseries", "Feed entity count per poll (gaps = failed or skipped polls)",
          (12, 51, 12, 8), [target("""
        SELECT ts AS time, feed AS metric, entity_count AS value
        FROM feed_health WHERE $__timeFilter(ts) AND ok
        ORDER BY 1""", fmt="time_series")],
        fieldConfig={"defaults": {"custom": {"lineWidth": 1, "spanNulls": False}},
                     "overrides": []}),
    panel("table", "Recent feed errors and bad messages", (0, 59, 24, 7), [target("""
        SELECT ts AS "When", feed AS "Source", coalesce(http_status::text, '') AS "HTTP",
               error AS "Error"
        FROM feed_health WHERE NOT ok AND ts > now() - interval '7 days'
        UNION ALL
        SELECT received_at, topic, '', error FROM bad_messages
        WHERE received_at > now() - interval '7 days'
        ORDER BY 1 DESC LIMIT 50""")]),
    panel("text", "", (0, 66, 24, 2), [],
          options={"mode": "markdown", "content":
                   f"{ATTRIBUTION} Data: TTC GTFS-Realtime (bustime.ttc.ca) and "
                   "Surface Routes and Schedules for Bustime (City of Toronto Open Data)."}),
]

dashboard = {
    "uid": "ttc-transit",
    "title": "TTC Real-Time Transit",
    "description": ATTRIBUTION,
    "tags": ["ttc", "gtfs"],
    "timezone": "America/Toronto",
    "refresh": "30s",
    "schemaVersion": 39,
    "time": {"from": "now-6h", "to": "now"},
    "templating": {"list": [{
        "name": "route", "label": "Route", "type": "query", "datasource": DS,
        "query": "SELECT route_short_name || ' ' || route_long_name AS __text, route_id AS __value "
                 "FROM routes WHERE route_type <> 1 ORDER BY route_short_name::int NULLS LAST",
        "definition": "routes", "refresh": 1, "sort": 0,
        "current": {"text": "504 King", "value": "504"}}]},
    "annotations": {"list": []},
    "panels": panels,
}

out = Path(__file__).resolve().parents[1] / "grafana" / "dashboards" / "ttc.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n")
print(f"wrote {out} ({len(panels)} panels)")
