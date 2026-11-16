-- Tables and hypertables.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------- static GTFS
CREATE TABLE gtfs_meta (
    id            int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    sha256        text NOT NULL,
    source_url    text NOT NULL,
    loaded_at     timestamptz NOT NULL,
    n_trips       int NOT NULL,
    n_stop_times  int NOT NULL
);

CREATE TABLE routes (
    route_id          text PRIMARY KEY,
    route_short_name  text,
    route_long_name   text,
    route_type        int,
    route_color       text,
    route_text_color  text
);

CREATE TABLE stops (
    stop_id    text PRIMARY KEY,
    stop_code  text,
    stop_name  text,
    stop_lat   double precision,
    stop_lon   double precision
);

CREATE TABLE trips (
    trip_id        text PRIMARY KEY,
    route_id       text NOT NULL,
    service_id     text NOT NULL,
    trip_headsign  text,
    direction_id   int,
    block_id       text,
    shape_id       text
);
CREATE INDEX trips_route_idx ON trips (route_id);

CREATE TABLE stop_times (
    trip_id        text NOT NULL,
    stop_sequence  int  NOT NULL,
    stop_id        text NOT NULL,
    arrival_s      int  NOT NULL,  -- seconds after service-day origin; may exceed 86400
    departure_s    int  NOT NULL,
    timepoint      smallint,
    PRIMARY KEY (trip_id, stop_sequence)
);

CREATE TABLE calendar (
    service_id  text PRIMARY KEY,
    monday      boolean NOT NULL,
    tuesday     boolean NOT NULL,
    wednesday   boolean NOT NULL,
    thursday    boolean NOT NULL,
    friday      boolean NOT NULL,
    saturday    boolean NOT NULL,
    sunday      boolean NOT NULL,
    start_date  date NOT NULL,
    end_date    date NOT NULL
);

CREATE TABLE calendar_dates (
    service_id      text NOT NULL,
    date            date NOT NULL,
    exception_type  smallint NOT NULL,
    PRIMARY KEY (service_id, date)
);

-- ---------------------------------------------------------------- realtime
-- One row per vehicle report. The vehicle's own timestamp is the time column,
-- so repeated snapshots of an unchanged report collapse onto one row.
CREATE TABLE vehicle_positions (
    ts              timestamptz NOT NULL,
    vehicle_id      text NOT NULL,
    trip_id         text,
    route_id        text,
    lat             double precision NOT NULL,
    lon             double precision NOT NULL,
    bearing         real,
    speed           real,
    stop_id         text,
    current_status  text,
    occupancy       text,
    feed_ts         timestamptz NOT NULL,
    trip_matched    boolean NOT NULL,
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (vehicle_id, ts)
);
SELECT create_hypertable('vehicle_positions', by_range('ts', INTERVAL '1 day'));
CREATE INDEX vehicle_positions_route_idx ON vehicle_positions (route_id, ts DESC);
CREATE INDEX vehicle_positions_trip_idx ON vehicle_positions (trip_id, ts DESC);

-- One row per (trip, service day, stop): the "observed arrival". Each new feed
-- snapshot overwrites the row with a fresher prediction until the bus reaches
-- the stop. Partitioned on scheduled_at because it never changes for a row.
CREATE TABLE stop_delays (
    scheduled_at   timestamptz NOT NULL,
    trip_id        text NOT NULL,
    service_date   date NOT NULL,
    stop_sequence  int  NOT NULL,
    stop_id        text NOT NULL,
    route_id       text NOT NULL,
    predicted_at   timestamptz NOT NULL,
    delay_s        int  NOT NULL,
    observed       boolean NOT NULL,  -- predicted_at <= feed_ts: the bus has reached the stop
    feed_ts        timestamptz NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (trip_id, service_date, stop_sequence, scheduled_at)
);
SELECT create_hypertable('stop_delays', by_range('scheduled_at', INTERVAL '1 day'));
CREATE INDEX stop_delays_route_idx ON stop_delays (route_id, scheduled_at DESC);

-- One row per producer poll (from the heartbeats topic).
CREATE TABLE feed_health (
    ts            timestamptz NOT NULL,
    feed          text NOT NULL,
    ok            boolean NOT NULL,
    entity_count  int NOT NULL,
    feed_ts       timestamptz,
    new_snapshot  boolean NOT NULL,
    http_status   int,
    latency_ms    int,
    error         text,
    PRIMARY KEY (feed, ts)
);
SELECT create_hypertable('feed_health', by_range('ts', INTERVAL '7 days'));

-- One row per topic per processed batch: data-quality counters.
CREATE TABLE processor_stats (
    ts               timestamptz NOT NULL DEFAULT now(),
    topic            text NOT NULL,
    received         int NOT NULL,
    malformed        int NOT NULL DEFAULT 0,
    inserted         int NOT NULL DEFAULT 0,
    updated          int NOT NULL DEFAULT 0,
    duplicates       int NOT NULL DEFAULT 0,
    matched_trips    int NOT NULL DEFAULT 0,
    unmatched_trips  int NOT NULL DEFAULT 0,
    added_trips      int NOT NULL DEFAULT 0,
    outliers         int NOT NULL DEFAULT 0,
    stop_mismatches  int NOT NULL DEFAULT 0
);
SELECT create_hypertable('processor_stats', by_range('ts', INTERVAL '7 days'));

-- Messages that failed validation; keyed by offset so replays don't duplicate.
CREATE TABLE bad_messages (
    topic       text NOT NULL,
    partition   int NOT NULL,
    "offset"    bigint NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    error       text NOT NULL,
    payload     text,
    PRIMARY KEY (topic, partition, "offset")
);

-- Daily on-time performance per route (F15).
CREATE TABLE route_otp_daily (
    service_date  date NOT NULL,
    route_id      text NOT NULL,
    n             int NOT NULL,
    early         int NOT NULL,
    on_time       int NOT NULL,
    late          int NOT NULL,
    otp_pct       double precision NOT NULL,
    avg_delay_s   double precision NOT NULL,
    computed_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (service_date, route_id)
);
