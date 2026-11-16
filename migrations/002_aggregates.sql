-- Continuous aggregates, compression and retention.

CREATE MATERIALIZED VIEW route_delay_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket(INTERVAL '15 minutes', scheduled_at) AS bucket,
    route_id,
    avg(delay_s)::double precision                              AS avg_delay_s,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY delay_s)         AS p90_delay_s,
    count(*)                                                     AS n
FROM stop_delays
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW route_delay_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket(INTERVAL '1 hour', scheduled_at) AS bucket,
    route_id,
    avg(delay_s)::double precision                              AS avg_delay_s,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY delay_s)         AS p90_delay_s,
    count(*)                                                     AS n
FROM stop_delays
GROUP BY 1, 2
WITH NO DATA;

-- Rows keep updating until the bus reaches the stop (at most a couple of hours
-- for very late trips), so refresh a trailing window that covers that.
SELECT add_continuous_aggregate_policy('route_delay_15m',
    start_offset => INTERVAL '6 hours',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '5 minutes');

SELECT add_continuous_aggregate_policy('route_delay_1h',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes');

-- Compression (F14).
ALTER TABLE vehicle_positions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'route_id',
    timescaledb.compress_orderby = 'ts DESC'
);
SELECT add_compression_policy('vehicle_positions', INTERVAL '1 day');

ALTER TABLE stop_delays SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'route_id',
    timescaledb.compress_orderby = 'scheduled_at DESC'
);
SELECT add_compression_policy('stop_delays', INTERVAL '3 days');

ALTER TABLE feed_health SET (timescaledb.compress, timescaledb.compress_segmentby = 'feed');
SELECT add_compression_policy('feed_health', INTERVAL '7 days');

-- Retention (F14): raw positions 14 days; delays 90 days; aggregates kept forever.
SELECT add_retention_policy('vehicle_positions', INTERVAL '14 days');
SELECT add_retention_policy('stop_delays', INTERVAL '90 days');
SELECT add_retention_policy('feed_health', INTERVAL '90 days');
SELECT add_retention_policy('processor_stats', INTERVAL '30 days');
