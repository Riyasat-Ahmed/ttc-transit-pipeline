# TTC Real-Time Transit Pipeline

[![ci](https://github.com/Riyasat-Ahmed/ttc-transit-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Riyasat-Ahmed/ttc-transit-pipeline/actions/workflows/ci.yml)

Streams live TTC bus and streetcar positions and predictions, computes how late
every trip is at every stop, and shows it on a live Grafana dashboard.

**Headline question:** which TTC routes run latest, and how much worse is it at rush hour than off-peak?

> **Finding:** _pending: collecting 7 days of data. Run `analysis/weekly_finding.py` and paste `analysis/output/finding.md` here._

![Dashboard](docs/dashboard_top.png)

<sub>Live dashboard on a Monday evening: 1,807 vehicles, colour-coded by delay. [Full dashboard](docs/dashboard_full.png).</sub>

**Measured on the first run (laptop, 2026-11-16):**

| Metric | Target | Measured |
|---|---|---|
| End-to-end latency (TTC publish → DB), p95 | < 60 s | **21 s** (median 18 s) |
| Polls succeeding | 99%+ | 100% |
| Trips matched to the schedule | 80%+ | **99.9%** of scheduled trips (91% of all trip updates; the rest are unscheduled `NEW` trips) |
| Duplicate rows in `stop_delays` | 0 | **0**, including after the restart tests |
| Vehicles per snapshot | n/a | about 1,800 on about 180 routes |
| Feed refresh interval | n/a | about 33 s |

## Architecture

```mermaid
flowchart LR
    subgraph TTC["TTC / City of Toronto"]
        V[GTFS-RT vehicles]
        T[GTFS-RT trip updates]
        S[Static GTFS zip]
    end
    P[producer<br/>poll every 20 s] -->|vehicle_positions<br/>trip_updates<br/>heartbeats| R[(Redpanda<br/>24 h retention)]
    V --> P
    T --> P
    R --> C[processor<br/>validate · dedupe · delay]
    C -->|upsert, then commit offsets| DB[(TimescaleDB<br/>hypertables + continuous aggregates)]
    S --> L[loader<br/>daily refresh + on-time job] --> DB
    DB --> G[Grafana<br/>map · rankings · data quality]
    DB --> A[alerter] --> D[Discord]
```

| Stage | What it does |
|---|---|
| `producer` | Polls both feeds every 20 s, decodes protobuf, publishes one JSON event per vehicle / trip, keyed `feed_ts:id`. Skips unchanged snapshots, backs off exponentially on errors, and publishes a heartbeat for every poll. |
| `redpanda` | Kafka-compatible broker. Keeps 24 h, so the processor can be down for hours and catch up. |
| `processor` | Consumer group. Validates each message (bad ones are quarantined in `bad_messages`, never fatal), computes delays against the schedule, writes in one transaction, and **commits offsets only after the DB commit**. |
| `timescaledb` | Hypertables for positions, delays and feed health; continuous aggregates for 15-min and hourly route delay; compression and retention policies. |
| `loader` | Loads the static GTFS on start and every 24 h (skipped if unchanged), computes daily on-time performance, prunes old bad messages. |
| `alerter` | Posts to Discord when a feed is stale or the producer/processor stops for 5+ min, and again on recovery. |
| `grafana` | Dashboard and data source provisioned from files in `grafana/`. |

### Correctness: at-least-once delivery + idempotent writes

* Every write is an upsert on a natural key: `vehicle_positions (vehicle_id, ts)`,
  `stop_delays (trip_id, service_date, stop_sequence)`, `feed_health (feed, ts)`,
  `bad_messages (topic, partition, offset)`. Replaying any message is a no-op.
* `stop_delays` rows are only overwritten by a **strictly newer** feed snapshot
  (`WHERE EXCLUDED.feed_ts > stop_delays.feed_ts`), so replays and out-of-order
  delivery across partitions can never move a row backwards.
* `scripts/restart_test.sh` SIGKILLs the processor while the producer keeps
  running, restarts it, and checks that every heartbeat published during the
  outage reached `feed_health` exactly once, every vehicle snapshot has rows,
  and there are zero duplicate keys. Results are in [docs/restart-test.md](docs/restart-test.md):
  after a 5-minute SIGKILL outage (38,618 messages of backlog), the processor caught up
  and the test passed.

### How delay is measured

The TTC trip-updates feed gives an absolute predicted (or, for passed stops,
actual) time for each stop but no `delay` field and no `start_date`. So:

1. **Service day is inferred.** GTFS times count from "noon minus 12 h" on
   the service day and can exceed `24:00:00`. For each trip the processor
   tries yesterday and today, keeps those where the trip's `service_id` is
   active, and picks the one whose schedule is closest to the prediction.
   DST days are handled by the noon-minus-12h rule.
2. **Observed arrivals, not every prediction.** Storing every prediction from
   every poll would be about 140M rows a day, and far-future predictions mostly
   echo the schedule. The processor writes only predictions up to 2 minutes
   ahead, then keeps overwriting that row with each fresher snapshot until the
   bus reaches the stop. The final row is the observed arrival
   (`observed = true`), at one row per stop visit.
3. Delays beyond ±2 h are dropped as outliers and counted on the data-quality panel.

**Cold-start caveat:** for the first hour or two after the pipeline starts from empty, the
delay numbers are biased. Stops scheduled before the start time only get rows if the bus is
still on its way, so those rows skew late. Stops already passed ahead of schedule skew early.
Once every stop visit is covered from approach to arrival, the bias disappears. It's
negligible over a week of data, but don't read much into the first hour's charts.

## Quick start

```bash
cp .env.example .env        # optional locally; change the passwords on a server
docker compose up -d --build
open http://localhost:3000  # admin / admin (or GRAFANA_ADMIN_PASSWORD)
```

Any Docker engine works. On a Mac without Docker Desktop (or where another macOS account
already runs it), [Colima](https://github.com/abiosoft/colima) is a lightweight alternative:
`brew install colima docker-compose && colima start --cpu 4 --memory 6 --disk 40`.

Vehicles appear on the map within about a minute. Delays need the static schedule,
which the `loader` downloads (66 MB) and loads in the first couple of minutes.
Watch it with `docker compose logs -f loader processor`.

Useful commands:

```bash
docker compose ps                                     # everything should be "running"
docker compose exec redpanda rpk group describe ttc-processor   # consumer lag
docker compose exec timescaledb psql -U ttc -d ttc    # poke at the data (host: localhost:15432)
docker compose run --rm loader load-static --force    # reload the schedule now
./scripts/restart_test.sh 90                          # the no-loss / no-dupes test
```

## Dashboard

* **Live:** a map of every vehicle reporting in the last 3 minutes, coloured by its trip's
  current delay; vehicle count; network average delay; the 10 latest-running routes in the last hour.
* **Routes:** worst routes with average and p90 delay; delay by hour of day over the past 7 days
  for a chosen route; route vs network delay over time; daily on-time performance (−1 to +5 min).
* **Data quality:** feed staleness, poll success rate, trips matched to the schedule, vehicles with no
  matching trip, end-to-end latency (TTC publish → DB), messages received / malformed /
  duplicates dropped per minute, entity counts per poll, recent errors.

The dashboard JSON is generated from `scripts/build_dashboard.py`, so the SQL is reviewable.
Edit the script, run `uv run python scripts/build_dashboard.py`, and Grafana picks up the change.

## Data sources

| Feed | URL |
|---|---|
| Vehicle positions (GTFS-RT) | `https://bustime.ttc.ca/gtfsrt/vehicles` |
| Trip updates (GTFS-RT) | `https://bustime.ttc.ca/gtfsrt/trips` |
| Static GTFS | [Surface Routes and Schedules for Bustime](https://open.toronto.ca/dataset/surface-routes-and-schedules-for-bustime/) (City of Toronto Open Data) |

**Use the "for Bustime" static feed.** Day-one checks (2026-11-16) found that the general
*TTC Routes and Schedules* dataset uses a different trip-id scheme. Only **4 of 2,353**
realtime trips matched it, and only 59% of stop ids. The bustime dataset matched
**2,353 / 2,353** scheduled trips and 99.9% of stop ids. The only unmatched trips are
the few the feed marks `NEW` (unscheduled extras), which are counted separately.

Other day-one checks (answers to the PRD's open questions):

* Trip updates cover about 2,400 trips with about 32,000 stop predictions per snapshot. Every
  prediction is an absolute time; none uses `delay`. Past stops are included.
* About 1,800 vehicles per snapshot, and about 90% carry a trip id.
* Feed snapshots refresh roughly every 20–30 s, and no API key is needed.

Polling etiquette: at most one request per feed every 20 s, a descriptive `User-Agent`,
and exponential backoff on errors (up to 5 min).

## Tests and CI

```bash
uv sync
uv run ruff check .
uv run pytest                                    # offline: fixtures in tests/fixtures
# DB idempotency tests: use a scratch database (they TRUNCATE tables)
docker compose exec timescaledb createdb -U ttc ttc_test
DATABASE_URL=postgresql://ttc:ttc@localhost:15432/ttc_test uv run pytest
```

`tests/fixtures/snapshots` holds 10 real feed snapshots (gzipped protobuf), and
`tests/fixtures/gtfs_subset` holds the matching slice of the static schedule. The tests cover
decoding, delay calculation (past-midnight trips, DST, calendar exceptions, real-snapshot
plausibility), de-duplication, quarantining malformed messages, and DB upsert idempotency.
GitHub Actions runs all of it against a TimescaleDB service container.

## Running it 24/7 on a VPS

See [docs/deploy.md](docs/deploy.md): a 2 vCPU / 4 GB box, about 10 minutes of setup,
HTTPS via Caddy, and Discord alerts.

## Weekly finding

```bash
uv run --extra analysis python analysis/weekly_finding.py --days 7
```

This writes `analysis/output/finding.md` (the headline sentence plus a ranking table) and
`analysis/output/rush_vs_offpeak.png`. Rush hour is weekdays 07–10 and 16–19; off-peak is
weekdays 10–16 and 19–22.

## Repo layout

```
docker-compose.yml        redpanda, timescaledb, grafana, migrate, loader, producer, processor, alerter
docker-compose.prod.yml   VPS overlay: Caddy HTTPS, DB/broker ports closed
src/ttc/                  config, feeds, producer, processor, delay, schedule, static_loader,
                          jobs (daily), alerter, db, models, cli
migrations/               001 tables + hypertables, 002 aggregates + compression + retention
grafana/                  provisioning + dashboards/ttc.json (generated by scripts/build_dashboard.py)
analysis/                 weekly_finding.py
tests/                    + fixtures/ (real snapshots and schedule subset)
scripts/                  restart_test.sh, build_dashboard.py
deploy/                   Caddyfile, setup_vps.sh
```

## Licence and attribution

Contains information licensed under the Open Government Licence – Toronto.
