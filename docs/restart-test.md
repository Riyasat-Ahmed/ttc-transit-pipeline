# Restart test: no loss, no duplicates

`scripts/restart_test.sh <seconds>` SIGKILLs the processor (no graceful shutdown, so a
batch can be killed mid-transaction) while the producer keeps polling and publishing.
It then restarts the processor, waits for consumer lag to reach 0, and checks:

1. **No loss:** every heartbeat published to Redpanda during the window is in `feed_health`.
   There is one heartbeat per poll, so this is an exact count rather than an estimate.
2. **No loss of data:** every new vehicle snapshot published during the outage has rows in
   `vehicle_positions`.
3. **No duplicates:** zero repeated keys in `stop_delays` and `vehicle_positions`.

## Results (2026-11-16, laptop, Colima 4 CPU / 6 GB)

| Outage | Backlog while down | Heartbeats published / stored | Missing | Duplicated | New vehicle snapshots without rows | Duplicate keys | Result |
|---|---|---|---|---|---|---|---|
| 90 s | n/a | 9 / 9 | 0 | 0 | 0 of 1 | 0 | PASS |
| 300 s | 38,618 messages | 32 / 32 | 0 | 0 | 0 of 11 | 0 | PASS |

Later the same evening the database container was recreated (a port change) while the
pipeline was running. The processor's retry loop reconnected and wrote its next batch 2 s
after Postgres came back. No container restarted.

## Why it works

* The processor commits Kafka offsets **only after** the DB transaction commits, so anything
  not durably written is re-delivered (at-least-once).
* Every write is idempotent: upserts on natural keys, and `stop_delays` only accepts a row
  from a strictly newer feed snapshot. Re-delivered messages are no-ops, whatever order
  they arrive in.
