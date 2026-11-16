# Deploying to a VPS (M5)

Target: a 2 vCPU / 4 GB Ubuntu 24.04 server in Toronto or Montreal, for under $10 CAD a month.
Any provider with a Canadian region works.

## 1. Server

```bash
ssh root@SERVER
curl -fsSL https://raw.githubusercontent.com/<you>/<repo>/main/deploy/setup_vps.sh | bash
```

The script installs Docker (enabled on boot), sets the firewall to allow only SSH, 80 and 443,
turns on unattended security upgrades, and adds 2 GB of swap.

## 2. DNS (for HTTPS)

Point an A record (e.g. `ttc.example.com`) at the server's IP.

## 3. App

```bash
git clone https://github.com/<you>/<repo>.git ttc && cd ttc
cp .env.example .env
# edit .env: set POSTGRES_PASSWORD, GRAFANA_ADMIN_PASSWORD, DOMAIN=ttc.example.com,
#            DISCORD_WEBHOOK_URL, optionally HEALTHCHECK_PING_URL
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

* Every service has `restart: unless-stopped`, and Docker starts on boot, so the
  stack survives crashes and reboots. To check, run `sudo reboot`, wait a minute, then `docker compose ps`.
* Grafana is only reachable through Caddy at `https://DOMAIN`. Postgres and Redpanda
  bind to the server's 127.0.0.1 only (and the firewall blocks everything but 22/80/443).
* Discord: Server settings → Integrations → Webhooks → New webhook → copy the URL.
* Dead-man's switch: the alerter can't report that the whole server is down, so make a free
  check at healthchecks.io (1-minute period, 5-minute grace) and set `HEALTHCHECK_PING_URL`.

## 4. Operating

```bash
docker compose logs -f --tail 50 processor     # JSON logs
docker compose exec timescaledb psql -U ttc -d ttc -c "SELECT hypertable_name,
  pg_size_pretty(hypertable_size(format('%I', hypertable_name)::regclass))
  FROM timescaledb_information.hypertables"
git pull && docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

Changing the Grafana admin password after first start: `GRAFANA_ADMIN_PASSWORD` only applies
when the Grafana volume is first created. Change it later in the UI or with
`docker compose exec grafana grafana cli admin reset-admin-password NEW`.

## 5. After 7 days (M6)

Postgres listens only on the server's `127.0.0.1`, so run the analysis from your laptop
through an SSH tunnel:

```bash
ssh -N -L 15432:127.0.0.1:5432 root@SERVER &
uv run --extra analysis python analysis/weekly_finding.py \
  --db postgresql://ttc:YOUR_POSTGRES_PASSWORD@localhost:15432/ttc
```

Commit `analysis/output/` and paste `finding.md` into the README.
