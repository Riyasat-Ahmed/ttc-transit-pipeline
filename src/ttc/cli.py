"""Command-line entry point: `ttc <command>`."""

from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path

from ttc.config import get_settings
from ttc.log import setup_logging


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="ttc")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="apply SQL migrations")
    p = sub.add_parser("load-static", help="download and load the static GTFS schedule")
    p.add_argument("--force", action="store_true", help="reload even if unchanged")
    sub.add_parser("producer", help="poll feeds and publish to Redpanda")
    sub.add_parser("processor", help="consume, compute delays and write to TimescaleDB")
    sub.add_parser("jobs", help="daily loop: static refresh + on-time performance")
    sub.add_parser("alerter", help="Discord alerts on stale feeds / stopped pipeline")
    p = sub.add_parser("otp", help="compute on-time performance for one service date")
    p.add_argument("date", type=date.fromisoformat)
    args = parser.parse_args(argv)

    settings = get_settings()
    setup_logging(settings.log_level)

    from ttc import db

    if args.cmd == "migrate":
        conn = db.connect(settings.database_url)
        applied = db.migrate(conn, Path(os.environ.get("MIGRATIONS_DIR", "migrations")))
        print(f"applied: {applied or 'nothing (up to date)'}")
    elif args.cmd == "load-static":
        from ttc import static_loader

        static_loader.run(settings, db.connect(settings.database_url), force=args.force)
    elif args.cmd == "producer":
        from ttc import producer

        producer.main(settings)
    elif args.cmd == "processor":
        from ttc import processor

        processor.main(settings)
    elif args.cmd == "jobs":
        from ttc import jobs

        jobs.main(settings)
    elif args.cmd == "alerter":
        from ttc import alerter

        alerter.main(settings)
    elif args.cmd == "otp":
        from ttc import jobs

        print(jobs.compute_otp(db.connect(settings.database_url), args.date), "routes")


if __name__ == "__main__":
    main()
