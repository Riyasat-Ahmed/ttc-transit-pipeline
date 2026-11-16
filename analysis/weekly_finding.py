"""Produce the README finding and chart from the collected data (F17).

    uv run --extra analysis python analysis/weekly_finding.py [--days 7]

Writes analysis/output/finding.md and analysis/output/rush_vs_offpeak.png.
"Rush hour" = weekdays 07:00-09:59 and 16:00-18:59 Toronto time; "off-peak" =
weekdays 10:00-15:59 and 19:00-21:59. Weekends are excluded from both so the
comparison isn't skewed by different service levels.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import psycopg  # noqa: E402

OUT = Path(__file__).parent / "output"
MIN_ARRIVALS = 2000  # per route per period, so a handful of trips can't top the ranking

SQL = """
WITH h AS (
    SELECT route_id,
           bucket AT TIME ZONE 'America/Toronto' AS local_bucket,
           avg_delay_s, n
    FROM route_delay_1h
    WHERE bucket > now() - make_interval(days => %(days)s)
)
SELECT h.route_id,
       coalesce(r.route_short_name || ' ' || r.route_long_name, h.route_id) AS route,
       extract(isodow FROM local_bucket)::int AS dow,
       extract(hour FROM local_bucket)::int AS hour,
       sum(avg_delay_s * n) / sum(n) AS avg_delay_s,
       sum(n) AS n
FROM h LEFT JOIN routes r USING (route_id)
GROUP BY 1, 2, 3, 4
"""

COVERAGE_SQL = """
SELECT min(ts), max(ts), count(*), 100.0 * count(*) FILTER (WHERE ok) / count(*)
FROM feed_health WHERE ts > now() - make_interval(days => %(days)s)
"""


def period(row) -> str | None:
    if row.dow >= 6:
        return None
    if 7 <= row.hour <= 9 or 16 <= row.hour <= 18:
        return "rush"
    if 10 <= row.hour <= 15 or 19 <= row.hour <= 21:
        return "offpeak"
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--db", default=os.environ.get(
        "DATABASE_URL", "postgresql://ttc:ttc@localhost:5432/ttc"))
    args = ap.parse_args()
    OUT.mkdir(exist_ok=True)

    with psycopg.connect(args.db) as conn:
        df = pd.DataFrame(conn.execute(SQL, {"days": args.days}).fetchall(),
                          columns=["route_id", "route", "dow", "hour", "avg_delay_s", "n"])
        first, last, polls, ok_pct = conn.execute(COVERAGE_SQL, {"days": args.days}).fetchone()
    if df.empty:
        raise SystemExit("no aggregated data yet; let the pipeline run first")

    df["period"] = df.apply(period, axis=1)
    df["w"] = df.avg_delay_s * df.n
    p = df.dropna(subset=["period"]).groupby(["route_id", "route", "period"])[["w", "n"]].sum()
    p["avg_min"] = p.w / p.n / 60
    wide = p.reset_index().pivot_table(index=["route_id", "route"], columns="period",
                                       values=["avg_min", "n"])
    wide.columns = [f"{a}_{b}" for a, b in wide.columns]
    wide = wide.dropna()
    wide = wide[(wide.n_rush >= MIN_ARRIVALS) & (wide.n_offpeak >= MIN_ARRIVALS)].reset_index()
    wide["ratio"] = wide.avg_min_rush / wide.avg_min_offpeak.where(wide.avg_min_offpeak > 0.25)
    worst = wide.sort_values("avg_min_rush", ascending=False).head(15)

    # Hour-of-day profile for the single worst route (weekdays).
    top = worst.iloc[0]
    prof = df[(df.route_id == top.route_id) & (df.dow <= 5)].groupby("hour")[["w", "n"]].sum()
    prof["avg_min"] = prof.w / prof.n / 60
    peak_hour = int(prof.avg_min.idxmax())
    peak_val = prof.avg_min.max()

    net = df[df.dow <= 5].groupby("hour")[["w", "n"]].sum()
    net["avg_min"] = net.w / net.n / 60

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(14, 6), gridspec_kw={"width_ratios": [1.1, 1]})
    y = range(len(worst))
    a1.barh([i + 0.2 for i in y], worst.avg_min_rush, height=0.4, label="Rush hour",
            color="#d9534f")
    a1.barh([i - 0.2 for i in y], worst.avg_min_offpeak, height=0.4, label="Off-peak",
            color="#8aa1b1")
    a1.set_yticks(list(y), worst.route)
    a1.invert_yaxis()
    a1.set_xlabel("Average delay at stops (minutes)")
    a1.set_title(f"15 latest-running TTC routes at rush hour (weekdays, last {args.days} days)")
    a1.legend(loc="lower right")
    a2.plot(prof.index, prof.avg_min, marker="o", color="#d9534f", label=top.route)
    a2.plot(net.index, net.avg_min, color="#555", ls="--", label="All surface routes")
    a2.set_xticks(range(0, 24, 2))
    a2.set_xlabel("Hour of day (Toronto)")
    a2.set_ylabel("Average delay (minutes)")
    a2.set_title("Delay by hour of day, weekdays")
    a2.legend()
    fig.text(0.01, 0.01, "Contains information licensed under the Open Government Licence – "
             "Toronto.", fontsize=8, color="#666")
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(OUT / "rush_vs_offpeak.png", dpi=150)

    ratio_txt = f", {top.ratio:.1f}x its off-peak delay" if pd.notna(top.ratio) else ""
    finding = (
        f"**{top.route}** was the latest-running TTC surface route at rush hour: "
        f"buses averaged **{top.avg_min_rush:.1f} min late** at weekday rush hour vs "
        f"{top.avg_min_offpeak:.1f} min off-peak{ratio_txt}. "
        f"Its worst hour was {peak_hour:02d}:00, at {peak_val:.1f} min."
    )
    lines = [
        "# Weekly finding", "", finding, "",
        f"Data: {polls:,} polls from {first:%Y-%m-%d %H:%M} to {last:%Y-%m-%d %H:%M} UTC, "
        f"{ok_pct:.2f}% successful. Routes need at least {MIN_ARRIVALS:,} stop arrivals in "
        "each period to be ranked.", "",
        "| Route | Rush (min) | Off-peak (min) | Ratio |", "|---|---:|---:|---:|",
        *[f"| {r.route} | {r.avg_min_rush:.1f} | {r.avg_min_offpeak:.1f} | "
          f"{'' if pd.isna(r.ratio) else f'{r.ratio:.1f}x'} |" for r in worst.itertuples()],
        "", "![Rush vs off-peak](analysis/output/rush_vs_offpeak.png)",
    ]
    (OUT / "finding.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
