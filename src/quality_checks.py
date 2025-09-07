Compute basic data quality KPIs over raw_prices:
- duplicates (defensive check)
- missing dates/gaps per symbol (daily freq)
- latency between event time (ts) and ingest time (ingest_ts)

Outputs a CSV report to data/output/quality_report.csv
"""

import os, json, csv, sqlite3, datetime
from collections import defaultdict
from typing import List, Dict, Any, Tuple

CONFIG_PATH = "config/config.json"
OUTPUT_DIR = "data/output"
REPORT_PATH = os.path.join(OUTPUT_DIR, "quality_report.csv")

def load_cfg():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def parse_iso(dt: str) -> datetime.datetime:
    # Handles "YYYY-MM-DDTHH:MM:SS[.ms]Z"
    if dt.endswith("Z"):
        dt = dt[:-1]
    # try with microseconds
    try:
        return datetime.datetime.fromisoformat(dt)
    except ValueError:
        # fallback: date-only like YYYY-MM-DD
        return datetime.datetime.fromisoformat(dt + "T00:00:00")

def expected_calendar(start: datetime.date, end: datetime.date) -> List[datetime.date]:
    # For daily frequency: every calendar day inclusive
    days = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += datetime.timedelta(days=1)
    return days

def fetch_raw(conn) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute("""
        SELECT source, symbol, asset_class, ts, ingest_ts, price, currency
        FROM raw_prices
        ORDER BY symbol, ts
    """)
    rows = []
    for r in cur.fetchall():
        rows.append({
            "source": r[0],
            "symbol": r[1],
            "asset_class": r[2],
            "ts": r[3],
            "ingest_ts": r[4],
            "price": r[5],
            "currency": r[6]
        })
    return rows

def compute_latency_hours(ts_iso: str, ingest_iso: str) -> float:
    ts_dt = parse_iso(ts_iso)
    ingest_dt = parse_iso(ingest_iso)
    delta = ingest_dt - ts_dt
    return delta.total_seconds() / 3600.0

def main():
    cfg = load_cfg()
    db_path = cfg.get("database_path", "data/db/mdf.db")
    q = cfg.get("quality", {})
    expected_freq = q.get("expected_frequency", "daily")
    max_latency_hours = float(q.get("max_latency_hours", 48))
    max_gap_days = int(q.get("max_gap_days", 3))

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    conn = sqlite3.connect(db_path)
    data = fetch_raw(conn)

    # --- 1) Duplicates (defensive): same (source, symbol, ts) present more than once ---
    seen = defaultdict(int)
    for r in data:
        key = (r["source"], r["symbol"], r["ts"])
        seen[key] += 1
    duplicates = [(k, c) for k, c in seen.items() if c > 1]
    dup_count = sum(c - 1 for _, c in duplicates)  # number of extra rows

    # --- 2) Missing dates/gaps per symbol (daily) ---
    gaps_by_symbol: Dict[str, List[Tuple[str, str, int]]] = defaultdict(list)
    if expected_freq == "daily":
        # group by (source,symbol) to examine gaps within each feed
        groups: Dict[Tuple[str, str], List[datetime.date]] = defaultdict(list)
        for r in data:
            ts_date = parse_iso(r["ts"]).date()
            groups[(r["source"], r["symbol"])].append(ts_date)

        for (source, symbol), dates in groups.items():
            if not dates:
                continue
            dates = sorted(set(dates))
            full = expected_calendar(dates[0], dates[-1])
            have = set(dates)
            missing_dates = [d for d in full if d not in have]

            # collapse consecutive missing dates into ranges
            if missing_dates:
                start = prev = missing_dates[0]
                for d in missing_dates[1:] + [None]:
                    if d is None or (d - prev).days > 1:
                        gap_len = (prev - start).days + 1
                        gaps_by_symbol[symbol].append(
                            (source, f"{start.isoformat()}→{prev.isoformat()}", gap_len)
                        )
                        if d is not None:
                            start = d
                    prev = d

    # --- 3) Latency stats per (source,symbol) ---
    lat_stats: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(lambda: {"count":0,"avg":0.0,"max":0.0,"violations":0})
    for r in data:
        try:
            lh = compute_latency_hours(r["ts"], r["ingest_ts"])
            key = (r["source"], r["symbol"])
            stat = lat_stats[key]
            stat["count"] += 1
            stat["avg"] = (stat["avg"] * (stat["count"] - 1) + lh) / stat["count"]
            stat["max"] = max(stat["max"], lh)
            if lh > max_latency_hours:
                stat["violations"] += 1
        except Exception:
            continue

    # --- aggregate KPI for header row ---
    total_rows = len(data)
    gap_symbols = sum(1 for v in gaps_by_symbol.values() if v)
    latency_violations = sum(int(v["violations"] > 0) for v in lat_stats.values())

    # --- Write CSV report ---
    with open(REPORT_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        # Header KPI
        w.writerow(["metric","value","notes"])
        w.writerow(["total_rows", total_rows, "All normalized rows in raw_prices"])
        w.writerow(["duplicate_rows", dup_count, "Rows beyond UNIQUE(source,symbol,ts)"])
        w.writerow(["symbols_with_gaps", gap_symbols, f"Max allowed consecutive gap (days): {max_gap_days} (informational in MVP)"])
        w.writerow(["sources_with_latency_violations", latency_violations, f"Threshold (hours): {max_latency_hours}"])
        w.writerow([])

        # Detailed latency by (source,symbol)
        w.writerow(["source","symbol","latency_avg_hours","latency_max_hours","violations_over_threshold"])
        for (source, symbol), stat in sorted(lat_stats.items()):
            w.writerow([source, symbol, f"{stat['avg']:.2f}", f"{stat['max']:.2f}", stat["violations"]])
        w.writerow([])

        # Missing ranges by symbol (if any)
        w.writerow(["symbol","source","missing_range","days_missing"])
        for symbol, ranges in sorted(gaps_by_symbol.items()):
            for (source, rng, days) in ranges:
                # Optionally flag if days exceed max_gap_days
                w.writerow([symbol, source, rng, days])

    print(f"[OK] Wrote report -> {REPORT_PATH}")
    print(f"Rows total: {total_rows}, duplicates: {dup_count}")
    # print a couple of lines so you see it's populated
    with open(REPORT_PATH, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            print(line.rstrip())
            if i > 10:
                print("... (truncated)")
                break

if __name__ == "__main__":
    main()
