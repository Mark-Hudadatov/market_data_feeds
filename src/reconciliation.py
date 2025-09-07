
reconciliation.py  (levels + returns)
- Levels: symmetric percentage difference on the same (symbol, ts) across sources.
- Returns: day-over-day returns per source compared on the same dates; reports abs diff,
           and outputs correlation of returns (scale-invariant agreement).

Report: data/output/reconciliation_report.csv
Sections:
1) metric,value,notes
   - total_pairs_compared
   - anomalies_over_threshold_levels
   - returns_pairs_compared
   - anomalies_over_threshold_returns
   - returns_correlation
2) Detailed level anomalies (symbol, ts, source_a, price_a, source_b, price_b, pct_diff)
3) Detailed return anomalies  (symbol, ts, source_a, ret_a_pct, source_b, ret_b_pct, abs_diff_pct)
"""

import os, json, csv, sqlite3, math
from collections import defaultdict

CONFIG_PATH = "config/config.json"
OUTPUT_DIR = "data/output"
REPORT_PATH = os.path.join(OUTPUT_DIR, "reconciliation_report.csv")

def load_cfg():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def fetch_pairs(conn):
    """
    Fetch price pairs on the same (symbol, ts) for all distinct source pairs.
    Returns list of tuples: (symbol, ts, source_a, price_a, source_b, price_b)
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
          a.symbol,
          a.ts,
          a.source AS source_a, a.price AS price_a,
          b.source AS source_b, b.price AS price_b
        FROM raw_prices a
        JOIN raw_prices b
          ON a.symbol = b.symbol
         AND a.ts = b.ts
         AND a.source < b.source      -- avoid double counting (A,B) vs (B,A)
        ORDER BY a.symbol, a.ts, a.source, b.source
    """)
    return cur.fetchall()

def sym_pct_diff(a: float, b: float) -> float:
    """Symmetric % diff: 200*|a-b|/(|a|+|b|). Returns % (e.g., 0.73)."""
    if a == 0.0 and b == 0.0:
        return 0.0
    return 200.0 * abs(a - b) / (abs(a) + abs(b))

def pearson_corr(x, y) -> float:
    """Pearson correlation (stdlib)."""
    n = len(x)
    if n != len(y) or n == 0:
        return float("nan")
    mx = sum(x) / n
    my = sum(y) / n
    num = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    denx = math.sqrt(sum((xi - mx) ** 2 for xi in x))
    deny = math.sqrt(sum((yi - my) ** 2 for yi in y))
    if denx == 0.0 or deny == 0.0:
        return float("nan")
    return num / (denx * deny)

def build_returns(series):
    """
    series: list of (ts, price) ASC for one source.
    returns list of (ts, return_pct) where return_pct is (p_t/p_{t-1}-1)*100
    """
    out = []
    prev_p = None
    prev_ts = None
    for ts, p in series:
        if prev_p not in (None, 0.0):
            out.append((ts, (p / prev_p - 1.0) * 100.0))
        prev_p, prev_ts = p, ts
    return out

def main():
    cfg = load_cfg()
    db_path = cfg.get("database_path", "data/db/mdf.db")
    recon = cfg.get("reconciliation", {})
    thr_levels = float(recon.get("price_delta_pct_threshold", 0.5))     # %
    thr_returns = float(recon.get("return_delta_pct_threshold", 0.2))   # percentage points

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = sqlite3.connect(db_path)

    # ---------- LEVELS ----------
    pairs = fetch_pairs(conn)
    total_pairs = len(pairs)
    level_anoms = []
    for symbol, ts, sa, pa, sb, pb in pairs:
        try:
            pa = float(pa); pb = float(pb)
            pct = sym_pct_diff(pa, pb)
            if pct > thr_levels:
                level_anoms.append([symbol, ts, sa, pa, sb, pb, round(pct, 6)])
        except Exception:
            continue

    # ---------- RETURNS ----------
    # Build per (symbol, source) time series and compute DoD returns
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, source, ts, price
        FROM raw_prices
        ORDER BY symbol, source, ts
    """)
    rows = cur.fetchall()

    series = defaultdict(list)  # (symbol, source) -> [(ts, price), ...]
    for symbol, source, ts, price in rows:
        try:
            series[(symbol, source)].append((ts, float(price)))
        except Exception:
            continue

    returns = {k: build_returns(v) for k, v in series.items()}  # (sym, src) -> [(ts, ret_pct)]
    # join returns across pairs of sources on same (symbol, ts)
    ret_pairs = []
    # reuse sources present in level pairs to limit scope
    for symbol, ts, sa, _, sb, _ in pairs:
        ka = (symbol, sa); kb = (symbol, sb)
        # we need return for the SAME ts (which corresponds to change from ts-1 to ts)
        # make maps for quick lookup
        # build once per key for speed
    ret_maps = {k: {t: r for t, r in v} for k, v in returns.items()}

    ret_pairs = []
    for symbol, ts, sa, _, sb, _ in pairs:
        va = ret_maps.get((symbol, sa), {})
        vb = ret_maps.get((symbol, sb), {})
        if ts in va and ts in vb:
            ret_pairs.append((symbol, ts, sa, va[ts], sb, vb[ts]))

    total_ret_pairs = len(ret_pairs)
    ret_anoms = []
    ax = []
    bx = []
    for symbol, ts, sa, ra, sb, rb in ret_pairs:
        try:
            diff = abs(ra - rb)  # percentage points
            if diff > thr_returns:
                ret_anoms.append([symbol, ts, sa, ra, sb, rb, round(diff, 6)])
            ax.append(ra); bx.append(rb)
        except Exception:
            continue

    ret_corr = pearson_corr(ax, bx)

    # ---------- WRITE REPORT ----------
    with open(REPORT_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        # Summary block
        w.writerow(["metric", "value", "notes"])
        w.writerow(["total_pairs_compared", total_pairs, "Pairs across distinct sources on the same (symbol, ts)"])
        w.writerow(["anomalies_over_threshold_levels", len(level_anoms), f"Levels threshold: {thr_levels}% (symmetric)"])
        w.writerow(["returns_pairs_compared", total_ret_pairs, "Pairs of day-over-day returns on the same (symbol, ts)"])
        w.writerow(["anomalies_over_threshold_returns", len(ret_anoms), f"Returns threshold: {thr_returns} pct-pts (abs diff)"])
        w.writerow(["returns_correlation", f"{ret_corr:.4f}", "Pearson correlation of returns (scale-invariant)"])
        w.writerow([])

        # Detailed level anomalies
        w.writerow(["symbol","ts","source_a","price_a","source_b","price_b","pct_diff"])
        for row in level_anoms[:5000]:
            w.writerow(row)
        w.writerow([])

        # Detailed return anomalies
        w.writerow(["symbol","ts","source_a","ret_a_pct","source_b","ret_b_pct","abs_diff_pct"])
        for row in ret_anoms[:5000]:
            w.writerow(row)

    print(f"[OK] Wrote reconciliation -> {REPORT_PATH}")
    print(f"Levels: pairs={total_pairs}, anomalies={len(level_anoms)} (thr {thr_levels}%)")
    print(f"Returns: pairs={total_ret_pairs}, anomalies={len(ret_anoms)} (thr {thr_returns} pct-pts), corr={ret_corr:.4f}")

    # Preview first lines
    with open(REPORT_PATH, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            print(line.rstrip())
            if i > 12:
                print("... (truncated)")
                break

if __name__ == "__main__":
    main()
