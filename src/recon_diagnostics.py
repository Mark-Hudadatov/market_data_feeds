
recon_diagnostics.py
Diagnose if reconciliation anomalies are systematic (scaling/time) rather than random errors.
Works on pairs (STOOQ vs INVESTING) for AAPL from SQLite.
"""

import sqlite3, statistics
from collections import defaultdict

DB = "data/db/mdf.db"

def fetch_pairs():
    q = """
    SELECT
      a.ts,
      a.source, a.price,
      b.source, b.price
    FROM raw_prices a
    JOIN raw_prices b
      ON a.symbol = b.symbol
     AND a.ts = b.ts
     AND a.source = 'INVESTING'
     AND b.source = 'STOOQ'
     AND a.symbol = 'AAPL'
    ORDER BY a.ts;
    """
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute(q)
    rows = cur.fetchall()
    conn.close()
    return rows

def pct_diff(a, b):
    if a == 0 and b == 0:
        return 0.0
    return 200.0 * abs(a - b) / (abs(a) + abs(b))

def main():
    rows = fetch_pairs()
    if not rows:
        print("No pairs found.")
        return

    # 1) basic stats of pct_diff
    diffs = [pct_diff(pa, pb) for _, _, pa, _, pb in rows]
    print(f"pairs: {len(diffs)}")
    print(f"pct_diff: median={statistics.median(diffs):.3f}%  mean={statistics.mean(diffs):.3f}%  p95={statistics.quantiles(diffs, n=20)[18]:.3f}%")

    # 2) look for constant multiplier k such that INVESTING ≈ k * STOOQ
    ratios = [pa / pb for _, _, pa, _, pb in rows if pb]
    k_med = statistics.median(ratios)
    print(f"median price ratio (INVESTING/STOOQ) = {k_med:.6f}")
    # If we divide INVESTING by k_med, residual pct_diff should shrink
    adj_diffs = [pct_diff(pa / k_med, pb) for _, _, pa, _, pb in rows]
    print(f"after de-bias by k: median={statistics.median(adj_diffs):.3f}%  mean={statistics.mean(adj_diffs):.3f}%")

    # 3) compare DAY-OVER-DAY returns agreement (scale-invariant)
    # build series in order
    inv = [pa for _, _, pa, _, _ in rows]
    stq = [pb for _, _, _, _, pb in rows]
    inv_ret = []
    stq_ret = []
    for i in range(1, len(inv)):
        try:
            inv_ret.append((inv[i] - inv[i-1]) / inv[i-1])
            stq_ret.append((stq[i] - stq[i-1]) / stq[i-1])
        except ZeroDivisionError:
            pass

    # correlation (Spearman-like quick approximation using ranks if stdlib only)
    def corr(x, y):
        import math
        n = len(x)
        if n != len(y) or n == 0:
            return float("nan")
        mx = sum(x)/n
        my = sum(y)/n
        num = sum((xi-mx)*(yi-my) for xi,yi in zip(x,y))
        denx = (sum((xi-mx)**2 for xi in x))**0.5
        deny = (sum((yi-my)**2 for yi in y))**0.5
        if denx == 0 or deny == 0:
            return float("nan")
        return num/(denx*deny)

    print(f"return correlation ≈ {corr(inv_ret, stq_ret):.4f}")
    print("Tip: if correlation ~1 but level diffs ~const → it is a definition/scale issue, not bad data.")

if __name__ == "__main__":
    main()
