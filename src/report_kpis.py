
report_kpis.py
Robust KPI aggregator:
- Parses ONLY the first 'metric,value,notes' block from each report,
  ignoring later sections with different column counts.
- Writes data/output/kpi_summary.csv
"""

import csv
from pathlib import Path

QUALITY = Path("data/output/quality_report.csv")
RECON   = Path("data/output/reconciliation_report.csv")
OUT     = Path("data/output/kpi_summary.csv")

def read_metrics_block(path: Path) -> dict:
    """
    Read the top block with header ['metric','value','notes'] and return {metric: value}.
    Stops on first empty line or when row has != 3 columns.
    """
    metrics = {}
    if not path.exists():
        return metrics
    with path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        # tolerate BOM / spacing
        header = [h.strip().lower() for h in header]
        if header[:3] != ["metric", "value", "notes"]:
            # Not the expected header → try to scan until we find it
            for row in reader:
                row_l = [c.strip().lower() for c in row]
                if row_l[:3] == ["metric", "value", "notes"]:
                    header = row_l
                    break
            else:
                return metrics  # header not found

        for row in reader:
            # stop at blank line or section change
            if not row or len(row) < 3:
                break
            key = row[0].strip()
            val = row[1].strip()
            # try numeric cast where possible
            try:
                if "." in val:
                    metrics[key] = float(val)
                else:
                    metrics[key] = int(val)
            except Exception:
                metrics[key] = val
    return metrics

def main():
    q = read_metrics_block(QUALITY)
    r = read_metrics_block(RECON)

    total_rows  = int(q.get("total_rows", 0))
    dup_rows    = int(q.get("duplicate_rows", 0))
    pairs       = int(r.get("total_pairs_compared", 0))
    anomalies   = int(r.get("anomalies_over_threshold", 0))
    anomaly_pct = round(100.0 * anomalies / max(pairs, 1), 2)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["metric","value"])
        w.writerow(["total_raw_rows", total_rows])
        w.writerow(["duplicate_rows", dup_rows])
        w.writerow(["pairs_compared", pairs])
        w.writerow(["anomalies_over_threshold", anomalies])
        w.writerow(["anomaly_rate_pct", anomaly_pct])

    print("[OK] KPI summary ->", OUT)
    with OUT.open("r", encoding="utf-8") as f:
        print(f.read())

if __name__ == "__main__":
    main()
