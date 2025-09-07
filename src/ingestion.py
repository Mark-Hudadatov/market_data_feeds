"""
ingestion.py
- Reads config/config.json
- Loads raw files (CSV from Stooq, JSON from EIA)
- Normalizes to a single schema
- Inserts into SQLite (raw_prices, ingestion_log)
"""

import os, json, csv, sqlite3, hashlib, datetime
from typing import Dict, Any
from parsers.investing import normalize_investing_csv

CONFIG_PATH = "config/config.json"
REQUIRED_FIELDS = ["source","symbol","asset_class","ts","price"]

# --- helper functions ---

def now_iso() -> str:
    """Return current UTC time in ISO8601 with ms"""
    return datetime.datetime.utcnow().isoformat(timespec="milliseconds")+"Z"

def row_checksum(row: Dict[str, Any]) -> str:
    """Stable hash across key fields to detect duplicates/content drift"""
    m = hashlib.sha256()
    payload = f"{row.get('source','')}|{row.get('symbol','')}|{row.get('asset_class','')}|{row.get('ts','')}|{row.get('price','')}|{row.get('currency','')}"
    m.update(payload.encode("utf-8"))
    return m.hexdigest()

def ensure_db(db_path: str):
    """Ensure DB exists and schema is ready (init_db already created tables)"""
    return sqlite3.connect(db_path)

def insert_row(cur, row: Dict[str, Any], source_file: str) -> int:
    """Try to insert normalized row. Returns 1 if inserted, 0 if duplicate."""
    cur.execute(
        """INSERT OR IGNORE INTO raw_prices
           (source,symbol,asset_class,ts,price,currency,source_file,checksum)
           VALUES (?,?,?,?,?,?,?,?)""",
        (row["source"], row["symbol"], row["asset_class"], row["ts"],
         float(row["price"]), row.get("currency","USD"), source_file, row_checksum(row))
    )
    return cur.rowcount

def log_ingestion(cur, source, source_file, total, inserted, duplicates, missing, started, finished):
    """Write summary line into ingestion_log"""
    cur.execute("""INSERT INTO ingestion_log
                   (source, source_file, records_total, inserted, duplicates, missing_fields, ingest_started, ingest_finished)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (source, source_file, total, inserted, duplicates, missing, started, finished))

# --- mapping functions ---

def normalize_stooq_csv(path: str):
    """
    Read Stooq CSV (Date,Open,High,Low,Close,Volume) for AAPL.
    Map to unified schema.
    """
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                rows.append({
                    "source": "STOOQ",
                    "symbol": "AAPL",
                    "asset_class": "equity",
                    "ts": r["Date"]+"T00:00:00Z",
                    "price": r["Close"],
                    "currency": "USD"
                })
            except Exception:
                continue
    return rows

def normalize_eia_json(path: str):
    """
    Read EIA JSON (response.data with fields: period, value).
    Map to unified schema.
    """
    rows = []
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    for r in data.get("response", {}).get("data", []):
        try:
            rows.append({
                "source": "EIA",
                "symbol": "WTI",
                "asset_class": "commodity",
                "ts": r["period"]+"T00:00:00Z",
                "price": r["value"],
                "currency": "USD"
            })
        except Exception:
            continue
    return rows

# --- main processing ---

def process_file(conn, ftype: str, path: str):
    """
    Route the input file to the correct normalizer and load rows into SQLite.

    CSV routing (by filename):
      - contains 'invest'  -> Investing.com CSV
      - contains 'yahoo'   -> Yahoo CSV (if you kept it)
      - contains 'alpha'   -> Alpha Vantage CSV (if you kept it)
      - otherwise          -> Stooq CSV (default)

    JSON routing (by filename):
      - contains 'fmp'     -> FinancialModelingPrep JSON (if used)
      - otherwise          -> EIA JSON (default)
    """
    cur = conn.cursor()
    started = now_iso()
    total = inserted = duplicates = missing = 0
    rows = []

    fname = os.path.basename(path).lower()

    if ftype == "csv":
        if "invest" in fname:
            rows = normalize_investing_csv(path)
        else:
            rows = normalize_stooq_csv(path)

    elif ftype == "json":
            rows = normalize_eia_json(path)

    else:
        raise ValueError(f"Unsupported type: {ftype}")

    total = len(rows)
    source_name = rows[0]["source"] if rows else "UNKNOWN"

    for r in rows:
        if not all(r.get(k) for k in REQUIRED_FIELDS):
            missing += 1
            continue
        try:
            inserted += insert_row(cur, r, os.path.basename(path))
        except Exception:
            duplicates += 1

    conn.commit()
    finished = now_iso()
    log_ingestion(
        cur, source_name, os.path.basename(path),
        total, inserted, duplicates, missing, started, finished
    )
    conn.commit()

def main():
    with open(CONFIG_PATH,"r",encoding="utf-8") as f:
        cfg = json.load(f)
    conn = ensure_db(cfg["database_path"])

    # In config we know file types: AAPL = csv, INVESTING_AAPL = csv, WTI = json
    inputs = [
        {"type": "csv", "path": "data/input/aapl_stooq.csv"},
        {"type": "csv", "path": "data/input/aapl_investing.csv"},
        {"type": "json", "path": "data/input/wti_eia.json"},
    ]
    for spec in inputs:
        if os.path.exists(spec["path"]):
            print(f"[INGEST] {spec['path']}")
            process_file(conn, spec["type"], spec["path"])
        else:
            print(f"[SKIP] not found {spec['path']}")

    # Quick sanity check
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw_prices;")
    print("Total raw_prices:", cur.fetchone()[0])
    cur.execute("SELECT source, source_file, records_total, inserted, duplicates, missing_fields FROM ingestion_log ORDER BY id DESC;")
    for row in cur.fetchall():
        print("LOG:", row)

    conn.close()

if __name__ == "__main__":
    main()
