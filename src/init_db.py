import os, json, sqlite3

CONFIG_PATH = "config/config.json"

DDL_RAW = """
CREATE TABLE IF NOT EXISTS raw_prices (
  id INTEGER PRIMARY KEY,
  source TEXT NOT NULL,
  symbol TEXT NOT NULL,
  asset_class TEXT NOT NULL,
  ts TEXT NOT NULL,                 -- ISO8601 datetime of the tick
  price REAL NOT NULL,
  currency TEXT,
  ingest_ts TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  source_file TEXT,                 -- original file name
  checksum TEXT NOT NULL,
  UNIQUE(source, symbol, ts)
);
"""

DDL_LOG = """
CREATE TABLE IF NOT EXISTS ingestion_log (
  id INTEGER PRIMARY KEY,
  source TEXT,
  source_file TEXT,
  records_total INTEGER,
  inserted INTEGER,
  duplicates INTEGER,
  missing_fields INTEGER,
  ingest_started TEXT,
  ingest_finished TEXT
);
"""

# полезные индексы для последующих проверок/сверок
DDL_IDX = [
  "CREATE INDEX IF NOT EXISTS ix_raw_symbol_ts ON raw_prices(symbol, ts);",
  "CREATE INDEX IF NOT EXISTS ix_raw_source ON raw_prices(source);"
]

def load_cfg():
  with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    return json.load(f)

def main():
  cfg = load_cfg()
  db_path = cfg.get("database_path", "data/db/mdf.db")
  os.makedirs(os.path.dirname(db_path), exist_ok=True)

  conn = sqlite3.connect(db_path)
  cur = conn.cursor()
  cur.executescript(DDL_RAW)
  cur.executescript(DDL_LOG)
  for stmt in DDL_IDX:
    cur.execute(stmt)
  conn.commit()

  # быстрая проверка структуры
  cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
  tables = [r[0] for r in cur.fetchall()]
  print("[OK] DB:", db_path)
  print("[OK] Tables:", ", ".join(tables))

  # проверим столбцы raw_prices
  cur.execute("PRAGMA table_info(raw_prices);")
  cols = [r[1] for r in cur.fetchall()]
  print("[OK] raw_prices columns:", cols)

  conn.close()

if __name__ == "__main__":
  main()
