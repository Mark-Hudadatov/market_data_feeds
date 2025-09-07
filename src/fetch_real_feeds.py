import os
import json
import urllib.request
import urllib.parse
import ssl
from datetime import date, timedelta

CONFIG_PATH = "config/config.json"
INPUT_DIR = "data/input"

def ensure_dirs():
    os.makedirs(INPUT_DIR, exist_ok=True)

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def fetch_stooq_aapl_csv():

    base_url = "https://stooq.com/q/d/l/"
    params = {"s": "aapl.us", "i": "d"}  
    url = base_url + "?" + urllib.parse.urlencode(params)
    out_path = os.path.join(INPUT_DIR, "aapl_stooq.csv")

    print(f"[STOOQ] GET {url}")
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(url, context=ctx, timeout=30) as resp:
        data = resp.read()

    with open(out_path, "wb") as f:
        f.write(data)

    print(f"[STOOQ] saved -> {out_path} ({len(data)} bytes)")
    return out_path

def fetch_eia_wti_json(api_key: str):
    #  EIA v2: /v2/petroleum/pri/spt/data/
    # RWTC — WTI spot price, daily
    base = "https://api.eia.gov/v2/petroleum/pri/spt/data/"

    end = date.today()
    start = end - timedelta(days=365)

    params = {
        "api_key": api_key,
        "frequency": "daily",
        "data[]": "value",
        "facets[series][]": "RWTC",   # WTI Spot
        "start": start.isoformat(),
        "end": end.isoformat()
    }
    url = base + "?" + urllib.parse.urlencode(params, doseq=True)
    out_path = os.path.join(INPUT_DIR, "wti_eia.json")

    print(f"[EIA] GET {url}")
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(url, context=ctx, timeout=30) as resp:
        data = resp.read()

    with open(out_path, "wb") as f:
        f.write(data)

    print(f"[EIA] saved -> {out_path} ({len(data)} bytes)")
    return out_path

def main():
    ensure_dirs()
    cfg = load_config()

    # 1) Stooq CSV
    try:
        fetch_stooq_aapl_csv()
    except Exception as e:
        print("[STOOQ] ERROR:", e)
        print("> Если корпоративная сеть блокирует запросы, можно скачать CSV вручную в браузере:")
        print("  https://stooq.com/q/d/l/?s=aapl.us&i=d  -> сохранить как data/input/aapl_stooq.csv")

    # 2) EIA JSON
    try:
        api_key = cfg.get("eia_api_key", "")
        if not api_key or api_key == "YOUR_EIA_KEY":
            raise ValueError("EIA API key is missing in config/config.json")
        fetch_eia_wti_json(api_key)
    except Exception as e:
        print("[EIA] ERROR:", e)
        print("> Если блокировка сети/прокси, альтернативно:")
        print("  - Открой https://api.eia.gov/ (личный ключ) и собери URL в их API Browser")
        print("  - Сохрани JSON вручную как data/input/wti_eia.json")


if __name__ == "__main__":
    main()
