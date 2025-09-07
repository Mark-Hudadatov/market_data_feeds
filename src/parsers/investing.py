# src/parsers/investing.py
"""
Robust Investing.com CSV normalizer.

Tolerates:
- Encodings: utf-8 / utf-8-sig / utf-16 / cp1252
- Delimiters: comma / semicolon / pipe / tab
- Messy headers (spaces, NBSP, dots, percent signs)
- Numbers with thousand separators and decimal commas
- Date format: MM/DD/YYYY -> ISO 'YYYY-MM-DDT00:00:00Z'
"""

import csv
import datetime
from typing import List, Dict, Any

def _norm_header(h: str) -> str:
    # Lowercase, remove NBSP, trim, keep alnum only
    h = (h or "").replace("\xa0", " ").strip().lower()
    return "".join(ch for ch in h if ch.isalnum())  # 'change %' -> 'change', 'vol.' -> 'vol'

def _parse_number(s: str) -> float:
    if s is None:
        raise ValueError("empty")
    s = s.replace("\xa0", "").strip()
    if s in ("", "-"):
        raise ValueError("empty")
    s = s.replace("−", "-")  # minus variant
    # If both '.' and ',' present → assume ',' is thousands
    if "," in s and "." in s:
        s = s.replace(",", "")
    elif "," in s and "." not in s:
        # decimal comma
        s = s.replace(".", "")
        s = s.replace(",", ".")
    return float(s)

def normalize_investing_csv(path: str) -> List[Dict[str, Any]]:
    encodings = ("utf-8", "utf-8-sig", "utf-16", "cp1252")
    for enc in encodings:
        try:
            with open(path, newline="", encoding=enc) as f:
                sample = f.read(4096)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
                except Exception:
                    class _D:  # fallback
                        delimiter = ","
                        quotechar = '"'
                    dialect = _D()
                reader = csv.DictReader(f, dialect=dialect)

                raw_headers = reader.fieldnames or []
                cleaned_headers = [(h or "").replace("\xa0", " ").strip() for h in raw_headers]
                norm_map = {_norm_header(h): h for h in cleaned_headers}

                date_key  = norm_map.get("date")
                price_key = norm_map.get("price") or norm_map.get("close")
                if not date_key or not price_key:
                    # headers not recognized under this encoding; try next
                    continue

                rows = []
                for r in reader:
                    try:
                        dt = datetime.datetime.strptime((r[date_key] or "").strip(), "%m/%d/%Y").date()
                        ts_iso = dt.isoformat() + "T00:00:00Z"
                        price = _parse_number(r[price_key])
                        rows.append({
                            "source": "INVESTING",
                            "symbol": "AAPL",
                            "asset_class": "equity",
                            "ts": ts_iso,
                            "price": price,
                            "currency": "USD"
                        })
                    except Exception:
                        continue
                if rows:
                    rows.sort(key=lambda x: x["ts"])
                    return rows
        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            return []
    return []
