# Market Data Feeds – Ingestion, Quality Checks, and Reconciliation

*A showcase project simulating workflows for market data quality.*

---

## 📌 Project Overview
This project demonstrates an end-to-end market data pipeline, inspired by real-world ICE governance practices.

**Pipeline stages:**
1. **Ingestion**: Load vendor feeds (Stooq, Investing.com, EIA) into a normalized SQLite database.
2. **Quality Checks**: Validate duplicates, missing fields, and latency anomalies.
3. **Reconciliation**: Compare across vendors (AAPL: Stooq vs Investing) in two modes:
   - **Levels** – raw price comparison.
   - **Returns** – day-over-day % changes.
4. **Reporting & Visualization**: Generate KPI tables and plots with pandas & matplotlib.

**Business value:**

- Detect anomalies early in the pipeline.
- Distinguish real errors from vendor methodology bias.
- Provide transparent KPIs to operations and product teams.
- Scalable design: applicable to commodities, equities, or derivatives feeds.

---

## 📂 Repository Structure
market_data_feeds/
├── data/
│ ├── input/ # raw CSV / JSON feeds
│ ├── db/ # SQLite database
│ └── output/ # reports, plots
├── src/
│ ├── init_db.py
│ ├── fetch_real_feeds.py
│ ├── ingestion.py
│ ├── quality_checks.py
│ ├── reconciliation.py
│ └── utils/ # helper functions (normalizers, etc.)



For details see the notebook (feed_notebook.ipynb)
└── notebooks/
└── reconciliation_analysis.ipynb

---
