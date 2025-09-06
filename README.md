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

**Business value (ICE-style):**
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
└── notebooks/
└── reconciliation_analysis.ipynb


## 🛠 How to Run

### 1. Setup
```bash
git clone <this-repo>
cd market_data_feeds
python3 -m venv venv
source venv/bin/activate
pip install pandas matplotlib

##
### 2. Run pipeline
python src/init_db.py
python src/fetch_real_feeds.py
python src/ingestion.py
python src/quality_checks.py
python src/reconciliation.py

##
### 3. Explore

Open the notebook:

notebooks/reconciliation_analysis.ipynb
