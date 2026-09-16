# Sectra

Tracks sector-wide market trends. A batch ETL + analytics pipeline that
fetches real NSE stock data, processes it with PySpark, loads it into
Postgres, and analyzes cross-stock and sector-level behavior.

**Architecture:** API fetch (Python/yfinance) → raw storage → PySpark (transform) → Postgres (load) → cross-stock analysis

```
 yfinance API (NSE tickers)
      |
      |  extract.py
      v
 Raw CSVs  (raw/)
      |
      |  transform.py (PySpark)
      v
 Clean + validated data (daily return, moving averages, volatility)
      |
      |  load.py
      v
 Postgres
   |-- clean_prices        (valid rows, upserted on ticker+date)
   |-- data_quality_log    (rows that failed validation)
      |
      |  analyze.py
      v
 Sector summary + correlation matrix + run summary
```

## Setup (Docker)

Requires only Docker installed on your machine — no local Java or Python
environment needed. Java and all Python dependencies live inside the image.

1. Copy `.env.example` to `.env` and fill in your Postgres credentials:
```bash
   cp .env.example .env
```

2. Build and start the container:
```bash
   docker compose build
   docker compose up -d
```

3. Open a shell inside the running container:
```bash
   docker compose exec pipeline bash
```

4. Verify everything works (run this inside the container shell):
```bash
   python src/verify_setup.py
```
   You should see a sample Spark DataFrame print, then a Postgres version string.

**Note:** if you edit `.env` after the container is already running, the
running container won't pick up the change automatically — recreate it with
`docker compose down && docker compose up -d` first.

**Day-to-day workflow:** edit code on your host machine as normal (the
`volumes` mount in `docker-compose.yml` syncs it live), then run/test inside
the container shell. You only rebuild the image (`docker compose build`) if
you change `requirements.txt` or the `Dockerfile` itself.

## Running the pipeline

Run the whole thing in one command, inside the container shell:
```bash
python run_pipeline.py
```
This runs extract → transform → load → analyze in sequence, stopping
immediately if any stage fails, with timing logged for each stage.

Or run each stage individually:

```bash
python src/extract.py
```
Fetches OHLCV data for a set of NSE-listed Indian stocks via yfinance and
saves each as a raw, untouched CSV in `raw/`.

```bash
python src/transform.py
```
Reads the raw CSVs into a PySpark DataFrame, cleans and validates the data,
and computes derived metrics (daily returns, moving averages, volatility)
using window functions.

```bash
python src/load.py
```
Writes the cleaned data to a staging table via Spark's JDBC writer, then
upserts it into `clean_prices` via a SQL `ON CONFLICT` statement (Spark's
JDBC writer has no native upsert, so this step uses psycopg2 directly).
Rows that failed validation are appended to a separate `data_quality_log`
table.

```bash
python src/analyze.py
```
Reads the loaded data back out and produces:
- **Sector summary** — average daily return and volatility grouped by
  sector (IT, Banking, Finance/NBFC, etc.)
- **Correlation matrix** — pairwise correlation of daily returns across
  all tickers, showing which stocks move together
- **Run summary** — top gainer/loser on the latest day, most volatile
  ticker overall, and basic load stats

Analytical SQL queries (top movers, volume spikes, cumulative returns) are
also available in `queries.sql` for direct use in Neon's SQL editor or psql.

## Design notes

- **Raw/clean separation:** `extract.py` writes data exactly as fetched, with
  no cleaning. This means if a bug is found in the transform logic later,
  the pipeline can be re-run on the same raw data without re-fetching from
  the API — a standard ETL resilience pattern.
- **Why Spark:** the transform layer is built on PySpark so the design
  reflects how it would scale at real trading-data volumes (many
  instruments, tick-level granularity) where a single-machine job breaks down.
- **Why Docker:** bundles the JVM (required by Spark) and all Python
  dependencies into a single reproducible image, so the environment doesn't
  depend on what's installed on any one machine.
- **Data quality:** rows that fail validation (e.g. missing values, negative
  prices) are routed to a quarantine table instead of silently dropped,
  so failures are visible and auditable.
- **Idempotency:** loads use upsert logic keyed on (ticker, date), so
  re-running the pipeline doesn't create duplicate rows.
- **Sector tagging:** sector classification is domain knowledge, not a fact
  derivable from price data, so it's applied in `analyze.py` at read time
  rather than stored as a column in `clean_prices`.
- **Findings worth noting:** in testing, TCS and Infosys (both large IT
  services firms) showed the highest pairwise correlation (0.80) of any
  pair, while cross-sector pairs (e.g. IT vs. Banking) correlated far more
  weakly — a sanity check that the underlying data and metrics behave the
  way real market structure would predict.