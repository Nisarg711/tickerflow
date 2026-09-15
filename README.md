# TickerFlow

A batch ETL pipeline that fetches real market data, processes it with PySpark,
and loads it into Postgres for querying and visualization.

**Architecture:** API fetch (Python) → raw storage → PySpark (transform) → Postgres (load) → FastAPI/Streamlit (serve)

## Status
- [x] Phase 0: Environment setup
- [ ] Phase 1: Extract
- [ ] Phase 2: Transform
- [ ] Phase 3: Load
- [ ] Phase 4: Orchestrate
- [ ] Phase 5: Serve

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

**Day-to-day workflow:** edit code on your host machine as normal (the
`volumes` mount in `docker-compose.yml` syncs it live), then run/test inside
the container shell. You only rebuild the image (`docker compose build`) if
you change `requirements.txt` or the `Dockerfile` itself.

## Design notes
(to be filled in as the pipeline is built — decisions on raw/clean separation,
why Spark, incremental loads, etc.)
