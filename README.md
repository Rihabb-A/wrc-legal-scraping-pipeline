# wrc-legal-scraping-pipeline

A Scrapy pipeline that scrapes legal decisions from the Irish
[Workplace Relations](https://www.workplacerelations.ie/en/search/?decisions=1)
website, stores metadata in MongoDB and the raw documents in MinIO, then
transforms them into a curated layer. Ingestion and transformation run as
separate Dagster ops with a real dependency between them.

Design decisions and their reasoning are in [ARCHITECTURE.md](ARCHITECTURE.md).
The evidence they are based on — measured against the live site — is in
[docs/recon.md](docs/recon.md).

```
                  ┌──────────────────────────────────────────┐
  start_date ───► │ INGESTION   scrapy crawl decisions       │
  end_date        │  partition the range (monthly)           │
                  │  search all four bodies                  │
                  │  download each decision                  │
                  └───────────────┬──────────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
             MongoDB: metadata            MinIO: wrc-landing
             landing_documents            raw bytes, unmodified
                    │                           │
                    └─────────────┬─────────────┘
                                  ▼
                  ┌──────────────────────────────────────────┐
                  │ TRANSFORMATION  transform.transform      │
                  │  HTML  → strip everything but the        │
                  │          decision (BeautifulSoup)        │
                  │  PDF/DOC → pass through untouched        │
                  │  rename to identifier.ext, re-hash       │
                  └───────────────┬──────────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
             MongoDB: metadata            MinIO: wrc-curated
             curated_documents            cleaned documents
```

---

## Prerequisites

| Tool | Version used |
|---|---|
| Python | 3.11+ (developed on 3.13) |
| Docker Desktop | with Compose v2 |
| Git | any recent version |

---

## 1. Installation

```powershell
git clone <repository-url>
cd wrc-legal-scraping-pipeline

python -m venv .venv
.venv\Scripts\Activate.ps1          # note the leading .\ on PowerShell
pip install -r requirements.txt

Copy-Item .env.example .env
```

Your prompt should now start with `(.venv)`. If it does not, the commands
below will not find `scrapy` — see [Troubleshooting](#troubleshooting).

Verify:

```powershell
python --version
scrapy list                          # should print: decisions
```

---

## 2. Start MongoDB and MinIO

```powershell
docker compose up -d
docker compose ps
```

Both `wrc-mongo` and `wrc-minio` should report `(healthy)`. A third
container, `wrc-minio-init`, creates the two buckets and then exits — a
status of `Exited (0)` is success, not a problem.

```powershell
docker compose logs minio-init       # should end with "--- buckets ready ---"
```

| Service | Host port | Purpose |
|---|---|---|
| MongoDB | 27017 | metadata |
| MinIO API | 9000 | object storage, used by the pipeline |
| MinIO Console | 9001 | web UI for humans |

All three are set in `.env` and can be changed there if a port is already in
use on your machine.

---

## 3. Run the pipeline

### Option A — Dagster (recommended)

```powershell
dagster dev -f orchestration/definitions.py
```

Open <http://localhost:3000>, go to **Jobs → wrc_pipeline → Launchpad**,
paste the run config below and click **Launch Run**:

```yaml
ops:
  ingest_decisions:
    config:
      start_date: "2025-08-01"
      end_date: "2025-08-31"
  transform_decisions:
    config:
      start_date: "2025-08-01"
      end_date: "2025-08-31"
```

`transform_decisions` will not start until `ingest_decisions` has finished,
and will not run at all if ingestion failed or left any record unaccounted
for.

### Option B — command line

The same two stages, run directly:

```powershell
# Ingestion
scrapy crawl decisions -a start_date=2025-08-01 -a end_date=2025-08-31

# Transformation
python -m transform.transform --start-date 2025-08-01 --end-date 2025-08-31
```

---

## 4. Command reference

### Ingestion

```powershell
scrapy crawl decisions -a start_date=YYYY-MM-DD -a end_date=YYYY-MM-DD [options]
```

| Argument | Default | Meaning |
|---|---|---|
| `start_date` | *required* | First day, inclusive, `YYYY-MM-DD` |
| `end_date` | *required* | Last day, inclusive |
| `partition_size` | `monthly` | `daily`, `weekly`, `monthly` or `yearly` |
| `bodies` | all four | Comma-separated body names |
| `refresh` | `false` | Re-download everything, ignoring what is already stored |

Examples:

```powershell
# One month, all four bodies
scrapy crawl decisions -a start_date=2025-08-01 -a end_date=2025-08-31

# One body only (quote names containing spaces)
scrapy crawl decisions -a start_date=2025-08-01 -a end_date=2025-08-31 -a "bodies=Labour Court"

# A full year in weekly partitions
scrapy crawl decisions -a start_date=2024-01-01 -a end_date=2024-12-31 -a partition_size=weekly

# Rebuild the landing zone after changing parsing logic
scrapy crawl decisions -a start_date=2025-08-01 -a end_date=2025-08-31 -a refresh=true
```

### Transformation

```powershell
python -m transform.transform --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--refresh]
```

Transformation reads only from the landing zone and writes only to the
curated zone, so it is safe to re-run at any time with improved cleaning
rules — no re-scraping required.

---

## 5. Inspecting the results

### MinIO — the documents

**Web console:** <http://localhost:9001> — log in with the
`MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` from your `.env`
(`minioadmin` / `minioadmin` by default). Browse **wrc-landing** or
**wrc-curated**, click any object and choose *Preview* or *Download*.

**From Python:**

```powershell
python -c "from storage.minio import MinioStore; from common.config import env, env_bool; s = MinioStore(env('MINIO_ENDPOINT'), env('MINIO_ACCESS_KEY'), env('MINIO_SECRET_KEY'), env('MINIO_LANDING_BUCKET'), env('MINIO_CURATED_BUCKET'), env_bool('MINIO_SECURE', False)).connect(); print('landing:', len(s.list_keys('wrc-landing'))); print('curated:', len(s.list_keys('wrc-curated'))); print(*sorted(s.list_keys('wrc-curated'))[:5], sep=chr(10))"
```

Save one curated document to disk and open it in a browser:

```powershell
python -c "from storage.minio import MinioStore; from common.config import env, env_bool; s = MinioStore(env('MINIO_ENDPOINT'), env('MINIO_ACCESS_KEY'), env('MINIO_SECRET_KEY'), env('MINIO_LANDING_BUCKET'), env('MINIO_CURATED_BUCKET'), env_bool('MINIO_SECURE', False)).connect(); k = sorted(s.list_keys('wrc-curated'))[0]; open('sample.html','wb').write(s.download('wrc-curated', k)); print('wrote sample.html from', k)"
start sample.html
```

### MongoDB — the metadata

**Command line**, via the running container:

```powershell
# Counts
docker exec -it wrc-mongo mongosh wrc --quiet --eval "db.landing_documents.countDocuments({})"
docker exec -it wrc-mongo mongosh wrc --quiet --eval "db.curated_documents.countDocuments({})"

# One landing record
docker exec -it wrc-mongo mongosh wrc --quiet --eval "db.landing_documents.findOne()"

# One curated record, showing its lineage back to the raw document
docker exec -it wrc-mongo mongosh wrc --quiet --eval "db.curated_documents.findOne({}, {identifier:1, file_path:1, file_hash:1, source_doc_url:1, source_file_path:1, source_file_hash:1})"

# Look up a specific decision
docker exec -it wrc-mongo mongosh wrc --quiet --eval "db.landing_documents.find({identifier: 'DWT2532'}).pretty()"

# How many per partition
docker exec -it wrc-mongo mongosh wrc --quiet --eval "db.landing_documents.aggregate([{\$group:{_id:'\$partition_date', n:{\$sum:1}}}, {\$sort:{_id:1}}])"

# The indexes
docker exec -it wrc-mongo mongosh wrc --quiet --eval "db.landing_documents.getIndexes()"
```

**GUI:** install [MongoDB Compass](https://www.mongodb.com/products/compass)
and connect to `mongodb://localhost:27017`, database `wrc`.

### Logs

Human-readable output goes to the console. A machine-readable record of every
run is appended to `structured_log.jsonl`, one JSON object per line:

```powershell
# The end-of-run summaries
Get-Content structured_log.jsonl | Select-String '"run_summary"'

# Any failures, each with its URL and reason
Get-Content structured_log.jsonl | Select-String '"download_failed"|"transform_failed"'

# Pretty-print the last event
python -c "from common.logging_config import read_events; import json; print(json.dumps(read_events('structured_log.jsonl')[-1], indent=2))"
```

The field that matters is **`unaccounted`**. It must be `0`: every record the
site reported was either downloaded, deliberately skipped as unchanged, or
explicitly failed with a logged reason.

---

## 6. Running the tests

```powershell
pytest                    # 147 tests
pytest -v                 # with names
pytest tests/test_partitioning.py
```

No network and no containers are required. The tests run against real pages
saved under `tests/fixtures/`, so they give the same answer whether or not
workplacerelations.ie is reachable.

| File | Covers |
|---|---|
| `test_partitioning.py` | Date ranges split with no gaps and no overlaps |
| `test_hashing.py` | SHA-256 vectors, and that an unchanged page compares as unchanged |
| `test_html_cleaning.py` | Site furniture removed, legal text and tables kept |
| `test_spider_parsing.py` | Selectors, normalisation, pagination, argument validation |

---

## 7. Verifying idempotency

Run the same range twice and compare:

```powershell
scrapy crawl decisions -a start_date=2025-08-01 -a end_date=2025-08-31 -a "bodies=Labour Court"
scrapy crawl decisions -a start_date=2025-08-01 -a end_date=2025-08-31 -a "bodies=Labour Court"
```

The second run should report `downloaded=0 unchanged=19`, with
`MONGO: inserted=0` and `MINIO: stored=0`. Measured over 994 documents, a
repeat run drops from 1,110 HTTP requests to 112 — the remainder being the
search listings, which must always be re-fetched because that is how newly
published decisions are discovered.

---

## Repository layout

| Path | Purpose |
|---|---|
| `wrc_scraper/spiders/decisions.py` | The spider: what to fetch and how to read it |
| `wrc_scraper/constants.py` | Facts about the site — body IDs, selectors, search URL |
| `wrc_scraper/pipelines.py` | Storing each scraped record |
| `wrc_scraper/settings.py` | Concurrency, retries, timeouts, all from `.env` |
| `common/partitioning.py` | Splitting a date range into work units |
| `common/hashing.py` | SHA-256 fingerprints |
| `common/paths.py` | Object keys for the landing and curated zones |
| `common/logging_config.py` | Structured JSON logging |
| `common/config.py` | Reading configuration from `.env` |
| `storage/mongo.py` | MongoDB adapter |
| `storage/minio.py` | MinIO adapter |
| `transform/html_cleaner.py` | Reducing a page to the legal decision |
| `transform/transform.py` | Landing → Curated |
| `orchestration/definitions.py` | Dagster job |
| `docker-compose.yml` | MongoDB + MinIO |
| `docs/recon.md` | Measured findings about the site |

---

## Troubleshooting

**`scrapy : The term 'scrapy' is not recognized`**
The virtual environment is not active. Run `.venv\Scripts\Activate.ps1` —
including the leading `.\` — and check your prompt starts with `(.venv)`.
Alternatively call it directly: `.\.venv\Scripts\scrapy.exe crawl decisions ...`

**`running scripts is disabled on this system`**
PowerShell's execution policy. Allow it for your user only:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

**`Bind for 0.0.0.0:9000 failed: port is already allocated`**
Another program is using that port. Change it in `.env` and restart —
no code change is needed:

```
MINIO_PORT=9002
MINIO_CONSOLE_PORT=9003
MINIO_ENDPOINT=localhost:9002
```

**`ServerSelectionTimeoutError: localhost:27017`**
MongoDB is not running. Start Docker Desktop, then `docker compose up -d`.
Containers stop when Docker Desktop shuts down.

**A run reports `failed` greater than zero**
Look up the reason and URL:

```powershell
Get-Content structured_log.jsonl | Select-String '"download_failed"'
```

Re-running the same range retries only the records that did not succeed;
everything already stored is skipped.

**Starting over**

```powershell
docker compose down -v      # deletes all stored data
docker compose up -d
```
