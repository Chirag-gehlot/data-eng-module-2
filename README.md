# ⚡ Data Engineering Zoomcamp — Module 2: Workflow Orchestration with Kestra

> My hands-on work for **Module 2** of the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) — covering workflow orchestration using **Kestra**, with data pipelines that load NYC Taxi data into both **PostgreSQL** and **Google Cloud (GCS + BigQuery)**.

---

## 📖 Module Overview

Module 2 introduces **workflow orchestration** — automating, scheduling, and monitoring data pipelines. The module uses [Kestra](https://kestra.io/), an open-source orchestration platform, to build progressively more complex flows: from a simple Hello World to a full cloud data lake pipeline loading NYC Taxi data into BigQuery via GCS.

---

## 🏗️ Infrastructure Setup

The entire local environment runs via **Docker Compose** (`docker-compose.yaml`) with four services:

| Service | Image | Purpose | Port |
|---------|-------|---------|------|
| `kestra` | `kestra/kestra:v1.1` | Orchestration UI & engine | `8080`, `8081` |
| `kestra_postgres` | `postgres:18` | Kestra's internal metadata store | — |
| `pgdatabase` | `postgres:18` | NY Taxi data warehouse | `5433` |
| `pgadmin` | `dpage/pgadmin4` | Database GUI | `8085` |

**Kestra** is configured with:
- PostgreSQL as its repository, queue, and storage backend
- GCP credentials mounted at `/app/creds/gcp-creds.json`
- Docker socket access for running Python tasks in containers
- Basic auth login: `admin@kestra.io` / `Admin1234!`

```bash
docker-compose up -d
```

Access Kestra UI at **http://localhost:8080**

---

## 🔄 Kestra Flows

All flows live in `kestra flows/` under the `zoomcamp` namespace and progress from basic concepts to a full production-style GCP pipeline.

---

### Flow 01 — Hello World (`01_hello_world.yaml`)

Introduces core Kestra concepts:

- **Inputs** — a `STRING` input with a default value (`Will`)
- **Variables** — a `welcome_message` variable rendered with Jinja templating
- **Tasks**: logging messages, generating outputs, sleeping for 15 seconds, then logging the output from a previous task
- **`pluginDefaults`** — setting the log level to `ERROR` for all Log tasks
- **Schedule trigger** — a daily cron at `0 10 * * *` (disabled by default), passing a different input (`Sarah`)
- **Concurrency control** — limiting to 2 concurrent executions with `FAIL` behaviour

---

### Flow 02 — Python in Docker (`02_python.yaml`)

Demonstrates running Python scripts inside Docker containers via Kestra:

- Uses `io.kestra.plugin.scripts.python.Script` with a **Docker task runner**
- Runs in a `python:slim` container with `requests` and `kestra` installed on the fly
- Fetches the Docker Hub download count for the `kestra/kestra` image via the Docker Hub API
- Uses `Kestra.outputs()` to pass values back to the workflow as named outputs

---

### Flow 03 — Getting Started: Data Pipeline (`03_getting_started_data_pipeline.yaml`)

A complete **Extract → Transform → Query** pipeline using a products dataset:

- **Extract** — downloads JSON data from `https://dummyjson.com/products` using `io.kestra.plugin.core.http.Download`
- **Transform** — runs a Python script in a `python:3.11-alpine` container that filters the JSON to only keep user-specified columns (passed via `ARRAY` input and an env variable)
- **Query** — uses **DuckDB** (`io.kestra.plugin.jdbc.duckdb.Queries`) to run SQL directly on the output JSON file, computing average price per brand with `GROUP BY` and `ORDER BY`

---

### Flow 04 — NYC Taxi → PostgreSQL (`04_postgres_taxi.yaml`)

Loads NYC Yellow or Green Taxi CSV data into a local PostgreSQL database.

**Inputs**: taxi type (`yellow`/`green`), year (`2019`/`2020`), month (`01`–`12`)

**Pipeline steps:**
1. **Label** the execution with taxi type and filename for traceability
2. **Extract** — downloads the gzipped CSV from DataTalksClub GitHub releases and decompresses it with `wget | gunzip`
3. **Conditional branching** (`io.kestra.plugin.core.flow.If`) — separate task chains for Yellow vs Green taxi schemas
4. For each taxi type:
   - Creates the main table and a **staging table** with `CREATE TABLE IF NOT EXISTS`
   - **Truncates** the staging table before each load
   - **Bulk copies** the CSV into staging using `io.kestra.plugin.jdbc.postgresql.CopyIn`
   - **Generates a `unique_row_id`** via `MD5` hash of key trip attributes (vendor, pickup/dropoff times, locations, fare, distance)
   - **Tags each row** with the source filename
   - **Merges** staging into the main table using `MERGE ... WHEN NOT MATCHED` — making the load **idempotent** (safe to re-run without duplicates)
5. **Purges** temporary files after execution

---

### Flow 05 — NYC Taxi → PostgreSQL (Scheduled) (`05_postgres_taxi_scheduled.yaml`)

The same pipeline as Flow 04, but fully **automated with monthly cron triggers**:

- `green_schedule`: runs on the 1st of each month at `09:00` (`0 9 1 * *`)
- `yellow_schedule`: runs on the 1st of each month at `10:00` (`0 10 1 * *`)
- Filename and data variables are derived from `trigger.date` — no manual input needed
- **Concurrency limit of 1** — prevents overlapping runs
- Supports **backfilling** historical months via the Kestra UI (label `backfill:true` recommended for tracking)

---

### Flow 06 — GCP Key-Value Store (`06_gcp_kv.yaml`)

Sets up shared GCP configuration as **Kestra KV Store** entries — a central config referenced by all downstream GCP flows:

| Key | Value |
|-----|-------|
| `GCP_PROJECT_ID` | `kestra-sandbox-497507` |
| `GCP_LOCATION` | `europe-west2` |
| `GCP_BUCKET_NAME` | `chirag-kestra-2026-unique-001` |
| `GCP_DATASET` | `zoomcamp` |

---

### Flow 07 — GCP Infrastructure Setup (`07_gcp_setup.yaml`)

Provisions GCP resources using KV store values from Flow 06:

- **Creates a GCS bucket** (`io.kestra.plugin.gcp.gcs.CreateBucket`) — `REGIONAL` storage class, skips if already exists
- **Creates a BigQuery dataset** (`io.kestra.plugin.gcp.bigquery.CreateDataset`) — skips if already exists
- Uses `pluginDefaults` to inject GCP project, location, and bucket into all GCP plugin tasks automatically

---

### Flow 08 — NYC Taxi → GCS + BigQuery (`08_gcp_taxi.yaml`)

The full cloud data lake pipeline — loads NYC Taxi CSV data to **GCS**, then ingests it into **BigQuery**.

**Inputs**: taxi type, year, month (`allowCustomValue: true` on year for custom input)

**Pipeline steps:**
1. **Extract** — downloads and decompresses the CSV (same approach as Flow 04)
2. **Upload to GCS** — uploads the raw CSV to `gs://<bucket>/<filename>` using `io.kestra.plugin.gcp.gcs.Upload`
3. **Conditional branching** for Yellow vs Green taxi, each doing:
   - **Create partitioned BigQuery table** (`PARTITION BY DATE(pickup_datetime)`) with full column-level descriptions
   - **Create external table** pointing directly at the GCS CSV file
   - **Create temp table** by selecting from the external table and computing `MD5` unique row IDs
   - **Merge** the temp table into the main partitioned table — idempotent, no duplicates on re-runs
4. **Purge** Kestra execution files after the run

---

## 🗂️ Repository Structure

```
.
├── docker-compose.yaml
└── kestra flows/
    ├── zoomcamp.01_hello_world.yaml                    # Kestra basics: inputs, vars, tasks, schedule
    ├── zoomcamp.02_python.yaml                         # Python in Docker, Kestra outputs
    ├── zoomcamp.03_getting_started_data_pipeline.yaml  # ETL: HTTP → Python → DuckDB
    ├── zoomcamp.04_postgres_taxi.yaml                  # NYC Taxi → PostgreSQL (manual)
    ├── zoomcamp.05_postgres_taxi_scheduled.yaml        # NYC Taxi → PostgreSQL (scheduled + backfill)
    ├── zoomcamp.06_gcp_kv.yaml                         # GCP config via Kestra KV store
    ├── zoomcamp.07_gcp_setup.yaml                      # Provision GCS bucket + BigQuery dataset
    └── zoomcamp.08_gcp_taxi.yaml                       # NYC Taxi → GCS → BigQuery
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Kestra v1.1 | Workflow orchestration |
| Docker / Docker Compose | Local environment |
| PostgreSQL 18 | Local data warehouse & Kestra backend |
| pgAdmin 4 | Database GUI |
| DuckDB | In-flow SQL queries on files |
| Google Cloud Storage | Data lake (raw CSV storage) |
| BigQuery | Cloud data warehouse |
| Python (via Docker) | Scripted transformations in flows |

---

## 🚀 Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running
- A GCP service account JSON key file

### 1. Add GCP credentials

```bash
cp /path/to/your/key.json ./gcp-creds.json
```

### 2. Start the stack

```bash
docker-compose up -d
```

### 3. Open Kestra

Go to **http://localhost:8080** and log in with `admin@kestra.io` / `Admin1234!`

### 4. Import flows

In the Kestra UI, navigate to **Flows → Import** and upload the YAML files from `kestra flows/`.

### 5. Run flows in order

Run **06 → 07** once to configure GCP, then trigger **08** to load data into BigQuery. For local PostgreSQL pipelines, use flows **04** or **05**.

---

## 📚 Resources

- [DataTalksClub DE Zoomcamp — Module 2](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/02-workflow-orchestration)
- [Kestra Documentation](https://kestra.io/docs)
- [NYC TLC Trip Data](https://github.com/DataTalksClub/nyc-tlc-data)
- [Course YouTube Playlist](https://www.youtube.com/playlist?list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

---

## 🙌 Acknowledgements

Thanks to [Alexey Grigorev](https://linkedin.com/in/agrigorev) and the DataTalksClub team for this excellent free course.
