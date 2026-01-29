# Redset Pipeline Architecture Review & Proposed Design

## Review of Current Architecture

**Overall assessment:** Your proposed flow is a solid foundation: replay producer → Kafka → dual consumers → DuckDB (hot) + S3/Redshift (cold) → Streamlit dashboard. It already covers separation of concerns, real-time vs. historical analytics, and buffering. Below are targeted suggestions to improve correctness, scalability, and observability.

## Architecture Explained in Simple Terms (Layman-Friendly)

Think of this project like **replaying a movie of database activity**. The Redset dataset is the movie. We want to play it **60× faster** (1 minute = 1 hour of real time), clean it up, store it, analyze it, and show results on a dashboard.

### The Big Picture (Plain English)
1. **Download the raw data** (Parquet files stored on S3).
2. **Replay it as if it’s “live”** (emit one query event at a time, faster than real time).
3. **Put those events into a message bus** (Kafka) so multiple services can read them safely.
4. **Clean + summarize the data** (handle missing values, compute hourly metrics).
5. **Store “recent” data in a fast store** (DuckDB) for live dashboards.
6. **Store full history in a big store** (S3 + Redshift) for heavy analysis.
7. **Show results in a dashboard** (Streamlit) with live + historical views.

If you imagine a **news pipeline**, it’s: *Reporter (Replay) → News Wire (Kafka) → Editors (Processor) → Short-term headlines (DuckDB) + Archives (Redshift) → Website (Dashboard).* 

## Tools & What They Do (Simple Explanation)

### 1) Amazon S3 (Data Source)
**What it is:** Cloud storage where the Redset files live.  
**Why we use it:** It’s the official dataset location, and it can store huge files cheaply.  
**You will do:** Download or stream Parquet files from S3.

### 2) Replay Producer (Python)
**What it is:** A small Python program that reads Redset files and sends events in time order.  
**Why we use it:** We want to simulate “live” query traffic.  
**Key idea:** We speed up time so 1 minute of replay equals 1 hour of real time data.

### 3) Apache Kafka (Message Bus)
**What it is:** A system that stores “streams of messages” reliably.  
**Why we use it:**  
- It prevents losing data if one service crashes.  
- Multiple consumers can read the same stream.  
- It handles spikes and backpressure.  

### 4) Stream Processor (Cleaning + Metrics)
**What it is:** A service that listens to Kafka, cleans data, and computes metrics.  
**Why we use it:** This is where we fix missing values and calculate things like cache hit rate or spill rate.  

### 5) DuckDB (Hot / Recent Data)
**What it is:** A lightweight analytical database you can run locally.  
**Why we use it:** It’s super fast for small-to-medium “recent” data, perfect for live dashboards.

### 6) S3 + Redshift (Cold / Historical Data)
**What it is:**  
- S3 stores the cleaned data in partitioned Parquet files.  
- Redshift loads that data for heavy, long-term analytics.  
**Why we use it:** Redshift is built for big queries across lots of data.

### 7) Streamlit Dashboard
**What it is:** A simple Python web app framework.  
**Why we use it:** It makes it easy to build interactive dashboards without frontend complexity.

## Environment Setup (Where You Run Things)

You can run the pipeline in one of two environments:

### Option A: Local laptop (small sample)
Use this for development or demos.  
- **Docker** for Kafka  
- **Python** for Replay + Processor  
- **DuckDB** for hot storage  
- **Streamlit** for dashboard  

### Option B: Cloud/VM (full pipeline)
Use this for a bigger demo or class deployment.  
- **EC2 or VM** for Kafka + services  
- **S3** for storage  
- **Redshift** for analytics  
- **Streamlit** hosted on the VM  

## Do We Need S3 + Redshift After Kafka?

**Short answer:** not always. It depends on your goals.

### If your focus is only live streaming + storing *analysis results* (not raw data)
You can skip Redshift and skip storing raw events in S3. Instead:
- Keep **DuckDB** for live views and short-term storage.
- Persist **only aggregated metrics** (hourly spill, cache hit rate, top-k tables, utilization) to a small store:
  - **Option A:** DuckDB (append-only aggregates)  
  - **Option B:** PostgreSQL (if you want multi-user access)
- This keeps storage cheaper and simpler.

### If you want full historical drill-down later
Keep S3 + Redshift so you can re-run new analyses on the full data.  
This is useful if requirements may expand or if you want “deep dives” later.

**Recommendation for your stated goals:**  
If you only need live streaming + saved metrics (not raw data), **drop S3→Redshift** and store only aggregates. Add S3/Redshift later if you need full historical re-analysis.

## Data Sanity Check (After Pre-Cleaning)

Because you already cleaned the dataset offline, you don’t need full cleaning again.  
Still, add a **sanity check** in the stream processor to prevent any unexpected or malformed rows from breaking the pipeline.

### Simple sanity checks (fast + safe)
Validate each incoming event before processing:
- **Required fields present:** `arrival_timestamp`, `instance_id`, `query_id`, `query_type`
- **Non-negative numbers:** durations, bytes, counts ≥ 0
- **Reasonable ranges:** e.g., `queue_duration_ms` not absurdly large (cap or drop)
- **Valid types:** timestamps parse correctly

### What to do when a row fails checks
- **Log it** (to a “bad rows” table or file)
- **Skip it** (so the pipeline does not crash)
- Optionally **count failures** for monitoring

This gives you stability without re-cleaning the data.

## What Analysis Are We Doing? (In Easy Terms)

We are answering questions like:

### 1) Spill Analysis
**What is “spill”?**  
Sometimes a query needs more memory than available, so it “spills” to disk. That makes it slower.  
**We measure:**  
- How many queries spill each hour  
- How much data is spilled  

### 2) Cache Hit Rate
**What is cache hit rate?**  
If Redshift already computed a query recently, it can reuse the answer. That’s a “cache hit.”  
**We measure:**  
- % of queries answered by cache  

### 3) Top-K Tables
**Which tables are most accessed?**  
We count how often each table is read and show the top 5 or top 10.  

### 4) Compile Time vs. Joins
**Is planning expensive?**  
Queries with more joins might take longer to compile.  
We compare number of joins vs. compile time.  

### 5) Utilization / Oversubscription
**Is the cluster too busy?**  
If multiple big queries run together, execution slows down.  
We compare “expected” performance vs. actual to estimate utilization and oversubscription.

## What’s good
- **Replay + Kafka** gives you controllable time scaling and back-pressure control.
- **DuckDB for hot data** + **Redshift for historical** is a practical split for latency and cost.
- **Micro-batch COPY into Redshift** matches Redshift strengths for batch ingestion.
- **Streamlit dual views** (live/historical) is a clean UX model.

## Gaps / risks to address
1. **Ordering + watermarking**
   - Ordering by arrival_timestamp in the producer is good, but late/out-of-order events can still appear (multiple files, partitions, or skew). You need **watermark logic** and **event-time windows** to compute hourly metrics (spills, cache hit rate, etc.) safely.
2. **Replay speed control + backpressure**
   - 1 minute = 1 hour implies 60x speed-up. If Kafka or consumers lag, you need **rate control** and **lag monitoring** (or adaptive slowdown).
3. **Data quality & schema enforcement**
   - Missingness + skew + zero inflation means you need explicit cleaning rules: null handling, type enforcement, sane defaults, and optional log transforms for heavy tails.
4. **Metric computation placement**
   - If you compute metrics only in the dashboard, you risk heavy repeated queries and inconsistent results. Precompute **hourly aggregates** in the processing layer (DuckDB for live, Redshift for history).
5. **S3 → Redshift ingestion**
   - Ensure the micro-batch staging uses **manifest files** and **idempotent loads** (e.g., partition by hour and instance_id; use MERGE/UPSERT or replace partition) to avoid duplicates.
6. **Observability**
   - Add a lightweight **metrics store** (Prometheus/Grafana or even DuckDB tables) for pipeline throughput, replay lag, Kafka lag, and ingestion errors.
7. **Result cache hit rate & hourly spill stats**
   - These are required dashboard metrics and should be computed in the processing layer with time-windowing; avoid computing raw from base data every time.

## Proposed Architecture (Implementable & Scalable)

### High-level flow
1. **Raw data ingestion** (S3 Parquet) → **Replay Service** (event-time order + watermark)
2. **Kafka** as the durable, scalable event bus
3. **Stream Processor** (Python or Spark/Flink-like; can be DuckDB + micro-batch) for cleaning + feature/metric prep
4. **Hot store** (DuckDB / PostgreSQL) for real-time queries
5. **Cold store** (S3 + Redshift) for full history
6. **Dashboard** (Streamlit) querying hot + cold stores

### Key changes vs. your plan
- Add a **stream processor layer** that handles:
  - schema enforcement
  - null handling
  - event-time windowing
  - hourly rollups (spill counts, cache hit rate, top-k tables, utilization)
- Add **watermarking** in replay + processing to handle out-of-order events.
- Add **observability** for lag/throughput/errors.

## Proposed Architecture Diagram (Mermaid)

```mermaid
flowchart TD
  A[S3 Redset Parquet] --> B[Replay Service
(Event-time order + watermark
1 min = 1 hr)]

  B --> C[Kafka Topics
raw_events]

  C --> D[Stream Processor
(Cleaning + Windowed Metrics)]

  D --> E1[Hot Store
DuckDB / Postgres
recent hours]
  D --> E2[Cold Store
S3 Parquet
partitioned by hour]

  E2 --> F[Redshift
COPY/MERGE
hourly partitions]

  E1 --> G[Streamlit Dashboard
Live metrics]
  F --> G[Streamlit Dashboard
Historical metrics]

  D --> H[Metrics/Monitoring
lag + errors + throughput]
```

## Data Processing & Cleaning Rules (Must-have)

1. **Schema normalization**
   - enforce types for timestamps, durations, counts
2. **Missingness strategy**
   - `cache_source_query_id`: allow null, don’t impute
   - `mbytes_scanned`, `mbytes_spilled`: null → 0 (if query_type indicates no scan)
   - `num_*`: null → 0
3. **Zero inflation**
   - keep separate flag features `is_zero_*`
4. **Heavy tails**
   - precompute log transforms for duration/bytes for analytics
5. **Late events**
   - watermark: allow 5–10 minutes of lateness before finalizing hourly aggregates

## Metric Computation Placement

**In Stream Processor** (hourly windows):
- Spill metrics: `sum(mbytes_spilled)`, `count(mbytes_spilled > 0)` by hour, query_type
- Cache hit rate: `sum(was_cached) / count(*)` by hour, instance_id
- Top-k tables: explode `read_table_ids` and compute counts per hour
- Compile vs joins correlation metrics for select queries
- Utilization scores (Q/S/P/C and weighted composite)

**In DuckDB/Redshift**:
- Store both raw events and aggregates
- Dashboards query aggregates for speed

## Implementation Roadmap

1. **Replay producer**: read parquet, order by event-time, send to Kafka
2. **Processor**: consume Kafka, clean/normalize, compute hourly aggregates
3. **Hot storage**: DuckDB with rolling window (last N hours)
4. **Cold storage**: partitioned parquet in S3, micro-batch load to Redshift
5. **Dashboard**: live + historical views querying pre-aggregated tables
6. **Observability**: log lag, throughput, and ingestion errors

## Why this architecture is better
- **Scalable**: Kafka + partitioned storage + Redshift batch loads
- **Correct**: watermarking prevents incorrect hourly metrics
- **Fast**: pre-aggregated metrics improve dashboard latency
- **Resilient**: separation of concerns isolates failures
