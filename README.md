# Open Source Ecosystem Intelligence Lakehouse

A production-style Data Engineering portfolio project built with **Databricks, PySpark, Apache Spark, Delta Lake, Unity Catalog, and Databricks Workflows** using public GitHub event data from **GH Archive**.

The project focuses on realistic engineering concerns: incremental ingestion, idempotency, nested semi-structured JSON, schema variation, data quality, identity handling, bot classification, performance analysis, orchestration, and analytical Gold models.

---

## Project Overview

GH Archive publishes compressed hourly files containing public GitHub events such as PushEvent, PullRequestEvent, IssuesEvent, IssueCommentEvent, WatchEvent, ForkEvent, and ReleaseEvent.

This project builds an end-to-end lakehouse pipeline that ingests those hourly files, preserves raw source data, normalizes common and event-specific fields, validates data quality, enriches actor identity information, and produces repository-level analytical models.

The project is primarily a **Data Engineering portfolio project**. The analytical layer provides purpose to the pipeline, but the main goal is to demonstrate engineering decisions and hands-on Spark/Databricks work.

---

## Analytical Question

> How do open-source repositories differ in contributor growth, contributor retention, activity trends, and contributor concentration over time?

Because the final dataset covers only seven days, the project deliberately avoids making long-term historical claims.

---

## Architecture

See [`docs/architecture.md`](docs/architecture.md).

High-level flow:

```text
GH Archive
    ↓
Landing / Raw Volume
    ↓
Bronze
    ↓
Silver Core
    ↓
Event-specific Silver tables
    ↓
Gold Repository Daily Activity
    ↓
Bot / Identity Enrichment
    ↓
Contributor Lifecycle Enrichment
    ↓
Data Quality
    ↓
Analytics / Visualizations
```

The pipeline follows a Medallion-style architecture:

- **Landing** — immutable hourly `.json.gz` files
- **Bronze** — raw event preservation plus ingestion metadata
- **Silver** — normalized common fields and event-specific payload models
- **Gold** — repository-day analytical metrics
- **Consumption** — ecosystem-level daily summary and visualizations

---

## Tech Stack

- Databricks
- PySpark
- Apache Spark
- Delta Lake
- Unity Catalog
- Databricks Workflows
- Python
- Spark SQL
- Git / GitHub
- GH Archive

The project intentionally avoids adding technologies such as Kafka, Airflow, ADF, or Kubernetes when they are not technically justified.

---

## Data Source

Source: **GH Archive**

GH Archive provides compressed hourly JSON files representing public GitHub activity.

Example files:

```text
2026-09-01-0.json.gz
2026-09-01-1.json.gz
...
2026-09-01-23.json.gz
```

Landing location:

```text
/Volumes/workspace/default/gharchive_raw
```

The downloader is parameterized by `start_date` and `end_date` and skips files that already exist.

---

## Final Dataset Scale

The final scale test uses **7 complete days** of GH Archive data: **September 1–7, 2026**.

| Metric | Value |
|---|---:|
| Hourly source files | 168 |
| Compressed raw data | ~5.964 GB |
| Bronze events | 8,838,503 |
| Silver Core events | 8,838,503 |
| Gold repository-day rows | 1,785,239 |
| Bronze duplicate event IDs | 0 |
| Silver duplicate event IDs | 0 |
| Gold duplicate `repo_id + event_date` keys | 0 |

The project intentionally stops at seven days because the scale is sufficient to demonstrate the engineering design without increasing compute usage only for a larger headline number.

---

## Repository Structure

```text
open-source-ecosystem-lakehouse/
├── README.md
├── .gitignore
├── .gitattributes
├── notebooks/
│   ├── 00_environment_check.ipynb
│   ├── 01_source_exploration.ipynb
│   ├── 02_bronze_ingestion_clean.ipynb
│   ├── 03_silver_core.ipynb
│   ├── 04_silver_push_events.ipynb
│   ├── 05_silver_pull_request_events.ipynb
│   ├── 06_silver_issue_events.ipynb
│   ├── 07_silver_issue_comment_events.ipynb
│   ├── 08_silver_watch_events.ipynb
│   ├── 09_silver_fork_events.ipynb
│   ├── 10_silver_release_events.ipynb
│   ├── 11_schema_variation_inventory.ipynb
│   ├── 12_data_quality_checks.ipynb
│   ├── 13_incremental_pipeline_validation.ipynb
│   ├── 14_spark_performance_analysis.ipynb
│   ├── 15_gold_repo_daily_activity.ipynb
│   ├── 16_bot_identity_enrichment.ipynb
│   ├── 17_download_gharchive_files.ipynb
│   ├── 18_scale_test_metrics.ipynb
│   ├── 19_contributor_lifecycle_metrics.ipynb
│   └── 20_analytics_consumption.ipynb
└── docs/
    ├── architecture.md
    ├── data_dictionary.md
    └── runbook.md
```

---

## Bronze Layer

Table:

```text
workspace.oss_ecosystem.bronze_github_events
```

Bronze preserves the original source record in `raw_json` and extracts only the top-level fields required downstream.

Key design decisions:

- preserve original raw event
- track source file and source hour
- store nested actor/repository/organization/payload structures as JSON strings
- deduplicate events on `event_id`
- use Delta Lake for persistence
- support rerun-safe incremental ingestion

A real duplicate source event was discovered during validation. The ingestion process was hardened with:

```python
bronze_batch = bronze_batch.dropDuplicates(["event_id"])
```

---

## Ingestion Control

Table:

```text
workspace.oss_ecosystem.ingestion_file_log
```

The control table tracks source files through statuses such as `discovered`, `processing`, `completed`, and `failed`.

This enables processing only unseen or eligible files, rerun safety, source file tracking, missing-hour detection, and file-level ingestion state.

A rerun of the completed seven-day download produced:

```text
Downloaded files: 0
Skipped existing files: 168
Failed files: 0
```

---

## Silver Layer

Core table:

```text
workspace.oss_ecosystem.silver_events_core
```

Silver Core normalizes fields shared by all event types: event ID, event type, event timestamp, actor ID and login, repository ID and name, organization ID, and source metadata.

Event-specific Delta tables:

- `silver_push_events`
- `silver_pull_request_events`
- `silver_issue_events`
- `silver_issue_comment_events`
- `silver_watch_events`
- `silver_fork_events`
- `silver_release_events`

This avoids creating one excessively wide and sparse schema for unrelated event payloads.

---

## Schema Variation Handling

The project does not simply enable automatic schema merging.

Instead, schema behavior was inspected explicitly through:

- top-level schema signature inspection
- payload variation inventory
- required-key validation
- unexpected-field detection
- schema baseline persistence
- synthetic malformed-record validation

Observed examples included multiple row-level payload variants for Issues and Pull Request events.

---

## Data Quality

Notebook:

```text
12_data_quality_checks
```

Results table:

```text
workspace.oss_ecosystem.dq_run_results
```

Checks include duplicate event IDs, required fields, malformed raw JSON, invalid timestamps, timestamp/source-hour consistency, missing source hours, repository ID availability, and Bronze/Silver reconciliation.

A real ForkEvent with a null `repo_id` was preserved in Silver rather than deleted. Because repository attribution was impossible, that event is excluded from repository-level Gold metrics.

---

## Gold Model

Main Gold table:

```text
workspace.oss_ecosystem.gold_repo_daily_activity
```

Grain:

```text
repo_id + event_date
```

Metrics include:

- total events
- active contributors
- Push events
- Pull Request events
- Issue events
- Issue Comment events
- Watch events
- Fork events
- Release events
- pull requests opened / closed
- issues opened / closed
- observed stars
- observed forks
- contributor concentration
- bot events
- human-or-unknown events
- bot contributors
- human-or-unknown contributors
- newly observed contributors
- returning contributors

Repository name is descriptive metadata and is not part of the analytical grain.

---

## Contributor Concentration

`top_contributor_share` is defined as the maximum number of events generated by one contributor in a repository-day divided by total contributor events for that repository-day.

A value of `1.0` is common for low-activity repository-days where only one contributor generated all observed events.

The metric is descriptive and is not interpreted as developer productivity.

---

## Bot and Identity Handling

Dimension:

```text
workspace.oss_ecosystem.dim_actor_identity
```

Stable GitHub actor IDs are used as the identity key.

The model retains the latest observed login, first and last observed timestamps, and count of observed login variants.

Bot classification is deliberately conservative:

> An actor is classified as a bot if at least one observed login ends with `[bot]`.

All other actors are classified as `human_or_unknown`.

---

## Contributor Lifecycle

Lifecycle metrics are calculated for `human_or_unknown` actors only.

### `newly_observed_contributors`

The contributor is first observed in that repository during the seven-day dataset. This does **not** mean the contributor is historically new to the repository.

### `returning_contributors`

The contributor was observed in the same repository on an earlier date within the dataset.

These metrics are intentionally described as observation-window metrics.

---

## Ecosystem Daily Summary

Consumption table:

```text
workspace.oss_ecosystem.gold_ecosystem_daily_summary
```

Grain:

```text
one row per event_date
```

Includes total events, active repositories, bot events, human-or-unknown events, newly observed contributors, returning contributors, bot event share, returning contributor share, stars, forks, pull requests opened, and issues opened.

---

## Analytics Output

The final analytics layer is intentionally small because this is primarily a Data Engineering project.

Final visualizations:

1. **Daily GitHub Event Activity**
2. **Observed Contributor Lifecycle by Day**
3. **Daily Bot Activity Share**

No claim of long-term ecosystem growth or decline is made from only seven days of observations.

---

## Incremental and Idempotent Processing

The project implements protection at three levels:

- **File level:** the ingestion control table prevents completed hourly files from being processed unnecessarily.
- **Event level:** Bronze and Silver use stable event IDs to prevent duplicate logical events.
- **Downloader level:** existing hourly files are skipped automatically.

This makes reruns safe without requiring complete pipeline resets.

---

## Orchestration

The production pipeline is orchestrated with Databricks Workflows.

```text
bronze_ingestion
        ↓
silver_core
        ↓
┌─────────────────────────────────────┐
│ Push                                │
│ Pull Request                        │
│ Issue                               │
│ Issue Comment                       │
│ Watch                               │
│ Fork                                │
│ Release                             │
└─────────────────────────────────────┘
        ↓
gold_repo_daily
        ↓
bot_identity_enrichment
        ↓
contributor_lifecycle_metrics
        ↓
data_quality_checks
```

The seven event-specific Silver tasks run in parallel.

A validated end-to-end run after lifecycle integration completed successfully:

| Field | Value |
|---|---|
| Run ID | `804699059046341` |
| Launch | Manual |
| Duration | 7m 36s |
| Status | Succeeded |
| Start | Sep 30, 2026, 12:02 PM |

Final Gold validation after that run:

```text
Missing required Gold columns: []
Gold rows: 1785239
Duplicate repo/date keys: 0
```

---

## Spark Performance Analysis

Performance work was done only after correctness.

The project inspected Spark execution plans, Adaptive Query Execution, shuffle behavior, partition counts, skew, Delta file counts, storage size, and representative aggregation runtime.

Final seven-day representative repository-day aggregation:

```text
Input Silver rows: 8,838,503
Output rows: 1,785,239
Observed runtime: 3.175 seconds
```

Earlier smaller workload:

```text
Input rows: ~616,687
Observed runtime: 0.559 seconds
```

These numbers are **not** presented as a formal speedup comparison because serverless resources, caching, and execution conditions can vary.

Observed skew did not justify adding salting, aggressive repartitioning, caching, or forced compaction.

---

## Storage Measurements

Final seven-day measurements:

### Raw

```text
168 files
6,403,659,046 compressed bytes
~5.964 GB
```

### Bronze Delta

```text
444 files
13,607,606,050 bytes
```

### Silver Core

```text
85 files
237,978,567 bytes
```

### Gold

```text
2 files
42,489,498 bytes
```

---

## Production-Style Hardening

The project includes:

- parameterized source download
- ingestion logging
- control-table state tracking
- Delta-backed targets
- bootstrap-safe table creation
- incremental execution
- rerun safety
- event-level deduplication
- schema validation
- data quality history
- identity handling
- bot enrichment
- orchestration dependencies
- documentation
- data dictionary
- pipeline runbook

Core Bronze and Silver targets use `CREATE TABLE IF NOT EXISTS` to avoid requiring manually pre-created tables in a new environment.

---

## Engineering Decisions

### No Kafka

The source already publishes complete hourly files. Streaming infrastructure would add complexity without solving a real requirement.

### No Airflow / ADF

Databricks Workflows is sufficient for the pipeline and avoids introducing an external orchestrator only for portfolio keywords.

### No aggressive Spark optimization

Measured performance did not justify techniques such as salting, caching, or complex partitioning.

### Seven-day final scale

The project was scaled until engineering behavior could be evaluated realistically, then stopped rather than spending more compute only to increase dataset size.

---

## Known Limitations

- The dataset covers seven days, not long-term GitHub history.
- `newly_observed_contributors` means first observation within the dataset, not historically new contributors.
- `returning_contributors` is also relative to the observation window.
- Bot detection based on `[bot]` suffix is intentionally conservative.
- WatchEvent activity is treated as observed star activity, not historical total stars.
- GH Archive represents public GitHub events and is not a complete model of repository behavior.
- Repository-level Gold excludes events where repository attribution is unavailable.
- Aggregated repository-contributor counts should not be interpreted as globally unique GitHub users.

---

## Documentation

- [`docs/architecture.md`](docs/architecture.md)
- [`docs/data_dictionary.md`](docs/data_dictionary.md)
- [`docs/runbook.md`](docs/runbook.md)

---

## How to Run

### 1. Download hourly files

Use:

```text
17_download_gharchive_files
```

Set:

```text
start_date
end_date
```

### 2. Run the Databricks Workflow

Main workflow:

```text
oss_ecosystem_lakehouse_pipeline
```

### 3. Validate the final Gold model

Expected seven-day result:

```text
Gold rows: 1,785,239
Duplicate repo/date keys: 0
```

### 4. Run analytics consumption

Use:

```text
20_analytics_consumption
```

---

## Key Takeaways

This project demonstrates:

- practical Spark-based ingestion of large semi-structured JSON
- incremental and idempotent processing
- Bronze / Silver / Gold lakehouse modeling
- Delta Lake usage
- nested payload normalization
- real duplicate handling
- explicit schema-variation analysis
- reusable data-quality controls
- stable actor identity handling
- conservative bot classification
- contributor lifecycle metrics
- Spark performance analysis
- orchestration with task dependencies
- measured scale testing
- cost-conscious engineering decisions
- production-style documentation

> **Correctness first, measurement second, optimization only when justified.**
