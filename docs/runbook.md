\# Pipeline Runbook



\## Purpose



This runbook describes how to operate and verify the Open Source Ecosystem Intelligence Lakehouse.



The project is implemented in Databricks using PySpark, Spark SQL, Delta Lake, Unity Catalog, and Databricks Workflows.



\## Environment



Catalog:



`workspace`



Schema:



`oss\_ecosystem`



Landing Volume:



`/Volumes/workspace/default/gharchive\_raw`



Main workflow:



`oss\_ecosystem\_lakehouse\_pipeline`



\## Source Data



Source:



GH Archive



File format:



`.json.gz`



File cadence:



one file per UTC hour



Example:



```text

2026-09-01-0.json.gz

2026-09-01-1.json.gz

...

2026-09-01-23.json.gz

```



\## Downloading Source Files



Notebook:



`17\_download\_gharchive\_files`



Parameters:



\- `start\_date`

\- `end\_date`



Example:



```text

start\_date = 2026-09-01

end\_date   = 2026-09-07

```



The downloader:



\- checks whether each hourly file already exists

\- skips existing files

\- streams missing files to the landing Volume

\- records downloaded, skipped, and failed counts



A rerun over the completed seven-day dataset produced:



```text

Downloaded files: 0

Skipped existing files: 168

Failed files: 0

```



This confirms download-level rerun safety.



\## Main Workflow



Core workflow order:



```text

bronze\_ingestion

&#x20;       ↓

silver\_core

&#x20;       ↓

event-specific Silver tasks

&#x20;       ↓

gold\_repo\_daily

&#x20;       ↓

bot\_identity\_enrichment

&#x20;       ↓

contributor\_lifecycle\_metrics

&#x20;       ↓

data\_quality\_checks

```



The event-specific Silver tasks run in parallel:



```text

silver\_push\_events

silver\_pull\_request\_events

silver\_issue\_events

silver\_issue\_comment\_events

silver\_watch\_events

silver\_fork\_events

silver\_release\_events

```



All required event-specific tasks must succeed before Gold processing begins.



\## Incremental Behavior



\### Bronze



The Bronze ingestion process uses `ingestion\_file\_log` to identify source files that still require processing.



Already completed hourly files are not processed again unnecessarily.



Within an incoming batch, records are deduplicated on:



`event\_id`



The Bronze target is updated using Delta Lake logic designed to avoid duplicate event insertion.



\### Silver



Silver transformations read normalized Bronze data and use event IDs to prevent duplicate insertion.



Event-specific Silver tables process only relevant GitHub event types.



\### Gold



The repository daily model is rebuilt at the grain:



`repo\_id + event\_date`



Repository name is descriptive metadata and is not part of the primary analytical grain.



Bot and contributor lifecycle enrichments are applied after the base Gold model.



\## Bootstrap Behavior



Core tables are created with `CREATE TABLE IF NOT EXISTS` so that the pipeline does not require manually pre-created Delta targets.



Bootstrap-safe targets include:



\- `bronze\_github\_events`

\- `ingestion\_file\_log`

\- `silver\_events\_core`

\- event-specific Silver tables



\## Data Quality



Notebook:



`12\_data\_quality\_checks`



Quality checks include:



\- duplicate event IDs

\- required-field validation

\- malformed raw JSON

\- timestamp consistency

\- source-hour completeness

\- repository ID availability

\- Bronze/Silver reconciliation



Results are persisted to:



`workspace.oss\_ecosystem.dq\_run\_results`



Not every warning represents a pipeline failure.



For example, a real ForkEvent was observed with a null `repo\_id`.



The event was preserved in Silver because it is valid source data, but it is excluded from repository-level Gold metrics because repository attribution is impossible.



\## Expected Seven-Day Counts



For the September 1 through September 7, 2026 dataset:



```text

Completed hourly files: 168



Bronze events:

8,838,503



Silver Core events:

8,838,503



Gold repository-day rows:

1,785,239

```



Expected duplicate checks:



```text

Bronze duplicate event IDs: 0

Silver duplicate event IDs: 0

Gold duplicate repo/date keys: 0

```



\## Final Gold Verification



After a successful workflow run, verify that final enrichments are still present:



```python

gold\_df = spark.table(

&#x20;   "workspace.oss\_ecosystem.gold\_repo\_daily\_activity"

)



required\_columns = \[

&#x20;   "bot\_events",

&#x20;   "human\_or\_unknown\_events",

&#x20;   "newly\_observed\_contributors",

&#x20;   "returning\_contributors"

]



missing\_columns = \[

&#x20;   c for c in required\_columns

&#x20;   if c not in gold\_df.columns

]



print("Missing required Gold columns:", missing\_columns)

print("Gold rows:", gold\_df.count())



dup\_count = (

&#x20;   gold\_df

&#x20;   .groupBy("repo\_id", "event\_date")

&#x20;   .count()

&#x20;   .filter("count > 1")

&#x20;   .count()

)



print("Duplicate repo/date keys:", dup\_count)

```



Expected result for the final seven-day dataset:



```text

Missing required Gold columns: \[]

Gold rows: 1785239

Duplicate repo/date keys: 0

```



\## Successful Orchestration Validation



Validated workflow run:



```text

Run ID: 804699059046341

Launch: Manual

Duration: 7m 36s

Status: Succeeded

Start: Sep 30, 2026, 12:02 PM

```



This run included contributor lifecycle processing inside the production DAG.



\## Scale-Test Results



Final raw dataset:



```text

168 files

6,403,659,046 compressed bytes

approximately 5.964 GB

```



Delta storage observed during the final scale test:



```text

Bronze:

13,607,606,050 bytes

444 files



Silver Core:

237,978,567 bytes

85 files



Gold:

42,489,498 bytes

2 files

```



Representative repository-day aggregation:



```text

Input Silver rows:

8,838,503



Output repository-day rows:

1,785,239



Observed runtime:

3.175 seconds

```



An earlier smaller test used approximately 616,687 Silver rows and completed the comparable aggregation in approximately 0.559 seconds.



These measurements should \*\*not\*\* be interpreted as a formal performance improvement comparison because serverless resources, caching, and execution conditions may differ.



\## Spark Performance Decision



Observed repository activity distribution showed measurable skew, but the workload remained small enough that more aggressive optimization was not justified.



The project therefore intentionally avoids unnecessary:



\- salting

\- caching

\- manual repartitioning

\- forced compaction

\- complex partitioning strategies



Optimization decisions are based on measured behavior rather than adding techniques only for portfolio appearance.



\## Analytics



Notebook:



`20\_analytics\_consumption`



Final visualizations:



1\. Daily GitHub Event Activity

2\. Observed Contributor Lifecycle by Day

3\. Daily Bot Activity Share



The visualization layer is intentionally small because the project focuses primarily on Data Engineering.



\## Known Limitations



The final dataset covers seven days and should not be interpreted as a long-term historical trend.



Contributor lifecycle metrics are relative to this seven-day observation window.



Bot detection is conservative and does not identify every automated account.



WatchEvent activity is used as observed star activity but does not represent a complete historical star count.



GitHub event data represents public events available through GH Archive and is not a complete model of all repository behavior.



One historical ingestion-log row reflects the original raw record count before the later Bronze source-deduplication fix. The final Bronze and Silver datasets contain no duplicate event IDs.



\## Recovery Guidance



If a workflow task fails:



1\. Inspect the failed Databricks task and error message.

2\. Confirm source files exist in the landing Volume.

3\. Check `ingestion\_file\_log` for the affected file state.

4\. Fix the underlying transformation or input problem.

5\. Rerun the failed workflow or workflow branch.

6\. Re-run data-quality checks.

7\. Verify final Gold row grain and required enrichment columns.



Do not delete valid raw source files as part of recovery.



\## Cost Control



This is a portfolio project designed around low-cost development.



Cost-conscious decisions include:



\- Databricks Free Edition/serverless development

\- gradual scaling

\- incremental ingestion

\- idempotent reruns

\- seven-day final dataset rather than unnecessary larger processing

\- avoiding external orchestration platforms without a technical requirement

\- avoiding Kafka, ADF, Airflow, or other infrastructure solely to increase the technology list

