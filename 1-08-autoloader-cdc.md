---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.19.5
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

<!-- #region slideshow={"slide_type": "slide"} -->
# Autoloader & CDC

**CDC (Change Data Capture)** = instead of receiving the full table every day, you receive a
stream of *changes*: `I`nsert / `U`pdate / `D`elete events with a timestamp. Your job is to
turn that event stream into a table holding the **current state** of every entity.

The pipeline you'll build (medallion architecture):

```
 source system ──files──▶ landing/ ──stream──▶ BRONZE ──stream + MERGE──▶ SILVER ──CDF──▶ downstream
                          (JSON)              raw events,               current state
                                              append-only               1 row per id
```

**About Autoloader:** `format("cloudFiles")` exists **only on Databricks**. Locally we use the
open-source Structured Streaming *file source*, which has the same core mechanics (a checkpoint
remembers processed files → each file is read exactly once). The real Autoloader code and what
it adds is at the end of this notebook.

Solutions: `1-08-autoloader-cdc-solutions.md` — peek only after trying.
Every task ends with `assert`s: when you see ✅, you're done.
<!-- #endregion -->

```python slideshow={"slide_type": "skip"}
import os, sys, json, shutil
os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@17")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pyspark
import pyspark.sql.functions as sf
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

builder = (
    pyspark.sql.SparkSession.builder
    .master("local[*]")
    .appName("autoloader-cdc")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")   # tiny data: 200 shuffle partitions would be pure overhead
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
```

### Setup: paths, a fake source system, and the CDC events

Run as-is. Re-running this cell **resets the whole lab** (deletes all tables and checkpoints).

```python
base = os.path.abspath("../data/_out/cdc_lab")
landing = f"{base}/landing"          # the "source system" drops files here
bronze_path = f"{base}/bronze"       # raw events, append-only
silver_path = f"{base}/silver"       # current state of every customer
chk = f"{base}/_checkpoints"

shutil.rmtree(base, ignore_errors=True)
os.makedirs(landing, exist_ok=True)


def drop_batch(name, events):
    """Simulate the source system writing one JSON-lines file of CDC events."""
    path = os.path.join(landing, f"{name}.json")
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"dropped {name}.json ({len(events)} events)")


cdc_schema = StructType([
    StructField("op", StringType()),      # I = insert, U = update, D = delete
    StructField("id", LongType()),
    StructField("name", StringType()),
    StructField("city", StringType()),
    StructField("ts", TimestampType()),   # when the change happened in the source
])

batch1 = [
    {"op": "I", "id": 1, "name": "Alice", "city": "Kyiv",    "ts": "2026-10-01T10:00:00"},
    {"op": "I", "id": 2, "name": "Bob",   "city": "Lviv",    "ts": "2026-10-01T10:00:00"},
    {"op": "I", "id": 3, "name": "Carol", "city": "Odesa",   "ts": "2026-10-01T10:00:00"},
    {"op": "U", "id": 2, "name": "Bob",   "city": "Kharkiv", "ts": "2026-10-01T12:00:00"},  # same file!
]
batch2 = [
    {"op": "U", "id": 1, "name": "Alice", "city": "Dnipro",  "ts": "2026-10-02T10:00:00"},
    {"op": "D", "id": 3, "name": None,    "city": None,      "ts": "2026-10-02T11:00:00"},
    {"op": "I", "id": 4, "name": "Dan",   "city": "Kyiv",    "ts": "2026-10-02T12:00:00"},
]
batch3 = [
    # LATE event: older than the Dnipro update silver already has -> must be ignored
    {"op": "U", "id": 1, "name": "Alice", "city": "Lviv",    "ts": "2026-10-01T15:00:00"},
    {"op": "I", "id": 5, "name": "Eve",   "city": "Kyiv",    "ts": "2026-10-03T09:00:00"},
    {"op": "D", "id": 2, "name": None,    "city": None,      "ts": "2026-10-03T10:00:00"},
]
batch4 = [
    # the source system added a new field: email
    {"op": "I", "id": 6, "name": "Frank", "city": "Lviv", "email": "frank@example.com", "ts": "2026-10-04T09:00:00"},
    {"op": "U", "id": 4, "name": "Dan",   "city": "Kyiv", "email": "dan@example.com",   "ts": "2026-10-04T10:00:00"},
]
```

<!-- #region slideshow={"slide_type": "slide"} -->
## Task 1 — "local Autoloader": incremental file ingestion into bronze

Write `run_bronze(schema)` that starts a streaming query which:

1. reads JSON files from `landing` with `spark.readStream` (streaming file sources **require** `.schema(...)`),
2. adds two audit columns: `_ingested_at` (`sf.current_timestamp()`) and
   `_source_file` (`sf.col("_metadata.file_name")` — hidden metadata column of file sources),
3. writes to Delta at `bronze_path` in `append` mode,
4. uses `checkpointLocation = f"{chk}/bronze"`,
5. sets `.option("mergeSchema", "true")` on the writer (you'll need it in task 5),
6. uses `.trigger(availableNow=True)` — process everything available, then stop,
7. calls `q.awaitTermination()` and returns `q`.

Hint: `writeStream ... .start(path)` returns a `StreamingQuery`.
<!-- #endregion -->

```python
def run_bronze(schema):
    # TODO
    ...


drop_batch("batch1", batch1)
q = run_bronze(cdc_schema)
bronze = spark.read.format("delta").load(bronze_path)
bronze.show(truncate=False)

assert bronze.count() == 4
print("✅ task 1")
```

<!-- #region slideshow={"slide_type": "slide"} -->
## Task 2 — what the checkpoint does

**2a.** Drop `batch2` and call `run_bronze(cdc_schema)` again. Print
`q.lastProgress["numInputRows"]` — how many rows were read *in this run*?

**2b.** Experiment in a **separate** table (`exp_path`, `exp_chk` below) so bronze stays intact:
write a small `run_experiment()` that streams `landing` → `exp_path` with checkpoint `exp_chk`
and returns the row count of `exp_path`. Then:
- run it twice → counts?
- delete the checkpoint folder (`shutil.rmtree(exp_chk)`) and run once more → count?

Question to answer for yourself: *why did the third run produce duplicates, and why would
a MERGE-based sink (task 3) not have that problem?*
<!-- #endregion -->

```python
# 2a
drop_batch("batch2", batch2)
# TODO: run bronze again, print numInputRows from q.lastProgress


bronze = spark.read.format("delta").load(bronze_path)
assert bronze.count() == 7
assert q.lastProgress["numInputRows"] == 3
print("✅ task 2a")
```

```python
# 2b
exp_path, exp_chk = f"{base}/bronze_experiment", f"{chk}/bronze_experiment"

def run_experiment():
    # TODO: stream landing -> exp_path with checkpoint exp_chk, return the row count
    ...

# TODO: run twice, delete exp_chk, run again — print the three counts
```

<!-- #region slideshow={"slide_type": "slide"} -->
## Task 3 — apply CDC to silver with `foreachBatch` + MERGE

Streaming sinks can only *append*. To **update/delete** rows we use `foreachBatch`: Spark gives
you every micro-batch as a normal DataFrame, and inside you can do anything — e.g. a Delta MERGE.

The silver table is created for you (with **Change Data Feed** on — needed in task 6).

Implement:

1. `latest_per_id(df)` — a micro-batch can contain several events for the same `id`
   (look at Bob in `batch1`!). Keep only the newest per `id`:
   `Window.partitionBy("id").orderBy(sf.col("ts").desc())` + `sf.row_number()` + filter `== 1`.
   Also drop the helper columns `_ingested_at`, `_source_file`.
2. `apply_cdc_v1(batch_df, batch_id)` — MERGE `latest` into silver on `t.id = s.id`:
   - matched & `op = 'D'` → delete
   - matched & `op <> 'D'` → update `name`, `city`, `ts`
   - not matched & `op <> 'D'` → insert
3. `run_silver(fn)` — stream **from the bronze Delta table**
   (`spark.readStream.format("delta").load(bronze_path)`) with `.foreachBatch(fn)`,
   checkpoint `f"{chk}/silver"`, `availableNow`; await; return the query.

Python API you need: `DeltaTable.forPath(spark, path).alias("t").merge(src.alias("s"), cond)`
`.whenMatchedDelete(condition=...)` `.whenMatchedUpdate(condition=..., set={...})`
`.whenNotMatchedInsert(condition=..., values={...})` `.execute()`
<!-- #endregion -->

```python
spark.sql(f"""
    CREATE TABLE delta.`{silver_path}` (id BIGINT, name STRING, city STRING, ts TIMESTAMP)
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")


def latest_per_id(df):
    # TODO
    ...


def apply_cdc_v1(batch_df, batch_id):
    # TODO
    ...


def run_silver(fn):
    # TODO
    ...


run_silver(apply_cdc_v1)
silver = spark.read.format("delta").load(silver_path).orderBy("id")
silver.show()

state = {r["id"]: r["city"] for r in silver.collect()}
assert state == {1: "Dnipro", 2: "Kharkiv", 4: "Kyiv"}, state
print("✅ task 3")
```

<!-- #region slideshow={"slide_type": "slide"} -->
## Task 4 — late / out-of-order events

`batch3` contains an update for Alice with a timestamp **older** than what silver already has
(`Lviv` at 2026-10-01 15:00 vs `Dnipro` at 2026-10-02 10:00). With `apply_cdc_v1` Alice would
move back to Lviv — wrong.

Write `apply_cdc_v2` = `apply_cdc_v1` + only update/delete when the event is **newer**
than the target row (`s.ts > t.ts`). Then ingest `batch3` into bronze and run silver with v2.

(Note: you can swap the `foreachBatch` function between runs — the checkpoint stores *what was
read*, not your code.)
<!-- #endregion -->

```python
def apply_cdc_v2(batch_df, batch_id):
    # TODO
    ...


drop_batch("batch3", batch3)
# TODO: run bronze, then silver with apply_cdc_v2


silver = spark.read.format("delta").load(silver_path).orderBy("id")
silver.show()
state = {r["id"]: r["city"] for r in silver.collect()}
assert state == {1: "Dnipro", 4: "Kyiv", 5: "Kyiv"}, state   # Alice NOT moved to Lviv, Bob deleted
print("✅ task 4")
```

<!-- #region slideshow={"slide_type": "slide"} -->
## Task 5 — schema drift (your work pain point)

`batch4` has a new field `email`. A stream with a fixed schema will **silently drop** it.

- **5a.** Detect drift before ingesting: batch-read `landing/batch4.json` with schema inference
  and compute `new_cols` = columns not in `cdc_schema`.
- **5b.** Read the same file with the old `cdc_schema` and print its columns — see `email` vanish.
- **5c.** Build `cdc_schema_v2` = old fields + `StructField("email", StringType())`
  (careful: `StructType.add()` **mutates** in place — build a new `StructType`).
  Run `run_bronze(cdc_schema_v2)`. Thanks to `mergeSchema` the bronze table gains `email`.
- **5d.** Write `apply_cdc_v3`: same as v2, but before the MERGE it compares the micro-batch
  columns with silver's columns and for each missing one (except `op`) runs
  `ALTER TABLE delta.\`{silver_path}\` ADD COLUMNS (<name> <type>)`
  (`field.dataType.simpleString()` gives `string`, `bigint`, ...). Build the `set`/`values`
  dicts **dynamically** from the batch columns. Use a **fresh** `DeltaTable.forPath(...)` after
  the ALTER (stale handles cache the old schema — remember 1-07).

Why explicit `ALTER` instead of `spark.databricks.delta.schema.autoMerge.enabled = true`?
Auto-merge works too, but it adds *any* new column silently — the explicit way gives you a
place to log, validate, or reject changes.
<!-- #endregion -->

```python
drop_batch("batch4", batch4)

# 5a TODO
new_cols = ...
print("new columns in source:", new_cols)
assert new_cols == {"email"}

# 5b TODO: read batch4.json with cdc_schema and print .columns
```

```python
# 5c TODO
cdc_schema_v2 = ...
run_bronze(cdc_schema_v2)

bronze = spark.read.format("delta").load(bronze_path)
assert "email" in bronze.columns
print("✅ task 5c")
```

```python
# 5d
def apply_cdc_v3(batch_df, batch_id):
    # TODO
    ...


run_silver(apply_cdc_v3)
silver = spark.read.format("delta").load(silver_path).orderBy("id")
silver.show()

rows = {r["id"]: (r["city"], r["email"]) for r in silver.collect()}
assert rows == {
    1: ("Dnipro", None),
    4: ("Kyiv", "dan@example.com"),
    5: ("Kyiv", None),
    6: ("Lviv", "frank@example.com"),
}, rows
print("✅ task 5d")
```

<!-- #region slideshow={"slide_type": "slide"} -->
## Task 6 — Change Data Feed: CDC *out* of silver

Silver now *produces* a change stream for downstream consumers (gold tables, other teams).
Read it with `.option("readChangeFeed", "true").option("startingVersion", 0)` and look at the
extra columns `_change_type` (`insert`, `update_preimage`, `update_postimage`, `delete`) and
`_commit_version`.

- Which ids were deleted? (should be only Bob — why is Carol **not** there? Hint: task 3's
  `latest_per_id` and which micro-batch Carol's insert + delete landed in.)
- What's Dan's latest `update_postimage`?

(Streaming version: `spark.readStream.format("delta").option("readChangeFeed", "true")...`)
<!-- #endregion -->

```python
changes = ...   # TODO
changes.select("id", "city", "_change_type", "_commit_version").orderBy("_commit_version", "id").show(50)

deleted = {r["id"] for r in changes.filter("_change_type = 'delete'").select("id").collect()}
assert deleted == {2}, deleted

dan_after = (changes.filter("id = 4 AND _change_type = 'update_postimage'")
                    .orderBy(sf.col("_commit_version").desc()).first())
assert dan_after["email"] == "dan@example.com"
print("✅ task 6")
```

<!-- #region slideshow={"slide_type": "slide"} -->
## The real Autoloader (Databricks only — read, run it at work)

Task 1 rewritten with Autoloader:

```python
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{chk}/bronze_schema")   # where inferred schema versions live
    .option("cloudFiles.inferColumnTypes", "true")                 # JSON: infer ints/timestamps, not all strings
    .option("cloudFiles.schemaHints", "id BIGINT, ts TIMESTAMP")   # pin the types you care about
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")     # the default
    .load(landing)
    .writeStream
    .option("checkpointLocation", f"{chk}/bronze")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("bronze_customers_cdc"))
```

| What you did by hand locally | What Autoloader gives you |
|---|---|
| wrote `cdc_schema` yourself | **schema inference** + stored in `schemaLocation`, `schemaHints` to pin types |
| task 5a: detected `email` manually | **schema evolution**: on a new column the stream **fails once** with `UnknownFieldException`, the new schema is saved → restart picks it up (`addNewColumns`) |
| task 5b: data silently dropped | **`_rescued_data`** column: unknown fields and type mismatches land there as JSON instead of being lost |
| checkpoint tracks the file list | same, but scales to millions of files; optional **file notification mode** (`cloudFiles.useNotifications`) instead of directory listing |

`schemaEvolutionMode` options:
- `addNewColumns` (default) — fail once, evolve, continue after restart (use a job with retries)
- `rescue` — never change the schema; unknown fields go to `_rescued_data`
- `failOnNewColumns` — stop and wait for a human
- `none` — ignore new columns (silent drop — the thing you saw in 5b)

**On Databricks you'd often not hand-write tasks 3–5 at all:** Lakeflow Declarative Pipelines
(formerly DLT) have a declarative CDC API (`AUTO CDC` / formerly `APPLY CHANGES INTO`) that does
dedup, ordering by a sequence column (= your task 4), deletes, and even SCD Type 1/2 for you.
Knowing how to build it by hand is what lets you debug it when it misbehaves.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "slide"} -->
## Review questions (answer out loud / bring to the trainer)

1. What exactly is stored in a streaming checkpoint? What happens if you delete it?
2. Why is `foreachBatch` + MERGE called "effectively exactly-once" even though `foreachBatch`
   itself is at-least-once? (Hint: what happens if the same batch is MERGEd twice?)
3. Why do we need `latest_per_id` *and* the `s.ts > t.ts` guard — don't they do the same thing?
4. `availableNow` vs `processingTime="1 minute"` vs continuous streaming — when would you use each?
5. Bronze keeps *all* events forever. Why not MERGE straight from landing into silver?
<!-- #endregion -->

```python
spark.stop()
```
