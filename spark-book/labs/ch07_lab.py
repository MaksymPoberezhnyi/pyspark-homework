# %% [markdown]
# # Chapter 7 — Auto Loader & CDC: your workbook
#
# Open this file in JupyterLab (right-click → *Open With → Notebook*, thanks to
# jupytext) or in VS Code (the `# %%` markers make runnable cells).
# Work through the cells while reading the book. Every task ends with asserts:
# when you see ✅ you're done. Re-running the setup cell resets the whole lab.

# %%
# ANCHOR: setup
import os
import shutil
import time

import pyspark.sql.functions as sf
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql.types import StringType, StructField, StructType

from common import get_spark
from lab07_data import (
    base, batch1, batch2, batch3, batch4, bronze_path, cdc_schema, chk,
    drop_batch, landing, reset_lab, silver_path,
)

spark = get_spark("ch07-autoloader-cdc")
reset_lab()
# ANCHOR_END: setup


# %% Task 1 — "local Auto Loader": incremental file ingestion into bronze
# ANCHOR: t1_starter
def run_bronze(schema):
    # TODO: readStream (json, .schema(schema)) from `landing`
    #       + _ingested_at, _source_file audit columns
    #       → writeStream to Delta at bronze_path, append, checkpoint f"{chk}/bronze",
    #         mergeSchema=true, trigger(availableNow=True); awaitTermination(); return the query
    ...


drop_batch("batch1", batch1)
q = run_bronze(cdc_schema)
bronze = spark.read.format("delta").load(bronze_path)
bronze.show(truncate=False)

assert bronze.count() == 4
print("✅ task 1")
# ANCHOR_END: t1_starter


# %% Task 2a — what the checkpoint does
# ANCHOR: t2a_starter
drop_batch("batch2", batch2)
# TODO: run bronze again and print q.lastProgress["numInputRows"]


bronze = spark.read.format("delta").load(bronze_path)
assert bronze.count() == 7
assert q.lastProgress["numInputRows"] == 3
print("✅ task 2a")
# ANCHOR_END: t2a_starter


# %% Task 2b — break the checkpoint on purpose (in a separate table)
# ANCHOR: t2b_starter
exp_path, exp_chk = f"{base}/bronze_experiment", f"{chk}/bronze_experiment"


def run_experiment(max_files=None):
    # TODO: stream landing -> exp_path with checkpoint exp_chk (availableNow);
    #       if max_files is set, add .option("maxFilesPerTrigger", max_files) on the reader;
    #       return the row count of exp_path
    ...


# TODO: c1 = run, c2 = run, delete exp_chk, c3 = run — print the three counts
c1 = c2 = c3 = None

assert (c1, c2, c3) == (7, 7, 14), (c1, c2, c3)
print("✅ task 2b")
# ANCHOR_END: t2b_starter


# %% Task 3 — apply CDC to silver with foreachBatch + MERGE
# ANCHOR: t3_starter
(DeltaTable.createIfNotExists(spark)
    .location(silver_path)
    .addColumn("id", "BIGINT")
    .addColumn("name", "STRING")
    .addColumn("city", "STRING")
    .addColumn("ts", "TIMESTAMP")
    .property("delta.enableChangeDataFeed", "true")      # needed in task 6
    .execute())


def latest_per_id(df):
    # TODO: newest event per id (Window + row_number), drop _ingested_at/_source_file
    ...


def apply_cdc_v1(batch_df, batch_id):
    # TODO: MERGE latest_per_id(batch_df) into silver on t.id = s.id
    #   matched & D → delete | matched & not D → update | not matched & not D → insert
    ...


def run_silver(fn):
    # TODO: readStream delta from bronze_path → foreachBatch(fn),
    #       checkpoint f"{chk}/silver", availableNow; await; return q
    ...


run_silver(apply_cdc_v1)
silver = spark.read.format("delta").load(silver_path).orderBy("id")
silver.show()

state = {r["id"]: r["city"] for r in silver.collect()}
assert state == {1: "Dnipro", 2: "Kharkiv", 4: "Kyiv"}, state
print("✅ task 3")
# ANCHOR_END: t3_starter


# %% Task 4 — late / out-of-order events
# ANCHOR: t4_starter
def apply_cdc_v2(batch_df, batch_id):
    # TODO: like v1, but only update/delete when the event is NEWER (s.ts > t.ts)
    ...


drop_batch("batch3", batch3)
# TODO: run bronze, then silver with apply_cdc_v2


silver = spark.read.format("delta").load(silver_path).orderBy("id")
silver.show()
state = {r["id"]: r["city"] for r in silver.collect()}
assert state == {1: "Dnipro", 4: "Kyiv", 5: "Kyiv"}, state   # Alice NOT moved to Lviv, Bob deleted
print("✅ task 4")
# ANCHOR_END: t4_starter


# %% Task 5a/5b — detect schema drift
# ANCHOR: t5ab_starter
drop_batch("batch4", batch4)

# 5a TODO: batch-read landing/batch4.json WITH inference; new_cols = columns not in cdc_schema
new_cols = ...
print("new columns in source:", new_cols)

# 5b TODO: read the same file with cdc_schema and print .columns — where did email go?

assert new_cols == {"email"}
print("✅ task 5a/5b")
# ANCHOR_END: t5ab_starter


# %% Task 5c — evolve bronze
# ANCHOR: t5c_starter
# TODO: cdc_schema_v2 = old fields + email (do NOT use .add(): it mutates cdc_schema)
cdc_schema_v2 = ...
run_bronze(cdc_schema_v2)

bronze = spark.read.format("delta").load(bronze_path)
bronze.printSchema()
assert "email" in bronze.columns
print("✅ task 5c")
# ANCHOR_END: t5c_starter


# %% Task 5d — evolve silver explicitly, MERGE with dynamic column lists
# ANCHOR: t5d_starter
def evolve_target_schema(batch_df, path, ignore=("op",)):
    # TODO: columns in batch_df but not in the Delta table at `path` (except `ignore`)
    #       → append an EMPTY DataFrame with just those fields and mergeSchema=true
    #       return the list of added StructFields
    ...


def apply_cdc_v3(batch_df, batch_id):
    # TODO: latest_per_id → evolve_target_schema → build set/values dicts from the
    #       batch columns → MERGE with v2's conditions (use a FRESH DeltaTable handle)
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
# ANCHOR_END: t5d_starter


# %% Task 6 — Change Data Feed: CDC *out* of silver
# ANCHOR: t6_starter
changes = ...   # TODO: batch read of silver's change feed from version 0

(changes.select("id", "city", "email", "_change_type", "_commit_version")
    .orderBy("_commit_version", "id").show(50))

deleted = {r["id"] for r in changes.filter("_change_type = 'delete'").select("id").collect()}
assert deleted == {2}, deleted

dan_after = (changes.filter("id = 4 AND _change_type = 'update_postimage'")
             .orderBy(sf.col("_commit_version").desc()).first())
assert dan_after["email"] == "dan@example.com"
print("✅ task 6")
# ANCHOR_END: t6_starter


# %% Task 7 — watch the pipeline run continuously
# ANCHOR: t7_starter
# TODO: start bronze_q (json → bronze, SAME checkpoint as run_bronze, schema v2)
#       and silver_q (bronze → foreachBatch(apply_cdc_v3), SAME checkpoint as run_silver)
#       both with .trigger(processingTime="5 seconds") — do NOT awaitTermination()
bronze_q = silver_q = None

drop_batch("batch5", [
    {"op": "U", "id": 5, "name": "Eve", "city": "Odesa", "email": "eve@example.com", "ts": "2026-10-05T09:00:00"},
])


def eve_city():
    row = spark.read.format("delta").load(silver_path).filter("id = 5").first()
    return row["city"] if row else None


# TODO: poll eve_city() every few seconds (max ~90 s), printing bronze_q.status["message"];
#       then print [p["batchId"] for p in bronze_q.recentProgress] and stop both queries

assert eve_city() == "Odesa"
print("✅ task 7")
# ANCHOR_END: t7_starter

# %%
spark.stop()
