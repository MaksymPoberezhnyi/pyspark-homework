# %% [markdown]
# # Chapter 7 — Auto Loader & CDC: reference solutions
#
# Run end-to-end as a self-test:  `python solutions/ch07_solutions.py`
# Every task prints ✅ when its asserts pass.

# %%
# ANCHOR: setup
import os
import shutil
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "labs"))

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


# %% Task 1 ------------------------------------------------------------------
# ANCHOR: t1_solution
def run_bronze(schema):
    q = (
        spark.readStream
        .format("json")
        .schema(schema)                                   # file streams need a schema up front
        .load(landing)
        .withColumn("_ingested_at", sf.current_timestamp())
        .withColumn("_source_file", sf.col("_metadata.file_name"))
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{chk}/bronze")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .start(bronze_path)
    )
    q.awaitTermination()
    return q
# ANCHOR_END: t1_solution


# ANCHOR: t1_check
drop_batch("batch1", batch1)
q = run_bronze(cdc_schema)
bronze = spark.read.format("delta").load(bronze_path)
bronze.show(truncate=False)

assert bronze.count() == 4
print("✅ task 1")
# ANCHOR_END: t1_check


# %% Task 2a -----------------------------------------------------------------
# ANCHOR: t2a_solution
drop_batch("batch2", batch2)
q = run_bronze(cdc_schema)
print("rows read in this run:", q.lastProgress["numInputRows"])
# ANCHOR_END: t2a_solution

# ANCHOR: t2a_check
bronze = spark.read.format("delta").load(bronze_path)
assert bronze.count() == 7
assert q.lastProgress["numInputRows"] == 3
print("✅ task 2a")
# ANCHOR_END: t2a_check


# %% Task 2b -----------------------------------------------------------------
# ANCHOR: t2b_solution
exp_path, exp_chk = f"{base}/bronze_experiment", f"{chk}/bronze_experiment"


def run_experiment(max_files=None):
    reader = spark.readStream.format("json").schema(cdc_schema)
    if max_files:
        reader = reader.option("maxFilesPerTrigger", max_files)
    q = (
        reader.load(landing)
        .writeStream.format("delta")
        .option("checkpointLocation", exp_chk)
        .trigger(availableNow=True)
        .start(exp_path)
    )
    q.awaitTermination()
    return spark.read.format("delta").load(exp_path).count()


c1 = run_experiment()
c2 = run_experiment()
shutil.rmtree(exp_chk)
c3 = run_experiment()
print("counts:", c1, c2, c3)
# ANCHOR_END: t2b_solution

# ANCHOR: t2b_check
assert (c1, c2, c3) == (7, 7, 14), (c1, c2, c3)
print("✅ task 2b")
# ANCHOR_END: t2b_check


# %% Task 3 ------------------------------------------------------------------
# ANCHOR: t3_create
(DeltaTable.createIfNotExists(spark)
    .location(silver_path)
    .addColumn("id", "BIGINT")
    .addColumn("name", "STRING")
    .addColumn("city", "STRING")
    .addColumn("ts", "TIMESTAMP")
    .property("delta.enableChangeDataFeed", "true")      # needed in task 6
    .execute())
# ANCHOR_END: t3_create


# ANCHOR: t3_solution
def latest_per_id(df):
    w = Window.partitionBy("id").orderBy(sf.col("ts").desc())
    return (
        df.withColumn("_rn", sf.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn", "_ingested_at", "_source_file")
    )


def apply_cdc_v1(batch_df, batch_id):
    latest = latest_per_id(batch_df)
    (DeltaTable.forPath(batch_df.sparkSession, silver_path).alias("t")
        .merge(latest.alias("s"), "t.id = s.id")
        .whenMatchedDelete(condition="s.op = 'D'")
        .whenMatchedUpdate(
            condition="s.op <> 'D'",
            set={"name": "s.name", "city": "s.city", "ts": "s.ts"})
        .whenNotMatchedInsert(
            condition="s.op <> 'D'",
            values={"id": "s.id", "name": "s.name", "city": "s.city", "ts": "s.ts"})
        .execute())


def run_silver(fn):
    q = (
        spark.readStream.format("delta").load(bronze_path)
        .writeStream
        .foreachBatch(fn)
        .option("checkpointLocation", f"{chk}/silver")
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    return q
# ANCHOR_END: t3_solution


# ANCHOR: t3_check
run_silver(apply_cdc_v1)
silver = spark.read.format("delta").load(silver_path).orderBy("id")
silver.show()

state = {r["id"]: r["city"] for r in silver.collect()}
assert state == {1: "Dnipro", 2: "Kharkiv", 4: "Kyiv"}, state
print("✅ task 3")
# ANCHOR_END: t3_check


# %% Task 4 ------------------------------------------------------------------
# ANCHOR: t4_solution
def apply_cdc_v2(batch_df, batch_id):
    latest = latest_per_id(batch_df)
    (DeltaTable.forPath(batch_df.sparkSession, silver_path).alias("t")
        .merge(latest.alias("s"), "t.id = s.id")
        .whenMatchedDelete(condition="s.op = 'D' AND s.ts > t.ts")
        .whenMatchedUpdate(
            condition="s.op <> 'D' AND s.ts > t.ts",
            set={"name": "s.name", "city": "s.city", "ts": "s.ts"})
        .whenNotMatchedInsert(
            condition="s.op <> 'D'",
            values={"id": "s.id", "name": "s.name", "city": "s.city", "ts": "s.ts"})
        .execute())


drop_batch("batch3", batch3)
run_bronze(cdc_schema)
run_silver(apply_cdc_v2)
# ANCHOR_END: t4_solution

# ANCHOR: t4_check
silver = spark.read.format("delta").load(silver_path).orderBy("id")
silver.show()
state = {r["id"]: r["city"] for r in silver.collect()}
assert state == {1: "Dnipro", 4: "Kyiv", 5: "Kyiv"}, state   # Alice NOT moved to Lviv, Bob deleted
print("✅ task 4")
# ANCHOR_END: t4_check


# %% Task 5 ------------------------------------------------------------------
# ANCHOR: t5ab_solution
drop_batch("batch4", batch4)

# 5a — infer the schema of the new file and diff it against the one we stream with
inferred = spark.read.json(f"{landing}/batch4.json")
new_cols = set(inferred.columns) - set(cdc_schema.fieldNames())
print("new columns in source:", new_cols)

# 5b — the same file read with the OLD schema: email silently disappears
print("columns with old schema:", spark.read.schema(cdc_schema).json(f"{landing}/batch4.json").columns)
# ANCHOR_END: t5ab_solution

# ANCHOR: t5ab_check
assert new_cols == {"email"}
print("✅ task 5a/5b")
# ANCHOR_END: t5ab_check

# ANCHOR: t5c_solution
# StructType.add() mutates in place — build a NEW StructType instead.
cdc_schema_v2 = StructType(cdc_schema.fields + [StructField("email", StringType())])
run_bronze(cdc_schema_v2)
# ANCHOR_END: t5c_solution

# ANCHOR: t5c_check
bronze = spark.read.format("delta").load(bronze_path)
bronze.printSchema()
assert "email" in bronze.columns
print("✅ task 5c")
# ANCHOR_END: t5c_check


# ANCHOR: t5d_solution
def evolve_target_schema(batch_df, path, ignore=("op",)):
    """Add columns that exist in the batch but not in the target — explicitly, so
    we have a place to log / validate / reject the change.

    Appending an EMPTY DataFrame with mergeSchema=true commits only a schema
    change (no data files) — a pure-PySpark way to say ALTER TABLE ADD COLUMNS.
    """
    session = batch_df.sparkSession
    target_cols = set(session.read.format("delta").load(path).columns)
    missing = [f for f in batch_df.schema.fields if f.name not in target_cols and f.name not in ignore]
    if missing:
        print(f"[batch] adding columns to target: {[f'{f.name} {f.dataType.simpleString()}' for f in missing]}")
        (session.createDataFrame([], StructType(missing))
            .write.format("delta").mode("append")
            .option("mergeSchema", "true")
            .save(path))
    return missing


def apply_cdc_v3(batch_df, batch_id):
    latest = latest_per_id(batch_df)
    evolve_target_schema(latest, silver_path)

    cols = [c for c in latest.columns if c != "op"]
    set_map = {c: f"s.{c}" for c in cols if c != "id"}
    values = {c: f"s.{c}" for c in cols}

    # a FRESH handle: one created before the schema change still sees the old schema
    (DeltaTable.forPath(batch_df.sparkSession, silver_path).alias("t")
        .merge(latest.alias("s"), "t.id = s.id")
        .whenMatchedDelete(condition="s.op = 'D' AND s.ts > t.ts")
        .whenMatchedUpdate(condition="s.op <> 'D' AND s.ts > t.ts", set=set_map)
        .whenNotMatchedInsert(condition="s.op <> 'D'", values=values)
        .execute())


run_silver(apply_cdc_v3)
# ANCHOR_END: t5d_solution

# ANCHOR: t5d_check
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
# ANCHOR_END: t5d_check


# %% Task 6 ------------------------------------------------------------------
# ANCHOR: t6_solution
changes = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .load(silver_path)
)
# ANCHOR_END: t6_solution

# ANCHOR: t6_check
(changes.select("id", "city", "email", "_change_type", "_commit_version")
    .orderBy("_commit_version", "id").show(50))

deleted = {r["id"] for r in changes.filter("_change_type = 'delete'").select("id").collect()}
assert deleted == {2}, deleted

dan_after = (changes.filter("id = 4 AND _change_type = 'update_postimage'")
             .orderBy(sf.col("_commit_version").desc()).first())
assert dan_after["email"] == "dan@example.com"
print("✅ task 6")
# ANCHOR_END: t6_check


# %% Task 7 — watch it run continuously ---------------------------------------
# ANCHOR: t7_solution
import time

bronze_q = (
    spark.readStream.format("json").schema(cdc_schema_v2).load(landing)
    .withColumn("_ingested_at", sf.current_timestamp())
    .withColumn("_source_file", sf.col("_metadata.file_name"))
    .writeStream.format("delta")
    .option("checkpointLocation", f"{chk}/bronze")       # SAME checkpoint: continues where we stopped
    .option("mergeSchema", "true")
    .trigger(processingTime="5 seconds")
    .queryName("bronze_live")
    .start(bronze_path)
)
silver_q = (
    spark.readStream.format("delta").load(bronze_path)
    .writeStream.foreachBatch(apply_cdc_v3)
    .option("checkpointLocation", f"{chk}/silver")
    .trigger(processingTime="5 seconds")
    .queryName("silver_live")
    .start()
)

drop_batch("batch5", [
    {"op": "U", "id": 5, "name": "Eve", "city": "Odesa", "email": "eve@example.com", "ts": "2026-10-05T09:00:00"},
])


def eve_city():
    row = spark.read.format("delta").load(silver_path).filter("id = 5").first()
    return row["city"] if row else None


deadline = time.time() + 90
while eve_city() != "Odesa" and time.time() < deadline:
    print("waiting…", bronze_q.status["message"], "|", silver_q.status["message"])
    time.sleep(3)

print("bronze batches so far:", [p["batchId"] for p in bronze_q.recentProgress])
bronze_q.stop()
silver_q.stop()
# ANCHOR_END: t7_solution

# ANCHOR: t7_check
assert eve_city() == "Odesa"
print("✅ task 7")
# ANCHOR_END: t7_check

# %%
spark.stop()
