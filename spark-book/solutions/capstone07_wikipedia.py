# %% [markdown]
# # Capstone 7 — live Wikipedia edits: Auto Loader-style ingestion + CDC MERGE
#
# Prerequisite (separate terminal, from the book folder):
#     python tools/wiki_collector.py --out data/wiki/landing --minutes 15 --drift-after-files 6
# (add --fake if your network blocks stream.wikimedia.org)
#
# Run as a script once ~3 files exist:  python solutions/capstone07_wikipedia.py

# %%
# ANCHOR: setup
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "labs"))

import pyspark.sql.functions as sf
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from common import DATA_DIR, get_spark

spark = get_spark("capstone07-wikipedia")

wiki = os.path.join(DATA_DIR, "wiki")
landing = f"{wiki}/landing"            # tools/wiki_collector.py writes here
schema_dir = f"{wiki}/_schemas"        # our home-made cloudFiles.schemaLocation
bronze_path = f"{wiki}/bronze"
pages_path = f"{wiki}/silver_pages"    # current state: one row per article
chk = f"{wiki}/_checkpoints"


def landed_files():
    return sorted(f for f in os.listdir(landing) if f.endswith(".json")) if os.path.isdir(landing) else []


print(len(landed_files()), "files in landing so far")
# ANCHOR_END: setup


# %% C1 — explore the raw events ------------------------------------------------
# ANCHOR: c1_solution
raw = spark.read.json(landing)
raw.printSchema()
raw.groupBy("type").count().orderBy(sf.desc("count")).show()
if "log_type" in raw.columns:                       # only if a log event has landed yet
    (raw.filter("type = 'log'").groupBy("log_type", "log_action").count()
        .orderBy(sf.desc("count")).show(truncate=False))
print("distinct pages:", raw.select("title").distinct().count(), "| events:", raw.count())
# ANCHOR_END: c1_solution


# %% C2 — a home-made schemaLocation with schema hints ---------------------------
# ANCHOR: c2_solution
# Types we insist on, whatever inference says (≈ cloudFiles.schemaHints).
# Listing log_type/log_action also guarantees the columns exist even if the
# sampled files happened to contain no log events at all.
HINTS = StructType([
    StructField("type", StringType()),
    StructField("title", StringType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("namespace", LongType()),
    StructField("timestamp", LongType()),            # unix seconds
    StructField("comment", StringType()),
    StructField("log_type", StringType()),
    StructField("log_action", StringType()),
    StructField("length", StructType([StructField("old", LongType()), StructField("new", LongType())])),
    StructField("revision", StructType([StructField("old", LongType()), StructField("new", LongType())])),
])
# Fields we deliberately do NOT give a column: their type changes from event to event
# (log_params is an array for some log types and an object for others).
# They stay available in the raw JSON.
IGNORED = ["$schema", "log_params"]


def apply_hints(schema, hints=HINTS, ignored=IGNORED):
    hinted = {f.name: f for f in hints.fields}
    fields = [hinted.get(f.name, f) for f in schema.fields if f.name not in ignored]
    have = {f.name for f in fields}
    fields += [f for f in hints.fields if f.name not in have]
    return StructType(fields)


def infer_schema(sample_files=2):
    """Like Auto Loader: infer from the first files only, not the whole folder."""
    files = [os.path.join(landing, f) for f in landed_files()[:sample_files]] if sample_files else [landing]
    return apply_hints(spark.read.json(files).schema)


def schema_versions():
    if not os.path.isdir(schema_dir):
        return []
    return sorted(int(f.split("_v")[1].split(".")[0]) for f in os.listdir(schema_dir) if f.startswith("schema_v"))


def save_schema(schema):
    os.makedirs(schema_dir, exist_ok=True)
    n = (schema_versions() or [-1])[-1] + 1
    with open(f"{schema_dir}/schema_v{n}.json", "w") as f:
        f.write(schema.json())
    print(f"saved schema v{n} ({len(schema.fields)} columns)")
    return n


def load_schema():
    versions = schema_versions()
    if not versions:
        return None
    with open(f"{schema_dir}/schema_v{versions[-1]}.json") as f:
        return StructType.fromJson(json.load(f))


schema = load_schema()
if schema is None:                                   # first run: infer + store
    schema = infer_schema()
    save_schema(schema)
print(schema.simpleString()[:300], "…")
# ANCHOR_END: c2_solution


# %% C3 — bronze: raw text in, parsed columns + rescued keys out -------------------
# ANCHOR: c3_solution
def start_bronze(schema, trigger="10 seconds"):
    known = sf.array(*[sf.lit(n) for n in schema.fieldNames() + IGNORED])
    w = (
        spark.readStream
        .format("text")                                  # one JSON document per line, kept verbatim
        .option("pathGlobFilter", "*.json")
        .load(landing)
        .select(
            sf.col("value").alias("_raw"),
            sf.from_json("value", schema).alias("e"),
            sf.col("_metadata.file_name").alias("_source_file"),
            sf.current_timestamp().alias("_ingested_at"),
        )
        .select(
            "e.*", "_raw", "_source_file", "_ingested_at",
            # ≈ Auto Loader's _rescued_data: top-level keys our schema does not know about
            sf.array_except(sf.expr("json_object_keys(_raw)"), known).alias("_rescued_keys"),
        )
        .writeStream.format("delta")
        .option("checkpointLocation", f"{chk}/bronze")
        .option("mergeSchema", "true")
        .queryName("wiki_bronze")
    )
    w = w.trigger(availableNow=True) if trigger == "availableNow" else w.trigger(processingTime=trigger)
    return w.start(bronze_path)
# ANCHOR_END: c3_solution


# %% C4 — silver: CDC MERGE into the current state of every article ----------------
# ANCHOR: c4_solution
def to_cdc(df):
    op = (
        sf.when(sf.col("type") == "new", "I")
        .when(sf.col("type") == "edit", "U")
        .when((sf.col("type") == "log") & (sf.col("log_type") == "delete") & (sf.col("log_action") == "delete"), "D")
    )
    return (
        df.withColumn("op", op)
        .filter(sf.col("op").isNotNull())                # moves, protections, uploads: not our concern here
        .select(
            "op",
            sf.col("title").alias("page"),
            sf.timestamp_seconds("timestamp").alias("ts"),
            sf.col("revision.new").alias("rev_id"),
            sf.col("length.new").alias("page_length"),
            sf.col("user").alias("last_user"),
            sf.col("bot").alias("last_by_bot"),
            sf.col("comment").alias("last_comment"),
        )
    )


(DeltaTable.createIfNotExists(spark)
    .location(pages_path)
    .addColumn("page", "STRING")
    .addColumn("ts", "TIMESTAMP")
    .addColumn("rev_id", "BIGINT")
    .addColumn("page_length", "BIGINT")
    .addColumn("last_user", "STRING")
    .addColumn("last_by_bot", "BOOLEAN")
    .addColumn("last_comment", "STRING")
    .addColumn("events_seen", "BIGINT")
    .addColumn("first_seen", "TIMESTAMP")
    .property("delta.enableChangeDataFeed", "true")
    .execute())

STATE_COLS = ["ts", "rev_id", "page_length", "last_user", "last_by_bot", "last_comment"]


def upsert_pages(batch_df, batch_id):
    ev = to_cdc(batch_df)
    # newest event per page; timestamps are whole seconds, so break ties on the revision id
    w = Window.partitionBy("page").orderBy(sf.col("ts").desc(), sf.col("rev_id").desc_nulls_last())
    stats = ev.groupBy("page").agg(sf.count("*").alias("n_events"), sf.min("ts").alias("min_ts"))
    latest = ev.withColumn("_rn", sf.row_number().over(w)).filter("_rn = 1").drop("_rn").join(stats, "page")

    (DeltaTable.forPath(batch_df.sparkSession, pages_path).alias("t")
        .merge(latest.alias("s"), "t.page = s.page")
        .whenMatchedDelete(condition="s.op = 'D' AND s.ts >= t.ts")
        .whenMatchedUpdate(
            condition="s.op <> 'D' AND s.ts >= t.ts",
            set={**{c: f"s.{c}" for c in STATE_COLS}, "events_seen": "t.events_seen + s.n_events"})
        .whenNotMatchedInsert(
            condition="s.op <> 'D'",
            values={"page": "s.page", **{c: f"s.{c}" for c in STATE_COLS},
                    "events_seen": "s.n_events", "first_seen": "s.min_ts"})
        .execute())


def start_silver(trigger="10 seconds"):
    w = (
        spark.readStream.format("delta").load(bronze_path)
        .writeStream.foreachBatch(upsert_pages)
        .option("checkpointLocation", f"{chk}/silver")
        .queryName("wiki_silver")
    )
    w = w.trigger(availableNow=True) if trigger == "availableNow" else w.trigger(processingTime=trigger)
    return w.start()
# ANCHOR_END: c4_solution


# %% C5 — run the pipeline and watch it -------------------------------------------
# ANCHOR: c5_solution
bronze_q = start_bronze(schema)
while not DeltaTable.isDeltaTable(spark, bronze_path):   # silver can only start once bronze exists
    time.sleep(2)
silver_q = start_silver()

for _ in range(8):                                        # ~2 minutes of watching
    time.sleep(15)
    b = bronze_q.lastProgress or {}
    s = silver_q.lastProgress or {}
    n_pages = spark.read.format("delta").load(pages_path).count()
    print(f"files={len(landed_files()):3d} | bronze batch {b.get('batchId')}: {b.get('numInputRows')} rows "
          f"| silver batch {s.get('batchId')}: {s.get('numInputRows')} rows | pages in silver: {n_pages}")
# ANCHOR_END: c5_solution


# %% C6 — ask the data questions ---------------------------------------------------
# ANCHOR: c6_solution
bronze = spark.read.format("delta").load(bronze_path)
pages = spark.read.format("delta").load(pages_path)

print("Most active articles right now:")
pages.orderBy(sf.desc("events_seen"), sf.desc("ts")).select(
    "page", "events_seen", "last_user", "last_by_bot", "page_length").show(10, truncate=40)

print("Who edits Wikipedia — bots or humans?")
(bronze.filter("type IN ('edit', 'new')")
    .groupBy("bot").count()
    .withColumn("share", sf.round(sf.col("count") / sf.sum("count").over(Window.partitionBy()), 3))
    .show())

print("Edits per minute:")
(bronze.filter("type = 'edit'")
    .groupBy(sf.window(sf.timestamp_seconds("timestamp"), "1 minute").alias("w"))
    .count()
    .select(sf.col("w.start").alias("minute"), "count")
    .orderBy("minute").show(30, truncate=False))

print("Biggest single edits (bytes added):")
(bronze.filter("type = 'edit'")
    .select("title", "user", (sf.col("length.new") - sf.col("length.old")).alias("bytes_added"))
    .orderBy(sf.desc("bytes_added")).show(5, truncate=40))

changes = (spark.read.format("delta").option("readChangeFeed", "true")
           .option("startingVersion", 0).load(pages_path))
print("Change feed of silver by change type:")
changes.groupBy("_change_type").count().show()
print("Deleted articles (removed from silver):")
changes.filter("_change_type = 'delete'").select("page", "last_user", "_commit_timestamp").show(10, truncate=40)
# ANCHOR_END: c6_solution


# %% C7 — schema drift: detect, evolve, restart -------------------------------------
# ANCHOR: c7_solution
rescued = (spark.read.format("delta").load(bronze_path)
           .select(sf.explode("_rescued_keys").alias("key"))
           .groupBy("key").count())
rescued.show()
new_keys = [r["key"] for r in rescued.collect()]

if new_keys:
    print("source sends keys our schema does not know:", new_keys)
    # 1. stop the pipeline (Auto Loader would fail the stream at this point)
    bronze_q.stop()
    silver_q.stop()
    # 2. infer from ALL files now, keep the hints, store as a new schema version
    schema = infer_schema(sample_files=None)
    save_schema(schema)
    # 3. restart with the SAME checkpoints: only new files are read, now with the new columns
    bronze_q = start_bronze(schema)
    # silver must not start before bronze has COMMITTED the evolved schema,
    # otherwise its Delta source would hit the schema change and stop again
    while not set(new_keys) <= set(spark.read.format("delta").load(bronze_path).columns):
        time.sleep(3)
    silver_q = start_silver()
    time.sleep(20)
    if silver_q.exception():
        raise silver_q.exception()
    print("bronze columns now:", [c for c in spark.read.format("delta").load(bronze_path).columns
                                  if not c.startswith("_")])
    # 4. optional backfill: rows ingested before the evolution still have NULL in the new
    #    columns — but their _raw JSON has the data, so nothing was lost.
else:
    print("no drift yet — run the collector with --drift-after-files and come back")
# ANCHOR_END: c7_solution

# %%
for q in spark.streams.active:
    q.stop()
spark.stop()
