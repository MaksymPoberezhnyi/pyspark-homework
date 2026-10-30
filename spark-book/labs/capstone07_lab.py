# %% [markdown]
# # Capstone 7 — live Wikipedia edits: your workbook
#
# 1. In a terminal (book folder):
#        python tools/wiki_collector.py --out data/wiki/landing --minutes 15 --drift-after-files 6
#    (`--fake` if the stream is blocked on your network)
# 2. Wait until ~3 files have landed, then work through the cells below.

# %%
# ANCHOR: setup
import json
import os
import time

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


# %% C1 — explore the raw events
# ANCHOR: c1_starter
raw = spark.read.json(landing)
# TODO: printSchema(); count events per `type`; for type = 'log', count per (log_type, log_action);
#       how many distinct titles vs events?
# ANCHOR_END: c1_starter


# %% C2 — a home-made schemaLocation with schema hints
# ANCHOR: c2_starter
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
IGNORED = ["$schema", "log_params"]     # type varies per event → keep only in the raw JSON


def apply_hints(schema, hints=HINTS, ignored=IGNORED):
    # TODO: return a NEW StructType where
    #   - every inferred field that has a hint is replaced by the hinted field,
    #   - fields named in `ignored` are dropped,
    #   - hinted fields that inference did not see are appended at the end.
    ...


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
if schema is None:
    schema = infer_schema()
    save_schema(schema)
print(schema.simpleString()[:300], "…")
assert {"log_type", "log_action", "revision", "length"} <= set(schema.fieldNames())
assert "log_params" not in schema.fieldNames()
print("✅ C2")
# ANCHOR_END: c2_starter


# %% C3 — bronze: raw text in, parsed columns + rescued keys out
# ANCHOR: c3_starter
def start_bronze(schema, trigger="10 seconds"):
    # TODO: readStream format("text") (+ pathGlobFilter "*.json") from landing, then select:
    #   _raw = value, e = from_json(value, schema), _source_file, _ingested_at
    # then: e.*, _raw, _source_file, _ingested_at, and
    #   _rescued_keys = array_except(json_object_keys(_raw), array of known names incl. IGNORED)
    # writeStream delta → bronze_path, checkpoint f"{chk}/bronze", mergeSchema, queryName "wiki_bronze",
    # trigger: availableNow=True if trigger == "availableNow" else processingTime=trigger
    # return the started query (no awaitTermination!)
    ...
# ANCHOR_END: c3_starter


# %% C4 — silver: CDC MERGE into the current state of every article
# ANCHOR: c4_starter
def to_cdc(df):
    # TODO: op = 'I' for type new, 'U' for edit, 'D' for log with log_type = log_action = 'delete';
    #       drop everything else; select op, page (= title), ts (= timestamp_seconds(timestamp)),
    #       rev_id (= revision.new), page_length (= length.new), last_user, last_by_bot, last_comment
    ...


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
    # TODO: to_cdc → newest event per page (order by ts desc, rev_id desc nulls last)
    #       joined with per-page n_events / min_ts → MERGE on page:
    #   D and s.ts >= t.ts → delete
    #   not D and s.ts >= t.ts → update STATE_COLS, events_seen = t.events_seen + s.n_events
    #   not matched and not D → insert (events_seen = n_events, first_seen = min_ts)
    ...


def start_silver(trigger="10 seconds"):
    w = (
        spark.readStream.format("delta").load(bronze_path)
        .writeStream.foreachBatch(upsert_pages)
        .option("checkpointLocation", f"{chk}/silver")
        .queryName("wiki_silver")
    )
    w = w.trigger(availableNow=True) if trigger == "availableNow" else w.trigger(processingTime=trigger)
    return w.start()
# ANCHOR_END: c4_starter


# %% C5 — run the pipeline and watch it
# ANCHOR: c5_starter
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
# ANCHOR_END: c5_starter


# %% C6 — ask the data questions
# ANCHOR: c6_starter
bronze = spark.read.format("delta").load(bronze_path)
pages = spark.read.format("delta").load(pages_path)
# TODO 1: the 10 most active articles (silver, events_seen)
# TODO 2: share of edit/new events made by bots vs humans (bronze)
# TODO 3: edits per minute (bronze, sf.window over timestamp_seconds(timestamp))
# TODO 4: the 5 biggest single edits in bytes added (length.new - length.old)
# TODO 5: silver's change feed: counts per _change_type, and the deleted articles
# ANCHOR_END: c6_starter


# %% C7 — schema drift: detect, evolve, restart
# ANCHOR: c7_starter
# TODO: explode bronze._rescued_keys and count per key → new_keys
# If there are new keys:
#   1. stop bronze_q and silver_q
#   2. schema = infer_schema(sample_files=None); save_schema(schema)
#   3. restart start_bronze(schema); wait until the new column shows up in bronze; then start_silver()
#   4. check the new column shows up in bronze
# ANCHOR_END: c7_starter

# %%
for q in spark.streams.active:
    q.stop()
spark.stop()
