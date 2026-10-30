# ANCHOR: all
"""Chapter 7 lab: paths, a fake source system, and the CDC events.

Calling reset_lab() deletes every table and checkpoint of the lab, so you can
start over at any time.
"""
import json
import os
import shutil

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from common import DATA_DIR

base = os.path.join(DATA_DIR, "cdc_lab")
landing = f"{base}/landing"          # the "source system" drops files here
bronze_path = f"{base}/bronze"       # raw events, append-only
silver_path = f"{base}/silver"       # current state of every customer
chk = f"{base}/_checkpoints"         # one sub-folder per streaming query


def reset_lab():
    shutil.rmtree(base, ignore_errors=True)
    os.makedirs(landing, exist_ok=True)
    print("lab reset:", base)


def drop_batch(name, events, folder=None):
    """Simulate the source system writing one JSON-lines file of CDC events.

    The file is written under a hidden name first and then renamed: Spark's
    file source (and Auto Loader) ignore files starting with '.' or '_', so a
    reader can never pick up a half-written file.
    """
    folder = folder or landing
    os.makedirs(folder, exist_ok=True)
    final = os.path.join(folder, f"{name}.json")
    tmp = os.path.join(folder, f".{name}.json.tmp")
    with open(tmp, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    os.replace(tmp, final)
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
# ANCHOR_END: all
