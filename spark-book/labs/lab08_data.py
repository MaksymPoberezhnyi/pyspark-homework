# ANCHOR: all
"""Chapter 8 lab: three daily loads from the source system + small helpers.

The source sends only NEW or CHANGED customers each day (a changeset, not a
full snapshot). Calling reset_lab() deletes every table of the lab.
"""
import os
import shutil

import pyspark.sql.functions as sf
from delta.tables import DeltaTable

from common import DATA_DIR

base = os.path.join(DATA_DIR, "scd_lab")
scd1_path, scd2_path, scd3_path = f"{base}/dim_scd1", f"{base}/dim_scd2", f"{base}/dim_scd3"
scd4_path, scd4_hist_path = f"{base}/dim_scd4_current", f"{base}/dim_scd4_history"

schema = "customer_id BIGINT, name STRING, city STRING, tier STRING"

D1, D2, D3 = "2026-10-01", "2026-10-05", "2026-10-10"
TRACKED = ["name", "city", "tier"]
END_OF_TIME = "9999-12-31"


def reset_lab():
    shutil.rmtree(base, ignore_errors=True)
    print("lab reset:", base)


def make_days(spark):
    day1 = spark.createDataFrame([
        (1, "Alice", "Kyiv",  "gold"),
        (2, "Bob",   "Lviv",  "silver"),
        (3, "Carol", "Odesa", "bronze"),
    ], schema)
    day2 = spark.createDataFrame([
        (1, "Alice", "Dnipro", "gold"),     # city changed
        (2, "Bob",   "Lviv",   "gold"),     # tier changed
        (3, "Carol", "Odesa",  "bronze"),   # sent again, nothing changed
        (4, "Dan",   "Kyiv",   "silver"),   # new
    ], schema)
    day3 = spark.createDataFrame([
        (1, "Alice", "Kyiv",  "gold"),      # moved BACK to Kyiv
        (4, "Dan",   "Kyiv",  "silver"),    # unchanged
        (5, "Eve",   "Lviv",  "bronze"),    # new
    ], schema)
    return day1, day2, day3


def add_hash(df, cols=TRACKED):
    """One hash over the tracked attributes = a cheap 'did anything change?' check.

    Why coalesce: concat_ws SKIPS nulls, so ('a', null) and (null, 'a') would both
    give 'a' and hash the same. Replacing null with a marker keeps them different.
    """
    parts = [sf.coalesce(sf.col(c).cast("string"), sf.lit("<null>")) for c in cols]
    return df.withColumn("row_hash", sf.sha2(sf.concat_ws("||", *parts), 256))


def last_merge_metrics(spark, path):
    """operationMetrics of the latest commit: numTargetRowsUpdated, numTargetRowsInserted, ..."""
    return DeltaTable.forPath(spark, path).history(1).first()["operationMetrics"]
# ANCHOR_END: all
