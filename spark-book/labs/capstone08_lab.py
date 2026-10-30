# %% [markdown]
# # Capstone 8 — NYC restaurant grades as an SCD Type 2 dimension: your workbook
#
# First, in a terminal (book folder):  python tools/get_nyc_inspections.py   (or --fake)

# %%
# ANCHOR: setup
import os
import shutil

import pyspark.sql.functions as sf
from delta.tables import DeltaTable
from pyspark.sql import Window

from common import DATA_DIR, get_spark
from lab08_data import END_OF_TIME, add_hash

spark = get_spark("capstone08-nyc")

nyc = os.path.join(DATA_DIR, "nyc")
raw_csv = f"{nyc}/inspections.csv"
dim_path = f"{nyc}/dim_restaurant_scd2"
shutil.rmtree(dim_path, ignore_errors=True)          # re-running the capstone starts from scratch

KEY = "camis"
TRACKED = ["dba", "cuisine_description", "zipcode", "grade"]
# ANCHOR_END: setup


# %% C1 — load and profile
# ANCHOR: c1_starter
raw = (
    spark.read.option("header", True).option("escape", '"').csv(raw_csv)
    .withColumn("grade_date", sf.to_date(sf.substring("grade_date", 1, 10)))
    .withColumn("inspection_date", sf.to_date(sf.substring("inspection_date", 1, 10)))
    .withColumn("score", sf.col("score").cast("int"))
)
# TODO: rows, distinct restaurants, min/max grade_date, rows per grade
# TODO: how many rows per (camis, grade_date)? what does that tell you about the file's grain?
# TODO: how many (camis, grade_date) pairs have MORE than one distinct grade?
# ANCHOR_END: c1_starter


# %% C2 — one row per restaurant per grading
# ANCHOR: c2_starter
# TODO: keep grade in (A, B, C) with a grade_date; one row per (camis, grade_date) using a Window:
#       on a conflict keep the worst grade, then the latest inspection_date, then dba (deterministic!)
#       select camis, dba, boro, zipcode, cuisine_description, grade, grade_date — and .cache() it
gradings = ...

assert gradings.groupBy(KEY, "grade_date").count().filter("count > 1").count() == 0
print("✅ C2 —", gradings.count(), "gradings")
# ANCHOR_END: c2_starter


# %% C3 — a reusable SCD2 merge with a per-row effective date
# ANCHOR: c3_starter
def scd2_merge_rows(updates, path, key, tracked, eff_col):
    # TODO: generalise Chapter 8's scd2_merge:
    #   - the key column and the tracked columns are parameters (add_hash(updates, tracked))
    #   - eff = sf.col(eff_col): every row has its OWN effective date
    #   - first load: write with valid_from = eff, valid_to = END_OF_TIME, is_current = true
    #   - a change only counts if it is NEWER than the current version (u.eff > c.valid_from),
    #     both in the `changed` filter and in the whenMatchedUpdate condition
    #   - insert values: every data column + validity columns (build the dict from upd.columns)
    ...
# ANCHOR_END: c3_starter


# %% C4 — replay history month by month
# ANCHOR: c4_starter
months = [r["m"] for r in gradings.select(sf.trunc("grade_date", "month").alias("m")).distinct().orderBy("m").collect()]
print(f"{len(months)} monthly loads: {months[0]} … {months[-1]}")

latest_in_month = Window.partitionBy(KEY).orderBy(sf.col("grade_date").desc())
for i, m in enumerate(months, 1):
    changeset = (
        gradings.filter(sf.trunc("grade_date", "month") == sf.lit(m))
        .withColumn("_rn", sf.row_number().over(latest_in_month)).filter("_rn = 1").drop("_rn")
    )
    scd2_merge_rows(changeset, dim_path, KEY, TRACKED, "grade_date")
    if i % 6 == 0 or i == len(months):
        print(f"  loaded {m}  ({i}/{len(months)})")

dim = spark.read.format("delta").load(dim_path).cache()
print("dimension rows:", dim.count(), "| restaurants:", dim.select(KEY).distinct().count())
# ANCHOR_END: c4_starter


# %% C5 — data-quality checks every SCD2 table should pass
# ANCHOR: c5_starter
# TODO 1: keys that do not have exactly one current row  → multi_current
# TODO 2: versions whose valid_to != the next version's valid_from (lead over valid_from) → gaps
# TODO 3: versions with valid_to <= valid_from → bad_intervals
multi_current = gaps = bad_intervals = ...

assert multi_current.count() == 0 and gaps.count() == 0 and bad_intervals.count() == 0
print("✅ C5")
# ANCHOR_END: c5_starter


# %% C6 — questions only an SCD2 table can answer
# ANCHOR: c6_starter
def as_of(dim_df, day):
    d = sf.lit(day).cast("date")
    return dim_df.filter((sf.col("valid_from") <= d) & (sf.col("valid_to") > d))


mid = months[len(months) // 2].isoformat()
# TODO Q1: grade distribution on `mid` vs today
# TODO Q2: restaurants whose grade went straight from A to C (lag over valid_from)
# TODO Q3: average and median number of days a grade lasts, per grade (closed versions only)
# TODO Q4: restaurants that had more than one DBA over the period
# TODO Q5: cuisines (n >= 50) with the highest share of C grades — today vs on `mid`
# ANCHOR_END: c6_starter


# %% C7 — idempotency and late data
# ANCHOR: c7_starter
# TODO: re-run the LAST month's changeset → the row count must not change
# TODO: re-deliver the FIRST month's changeset (late data) → what happens, and why?
# ANCHOR_END: c7_starter

# %%
spark.stop()
