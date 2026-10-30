# %% [markdown]
# # Capstone 8 — NYC restaurant grades as an SCD Type 2 dimension
#
# Prerequisite (terminal, book folder):   python tools/get_nyc_inspections.py   (or --fake)
# Run as a script:                         python solutions/capstone08_nyc.py

# %%
# ANCHOR: setup
import os
import shutil
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "labs"))

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


# %% C1 — load and profile ---------------------------------------------------------------
# ANCHOR: c1_solution
raw = (
    spark.read.option("header", True).option("escape", '"').csv(raw_csv)
    .withColumn("grade_date", sf.to_date(sf.substring("grade_date", 1, 10)))
    .withColumn("inspection_date", sf.to_date(sf.substring("inspection_date", 1, 10)))
    .withColumn("score", sf.col("score").cast("int"))
)
raw.printSchema()
print("rows:", raw.count(), "| restaurants:", raw.select(KEY).distinct().count())
print("date range:", raw.agg(sf.min("grade_date"), sf.max("grade_date")).first())
raw.groupBy("grade").count().orderBy("grade").show()

print("rows per (restaurant, grade_date): the file is VIOLATION-level, not inspection-level")
(raw.groupBy(KEY, "grade_date").count()
    .groupBy(sf.col("count").alias("rows_per_grading")).count()
    .orderBy("rows_per_grading").show(10))

conflicts = raw.groupBy(KEY, "grade_date").agg(sf.countDistinct("grade").alias("n_grades")).filter("n_grades > 1")
print("gradings with conflicting grades on the same day:", conflicts.count())
# ANCHOR_END: c1_solution


# %% C2 — one row per restaurant per grading --------------------------------------------------
# ANCHOR: c2_solution
w = Window.partitionBy(KEY, "grade_date").orderBy(
    sf.col("grade").desc(),            # on a conflict keep the WORST grade (C > B > A as strings)
    sf.col("inspection_date").desc(),
    sf.col("dba"),
)
gradings = (
    raw.filter(sf.col("grade").isin("A", "B", "C") & sf.col("grade_date").isNotNull())
    .withColumn("_rn", sf.row_number().over(w))
    .filter("_rn = 1")
    .select(KEY, "dba", "boro", "zipcode", "cuisine_description", "grade", "grade_date")
    .cache()
)
print("gradings:", gradings.count())
assert gradings.groupBy(KEY, "grade_date").count().filter("count > 1").count() == 0
# ANCHOR_END: c2_solution


# %% C3 — a reusable SCD2 merge with a per-row effective date ------------------------------------
# ANCHOR: c3_solution
def scd2_merge_rows(updates, path, key, tracked, eff_col):
    """SCD2 MERGE where every source row carries its OWN effective date.

    `updates` must hold at most one row per key. Changes that are not newer than the
    current version are ignored (late data is a separate problem, see the review).
    """
    upd = add_hash(updates, tracked).withColumn("eff", sf.col(eff_col))
    data_cols = [c for c in upd.columns if c != "eff"]

    if not DeltaTable.isDeltaTable(spark, path):
        (upd.withColumn("valid_from", sf.col("eff"))
            .withColumn("valid_to", sf.lit(END_OF_TIME).cast("date"))
            .withColumn("is_current", sf.lit(True))
            .drop("eff")
            .write.format("delta").save(path))
        return

    target = DeltaTable.forPath(spark, path)
    current = target.toDF().filter("is_current")
    changed = (
        upd.alias("u")
        .join(current.alias("c"), sf.col(f"u.{key}") == sf.col(f"c.{key}"))
        .filter((sf.col("u.row_hash") != sf.col("c.row_hash")) & (sf.col("u.eff") > sf.col("c.valid_from")))
        .select(sf.lit(None).cast(upd.schema[key].dataType).alias("merge_key"), *[sf.col(f"u.{c}") for c in upd.columns])
    )
    staged = upd.select(sf.col(key).alias("merge_key"), *upd.columns).unionByName(changed)

    (target.alias("t")
        .merge(staged.alias("s"), f"t.{key} = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash AND s.eff > t.valid_from",
            set={"is_current": "false", "valid_to": "s.eff"})
        .whenNotMatchedInsert(
            # keyed copies of KNOWN keys never get here (they matched); keyed copies of NEW keys do
            values={**{c: f"s.{c}" for c in data_cols},
                    "valid_from": "s.eff",
                    "valid_to": f"cast('{END_OF_TIME}' as date)",
                    "is_current": "true"})
        .execute())
# ANCHOR_END: c3_solution


# %% C4 — replay history month by month ---------------------------------------------------------
# ANCHOR: c4_solution
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
# ANCHOR_END: c4_solution


# %% C5 — data-quality checks every SCD2 table should pass -----------------------------------------
# ANCHOR: c5_solution
# 1. exactly one current version per key
multi_current = dim.groupBy(KEY).agg(sf.sum(sf.col("is_current").cast("int")).alias("n")).filter("n <> 1")
# 2. versions are contiguous: each version ends exactly where the next one starts
nxt = Window.partitionBy(KEY).orderBy("valid_from")
gaps = (dim.withColumn("next_from", sf.lead("valid_from").over(nxt))
        .filter(sf.col("next_from").isNotNull() & (sf.col("valid_to") != sf.col("next_from"))))
# 3. no empty or negative intervals
bad_intervals = dim.filter(sf.col("valid_to") <= sf.col("valid_from"))

print("keys with != 1 current row:", multi_current.count())
print("gaps/overlaps:", gaps.count(), "| empty intervals:", bad_intervals.count())
assert multi_current.count() == 0 and gaps.count() == 0 and bad_intervals.count() == 0

print("versions per restaurant:")
dim.groupBy(KEY).count().groupBy(sf.col("count").alias("versions")).count().orderBy("versions").show()
# ANCHOR_END: c5_solution


# %% C6 — questions only an SCD2 table can answer ----------------------------------------------------
# ANCHOR: c6_solution
def as_of(dim_df, day):
    d = sf.lit(day).cast("date")
    return dim_df.filter((sf.col("valid_from") <= d) & (sf.col("valid_to") > d))


mid = months[len(months) // 2].isoformat()
print(f"Q1 — grade distribution on {mid} vs today")
(as_of(dim, mid).groupBy("grade").count().withColumnRenamed("count", f"on_{mid}")
    .join(dim.filter("is_current").groupBy("grade").count().withColumnRenamed("count", "today"), "grade", "full")
    .orderBy("grade").show())

print("Q2 — restaurants that went straight from A to C")
prev = Window.partitionBy(KEY).orderBy("valid_from")
transitions = dim.withColumn("prev_grade", sf.lag("grade").over(prev))
(transitions.filter("prev_grade = 'A' AND grade = 'C'")
    .select(KEY, "dba", "cuisine_description", "valid_from").orderBy(sf.desc("valid_from")).show(10, truncate=30))

print("Q3 — how long does a grade last? (closed versions only)")
(dim.filter("NOT is_current")
    .withColumn("days", sf.datediff("valid_to", "valid_from"))
    .groupBy("grade").agg(sf.count("*").alias("versions"), sf.round(sf.avg("days")).alias("avg_days"),
                          sf.percentile_approx("days", 0.5).alias("median_days"))
    .orderBy("grade").show())

print("Q4 — restaurants that changed their name (DBA) over the period")
(dim.groupBy(KEY).agg(sf.countDistinct("dba").alias("names"), sf.collect_set("dba").alias("all_names"))
    .filter("names > 1").show(10, truncate=60))

print("Q5 — cuisines with the biggest share of C grades, as of today vs as of", mid)


def c_share(df):
    return df.groupBy("cuisine_description").agg(
        sf.count("*").alias("n"), sf.round(sf.avg((sf.col("grade") == "C").cast("int")), 3).alias("c_share"))


(c_share(dim.filter("is_current")).withColumnRenamed("c_share", "c_share_today")
    .join(c_share(as_of(dim, mid)).select("cuisine_description", sf.col("c_share").alias(f"c_share_{mid}")),
          "cuisine_description")
    .filter("n >= 50").orderBy(sf.desc("c_share_today")).show(10, truncate=30))
# ANCHOR_END: c6_solution


# %% C7 — idempotency and late data -----------------------------------------------------------------
# ANCHOR: c7_solution
before = dim.count()
last = months[-1]
changeset = (gradings.filter(sf.trunc("grade_date", "month") == sf.lit(last))
             .withColumn("_rn", sf.row_number().over(latest_in_month)).filter("_rn = 1").drop("_rn"))
scd2_merge_rows(changeset, dim_path, KEY, TRACKED, "grade_date")          # retry of the last load
after_retry = spark.read.format("delta").load(dim_path).count()
print("rows before / after re-running the last month:", before, after_retry)
assert after_retry == before

first = months[0]
old = (gradings.filter(sf.trunc("grade_date", "month") == sf.lit(first))
       .withColumn("_rn", sf.row_number().over(latest_in_month)).filter("_rn = 1").drop("_rn"))
scd2_merge_rows(old, dim_path, KEY, TRACKED, "grade_date")                # a LATE re-delivery
print("rows after re-delivering the FIRST month:", spark.read.format("delta").load(dim_path).count(),
      "(unchanged: the 'newer than current' guard ignores it)")
# ANCHOR_END: c7_solution

# %%
spark.stop()
