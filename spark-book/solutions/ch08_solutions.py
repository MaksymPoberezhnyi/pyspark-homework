# %% [markdown]
# # Chapter 8 — Slowly Changing Dimensions: reference solutions
#
# Run end-to-end as a self-test:  `python solutions/ch08_solutions.py`

# %%
# ANCHOR: setup
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "labs"))

import pyspark.sql.functions as sf
from delta.tables import DeltaTable
from pyspark.sql import Window

from common import get_spark
from lab08_data import (
    D1, D2, D3, END_OF_TIME, TRACKED, add_hash, base, last_merge_metrics, make_days,
    reset_lab, scd1_path, scd2_path, scd3_path, scd4_hist_path, scd4_path,
)

spark = get_spark("ch08-scd")
reset_lab()
day1, day2, day3 = make_days(spark)
# ANCHOR_END: setup


# %% Task 1 — SCD Type 1 ------------------------------------------------------------
# ANCHOR: t1_solution
def scd1_merge(updates, path):
    src = add_hash(updates)
    if not DeltaTable.isDeltaTable(spark, path):
        src.write.format("delta").save(path)
        return
    (DeltaTable.forPath(spark, path).alias("t")
        .merge(src.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash")   # skip unchanged rows
        .whenNotMatchedInsertAll()
        .execute())
# ANCHOR_END: t1_solution


# ANCHOR: t1_check
scd1_merge(day1, scd1_path)
scd1_merge(day2, scd1_path)
m = last_merge_metrics(spark, scd1_path)
print("day2:", {k: m[k] for k in ["numTargetRowsUpdated", "numTargetRowsInserted"]})
assert m["numTargetRowsUpdated"] == "2" and m["numTargetRowsInserted"] == "1"   # Alice+Bob updated, Dan inserted, Carol untouched

scd1_merge(day3, scd1_path)
dim1 = spark.read.format("delta").load(scd1_path).orderBy("customer_id")
dim1.drop("row_hash").show()
assert dim1.count() == 5
assert {r["customer_id"]: r["city"] for r in dim1.collect()}[1] == "Kyiv"
print("✅ task 1")
# ANCHOR_END: t1_check


# %% Task 2 — SCD Type 2: initial load ------------------------------------------------
# ANCHOR: t2_solution
def scd2_initial_load(df, path, effective_date):
    (add_hash(df)
        .withColumn("valid_from", sf.lit(effective_date).cast("date"))
        .withColumn("valid_to", sf.lit(END_OF_TIME).cast("date"))
        .withColumn("is_current", sf.lit(True))
        .write.format("delta").mode("overwrite").save(path))
# ANCHOR_END: t2_solution


# ANCHOR: t2_check
scd2_initial_load(day1, scd2_path, D1)
dim2 = spark.read.format("delta").load(scd2_path)
dim2.show()
assert dim2.count() == 3 and dim2.filter("is_current").count() == 3
print("✅ task 2")
# ANCHOR_END: t2_check


# %% Task 3 — SCD Type 2 MERGE with the staged union ----------------------------------
# ANCHOR: t3_solution
def scd2_merge(updates, path, effective_date):
    upd = add_hash(updates).withColumn("eff", sf.lit(effective_date).cast("date"))
    target = DeltaTable.forPath(spark, path)
    current = target.toDF().filter("is_current")

    # copy 2 of every CHANGED customer: NULL key → can never match → falls into the INSERT clause
    changed = (
        upd.alias("u")
        .join(current.alias("c"), sf.col("u.customer_id") == sf.col("c.customer_id"))
        .filter(sf.col("u.row_hash") != sf.col("c.row_hash"))
        .select(sf.lit(None).cast("bigint").alias("merge_key"), *[sf.col(f"u.{c}") for c in upd.columns])
    )
    # copy 1 of EVERY incoming customer, keyed normally
    staged = upd.select(sf.col("customer_id").alias("merge_key"), *upd.columns).unionByName(changed)

    (target.alias("t")
        .merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={"is_current": "false", "valid_to": "s.eff"})
        .whenNotMatchedInsert(values={
            "customer_id": "s.customer_id",
            "name": "s.name",
            "city": "s.city",
            "tier": "s.tier",
            "row_hash": "s.row_hash",
            "valid_from": "s.eff",
            "valid_to": f"cast('{END_OF_TIME}' as date)",
            "is_current": "true",
        })
        .execute())
# ANCHOR_END: t3_solution


# ANCHOR: t3_check
def show_dim2():
    (spark.read.format("delta").load(scd2_path)
        .drop("row_hash").orderBy("customer_id", "valid_from").show())


scd2_merge(day2, scd2_path, D2)
show_dim2()
dim2 = spark.read.format("delta").load(scd2_path)
assert dim2.count() == 6
assert dim2.filter("is_current").count() == 4

scd2_merge(day3, scd2_path, D3)
show_dim2()
dim2 = spark.read.format("delta").load(scd2_path)
assert dim2.count() == 8
assert (dim2.groupBy("customer_id").agg(sf.sum(sf.col("is_current").cast("int")).alias("n"))
        .filter("n <> 1").count() == 0)                          # exactly one current row per customer
alice = dim2.filter("customer_id = 1").orderBy("valid_from").collect()
assert [r["city"] for r in alice] == ["Kyiv", "Dnipro", "Kyiv"]
assert all(alice[i]["valid_to"] == alice[i + 1]["valid_from"] for i in range(len(alice) - 1))  # no gaps
print("✅ task 3")
# ANCHOR_END: t3_check


# %% Task 4 — idempotency and duplicate keys -------------------------------------------
# ANCHOR: t4a_solution
scd2_merge(day3, scd2_path, D3)       # a retry of the same load
# ANCHOR_END: t4a_solution

# ANCHOR: t4a_check
assert spark.read.format("delta").load(scd2_path).count() == 8
print("✅ task 4a")
# ANCHOR_END: t4a_check

# ANCHOR: t4b_solution
day4_dirty = spark.createDataFrame([
    (5, "Eve", "Odesa", "bronze"),
    (5, "Eve", "Kharkiv", "bronze"),
], "customer_id BIGINT, name STRING, city STRING, tier STRING")

try:
    scd2_merge(day4_dirty, scd2_path, "2026-10-12")
except Exception as e:                       # the Java stack trace above is expected
    print("MERGE failed as expected:", str(e).splitlines()[0][:200])

# keep ONE row per key, deterministically. (orderBy + dropDuplicates does NOT guarantee
# which row survives; a window does.) In real life order by the source's event time/sequence.
w = Window.partitionBy("customer_id").orderBy("city")
day4_clean = day4_dirty.withColumn("_rn", sf.row_number().over(w)).filter("_rn = 1").drop("_rn")
scd2_merge(day4_clean, scd2_path, "2026-10-12")
# ANCHOR_END: t4b_solution

# ANCHOR: t4b_check
eve = spark.read.format("delta").load(scd2_path).filter("customer_id = 5").orderBy("valid_from")
eve.drop("row_hash").show()
assert eve.count() == 2
print("✅ task 4b")
# ANCHOR_END: t4b_check


# %% Task 5 — point-in-time join -----------------------------------------------------------
# ANCHOR: t5_data
orders = (spark.createDataFrame([
    (101, 1, "2026-10-03", 50.0),
    (102, 1, "2026-10-07", 30.0),
    (103, 1, "2026-10-11", 20.0),
    (104, 2, "2026-10-03", 10.0),
    (105, 2, "2026-10-06", 15.0),
], "order_id INT, customer_id BIGINT, order_date STRING, amount DOUBLE")
    .withColumn("order_date", sf.to_date("order_date")))

dim2 = spark.read.format("delta").load(scd2_path)
# ANCHOR_END: t5_data

# ANCHOR: t5_solution
o, d = orders.alias("o"), dim2.alias("d")
as_of = (
    o.join(
        d,
        (sf.col("o.customer_id") == sf.col("d.customer_id"))
        & (sf.col("o.order_date") >= sf.col("d.valid_from"))
        & (sf.col("o.order_date") < sf.col("d.valid_to")),       # valid_to is EXCLUSIVE
        "left",
    )
    .select("o.order_id", "o.order_date", "o.amount", "d.city", "d.tier")
)
as_of.orderBy("order_id").show()

revenue_then = as_of.groupBy("city").agg(sf.sum("amount").alias("revenue_at_order_time"))
revenue_now = (
    orders.join(dim2.filter("is_current"), "customer_id")
    .groupBy("city").agg(sf.sum("amount").alias("revenue_by_current_city"))
)
revenue_then.join(revenue_now, "city", "full").orderBy("city").show()
# ANCHOR_END: t5_solution

# ANCHOR: t5_check
result = {r["order_id"]: (r["city"], r["tier"]) for r in as_of.collect()}
assert result == {
    101: ("Kyiv", "gold"), 102: ("Dnipro", "gold"), 103: ("Kyiv", "gold"),
    104: ("Lviv", "silver"), 105: ("Lviv", "gold"),
}, result
assert revenue_now.filter("city = 'Dnipro'").count() == 0      # Dnipro's revenue "disappears"
print("✅ task 5")
# ANCHOR_END: t5_check


# %% Task 6 — SCD Type 3 -----------------------------------------------------------------
# ANCHOR: t6_solution
def scd3_merge(updates, path, effective_date):
    if not DeltaTable.isDeltaTable(spark, path):
        (updates
            .withColumn("previous_city", sf.lit(None).cast("string"))
            .withColumn("city_changed_on", sf.lit(None).cast("date"))
            .write.format("delta").save(path))
        return
    src = updates.withColumn("eff", sf.lit(effective_date).cast("date"))
    (DeltaTable.forPath(spark, path).alias("t")
        .merge(src.alias("s"), "t.customer_id = s.customer_id")
        # first matching clause wins → the "city changed" clause must come first
        .whenMatchedUpdate(
            condition="NOT (t.city <=> s.city)",                  # null-safe comparison
            set={"previous_city": "t.city", "city": "s.city", "city_changed_on": "s.eff",
                 "name": "s.name", "tier": "s.tier"})
        .whenMatchedUpdate(
            condition="NOT (t.name <=> s.name) OR NOT (t.tier <=> s.tier)",
            set={"name": "s.name", "tier": "s.tier"})             # previous_city untouched
        .whenNotMatchedInsert(values={
            "customer_id": "s.customer_id", "name": "s.name", "city": "s.city", "tier": "s.tier",
            "previous_city": "cast(null as string)", "city_changed_on": "cast(null as date)"})
        .execute())


for df, day in [(day1, D1), (day2, D2), (day3, D3)]:
    scd3_merge(df, scd3_path, day)
# ANCHOR_END: t6_solution

# ANCHOR: t6_check
dim3 = spark.read.format("delta").load(scd3_path).orderBy("customer_id")
dim3.show()
alice = dim3.filter("customer_id = 1").first()
assert (alice["city"], alice["previous_city"]) == ("Kyiv", "Dnipro")
bob = dim3.filter("customer_id = 2").first()
assert (bob["tier"], bob["previous_city"]) == ("gold", None)
print("✅ task 6")
# ANCHOR_END: t6_check


# %% Task 7 — SCD Type 4: Type-1 table + its change feed as history ----------------------
# ANCHOR: t7_solution
# Every NEW table created while this conf is set gets delta.enableChangeDataFeed = true.
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
for df, day in [(day1, D1), (day2, D2), (day3, D3)]:
    scd1_merge(df.withColumn("updated_on", sf.lit(day).cast("date")), scd4_path)
spark.conf.unset("spark.databricks.delta.properties.defaults.enableChangeDataFeed")

history = (
    spark.read.format("delta")
    .option("readChangeFeed", "true").option("startingVersion", 0)
    .load(scd4_path)
    .filter("_change_type IN ('insert', 'update_postimage')")     # every version a row ever had
    .select("customer_id", "name", "city", "tier", "updated_on",
            sf.col("_commit_version").alias("table_version"))
)
history.write.format("delta").mode("overwrite").save(scd4_hist_path)
# ANCHOR_END: t7_solution

# ANCHOR: t7_check
current4 = spark.read.format("delta").load(scd4_path)
hist4 = spark.read.format("delta").load(scd4_hist_path).orderBy("customer_id", "table_version")
hist4.show()
assert current4.count() == 5                                     # current table: 1 row per customer
assert [r["city"] for r in hist4.filter("customer_id = 1").collect()] == ["Kyiv", "Dnipro", "Kyiv"]
assert hist4.filter("customer_id = 3").count() == 1             # Carol never changed
print("✅ task 7")
# ANCHOR_END: t7_check


# %% Task 8 — SCD Type 6 as a view over SCD2 ----------------------------------------------
# ANCHOR: t8_solution
dim2 = spark.read.format("delta").load(scd2_path)
current_city = dim2.filter("is_current").select("customer_id", sf.col("city").alias("current_city"))
dim6 = dim2.join(current_city, "customer_id")       # historical city AND current city on every row

report = (
    orders.alias("o")
    .join(dim6.alias("d"),
          (sf.col("o.customer_id") == sf.col("d.customer_id"))
          & (sf.col("o.order_date") >= sf.col("d.valid_from"))
          & (sf.col("o.order_date") < sf.col("d.valid_to")))
    .groupBy("d.city", "d.current_city")
    .agg(sf.sum("o.amount").alias("revenue"))
    .orderBy("city", "current_city")
)
report.show()
# ANCHOR_END: t8_solution

# ANCHOR: t8_check
assert {r["current_city"] for r in dim6.filter("customer_id = 1").collect()} == {"Kyiv"}
assert report.filter("city = 'Dnipro' AND current_city = 'Kyiv'").first()["revenue"] == 30.0
print("✅ task 8")
# ANCHOR_END: t8_check


# %% Task 9 (bonus) — full snapshots: soft-delete customers that disappeared ---------------
# ANCHOR: t9_solution
def scd2_merge_snapshot(snapshot, path, effective_date):
    upd = add_hash(snapshot).withColumn("eff", sf.lit(effective_date).cast("date"))
    target = DeltaTable.forPath(spark, path)
    current = target.toDF().filter("is_current")
    changed = (
        upd.alias("u")
        .join(current.alias("c"), sf.col("u.customer_id") == sf.col("c.customer_id"))
        .filter(sf.col("u.row_hash") != sf.col("c.row_hash"))
        .select(sf.lit(None).cast("bigint").alias("merge_key"), *[sf.col(f"u.{c}") for c in upd.columns])
    )
    staged = upd.select(sf.col("customer_id").alias("merge_key"), *upd.columns).unionByName(changed)

    (target.alias("t")
        .merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={"is_current": "false", "valid_to": "s.eff"})
        .whenNotMatchedInsert(values={
            "customer_id": "s.customer_id", "name": "s.name", "city": "s.city", "tier": "s.tier",
            "row_hash": "s.row_hash", "valid_from": "s.eff",
            "valid_to": f"cast('{END_OF_TIME}' as date)", "is_current": "true"})
        # a CURRENT target row that no snapshot row matched = the customer disappeared
        .whenNotMatchedBySourceUpdate(
            condition="t.is_current = true",       # without it, every HISTORICAL row would be rewritten too
            set={"is_current": "false", "valid_to": f"cast('{effective_date}' as date)"})
        .execute())


current_now = (spark.read.format("delta").load(scd2_path)
               .filter("is_current AND customer_id <> 3")
               .select("customer_id", "name", "city", "tier"))
scd2_merge_snapshot(current_now, scd2_path, "2026-10-15")
# ANCHOR_END: t9_solution

# ANCHOR: t9_check
carol = spark.read.format("delta").load(scd2_path).filter("customer_id = 3").collect()
assert len(carol) == 1 and carol[0]["is_current"] is False
assert spark.read.format("delta").load(scd2_path).filter("is_current").count() == 4
print("✅ task 9")
# ANCHOR_END: t9_check


# %% Task 10 — CDC stream → SCD2 (Chapter 7 meets Chapter 8) --------------------------------
# ANCHOR: t10_setup
from lab07_data import batch1, batch2, batch3, cdc_schema, drop_batch  # noqa: E402

stream_landing = f"{base}/cdc_landing"
stream_dim_path = f"{base}/dim_customer_stream"
stream_chk = f"{base}/_checkpoints/dim_customer_stream"

(DeltaTable.createIfNotExists(spark)
    .location(stream_dim_path)
    .addColumn("id", "BIGINT")
    .addColumn("name", "STRING")
    .addColumn("city", "STRING")
    .addColumn("row_hash", "STRING")
    .addColumn("valid_from", "TIMESTAMP")
    .addColumn("valid_to", "TIMESTAMP")
    .addColumn("is_current", "BOOLEAN")
    .execute())
# ANCHOR_END: t10_setup


# ANCHOR: t10_solution
def scd2_from_cdc(batch_df, batch_id):
    session = batch_df.sparkSession
    w = Window.partitionBy("id").orderBy(sf.col("ts").desc())
    ev = add_hash(batch_df.withColumn("_rn", sf.row_number().over(w)).filter("_rn = 1").drop("_rn"),
                  ["name", "city"])
    target = DeltaTable.forPath(session, stream_dim_path)
    current = target.toDF().filter("is_current")

    # events that must become a NEW version: not a delete, and either a brand-new key
    # or a real change that is newer than the current version
    new_versions = (
        ev.alias("e")
        .join(current.alias("c"), sf.col("e.id") == sf.col("c.id"), "left")
        .filter(
            (sf.col("e.op") != "D")
            & (sf.col("c.id").isNull()
               | ((sf.col("e.row_hash") != sf.col("c.row_hash")) & (sf.col("e.ts") > sf.col("c.valid_from"))))
        )
        .select(sf.lit(None).cast("bigint").alias("merge_key"), "e.*")
    )
    staged = ev.select(sf.col("id").alias("merge_key"), *ev.columns).unionByName(new_versions)

    (target.alias("t")
        .merge(staged.alias("s"), "t.id = s.merge_key AND t.is_current = true")
        # expire the current version on a newer delete or a newer real change
        .whenMatchedUpdate(
            condition="s.ts > t.valid_from AND (s.op = 'D' OR t.row_hash <> s.row_hash)",
            set={"is_current": "false", "valid_to": "s.ts"})
        # only the NULL-keyed copies insert: keyed copies of new ids must not insert twice
        .whenNotMatchedInsert(
            condition="s.merge_key IS NULL",
            values={"id": "s.id", "name": "s.name", "city": "s.city", "row_hash": "s.row_hash",
                    "valid_from": "s.ts", "valid_to": f"cast('{END_OF_TIME}' as timestamp)",
                    "is_current": "true"})
        .execute())


def run_stream_scd2():
    q = (spark.readStream.format("json").schema(cdc_schema).load(stream_landing)
         .writeStream.foreachBatch(scd2_from_cdc)
         .option("checkpointLocation", stream_chk)
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()


for name, events in [("batch1", batch1), ("batch2", batch2), ("batch3", batch3)]:
    drop_batch(name, events, folder=stream_landing)
    run_stream_scd2()
# ANCHOR_END: t10_solution

# ANCHOR: t10_check
dim_s = spark.read.format("delta").load(stream_dim_path).orderBy("id", "valid_from")
dim_s.drop("row_hash").show(truncate=False)
assert dim_s.count() == 6
assert {r["id"]: r["city"] for r in dim_s.filter("is_current").collect()} == {1: "Dnipro", 4: "Kyiv", 5: "Kyiv"}
assert dim_s.filter("id IN (2, 3) AND is_current").count() == 0     # Bob and Carol: closed, not erased
print("✅ task 10")
# ANCHOR_END: t10_check

# %%
spark.stop()
