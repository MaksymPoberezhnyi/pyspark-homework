# %% [markdown]
# # Chapter 8 — Slowly Changing Dimensions: your workbook
#
# Open in JupyterLab (Open With → Notebook) or VS Code. Every task ends with asserts → ✅.
# Re-running the setup cell resets the lab.

# %%
# ANCHOR: setup
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
day1.show(); day2.show(); day3.show()
# ANCHOR_END: setup


# %% Task 1 — SCD Type 1 (overwrite, no history)
# ANCHOR: t1_starter
def scd1_merge(updates, path):
    # TODO: add_hash(updates); first load → just write the table;
    #       otherwise MERGE on customer_id:
    #         matched AND hash differs → whenMatchedUpdateAll(condition=...)
    #         not matched → whenNotMatchedInsertAll()
    ...


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
# ANCHOR_END: t1_starter


# %% Task 2 — SCD Type 2: table design + initial load
# ANCHOR: t2_starter
def scd2_initial_load(df, path, effective_date):
    # TODO: add_hash + valid_from (= effective_date), valid_to (= END_OF_TIME), is_current (= True)
    #       dates via sf.lit(...).cast("date"); overwrite the table at `path`
    ...


scd2_initial_load(day1, scd2_path, D1)
dim2 = spark.read.format("delta").load(scd2_path)
dim2.show()
assert dim2.count() == 3 and dim2.filter("is_current").count() == 3
print("✅ task 2")
# ANCHOR_END: t2_starter


# %% Task 3 — SCD Type 2 MERGE (the staged-union trick)
# ANCHOR: t3_starter
def scd2_merge(updates, path, effective_date):
    # TODO:
    # 1. upd = add_hash(updates) + column eff = effective_date (as date)
    # 2. current = target rows with is_current = true
    # 3. changed = upd ⋈ current on customer_id where hashes differ,
    #    select NULL AS merge_key (sf.lit(None).cast("bigint")) + all upd columns (use aliases u / c)
    # 4. staged = upd with customer_id AS merge_key + all columns, unionByName(changed)
    # 5. MERGE staged into target ON t.customer_id = s.merge_key AND t.is_current = true
    #      whenMatchedUpdate(condition hash differs) → is_current = false, valid_to = s.eff
    #      whenNotMatchedInsert → new current version (valid_from = s.eff, valid_to = END_OF_TIME)
    ...


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
# ANCHOR_END: t3_starter


# %% Task 4a — idempotency
# ANCHOR: t4a_starter
# TODO: run scd2_merge(day3, ...) a second time — no code changes should be needed

assert spark.read.format("delta").load(scd2_path).count() == 8
print("✅ task 4a")
# ANCHOR_END: t4a_starter


# %% Task 4b — duplicate keys in one load
# ANCHOR: t4b_starter
day4_dirty = spark.createDataFrame([
    (5, "Eve", "Odesa", "bronze"),
    (5, "Eve", "Kharkiv", "bronze"),
], "customer_id BIGINT, name STRING, city STRING, tier STRING")

# TODO: try scd2_merge(day4_dirty, scd2_path, "2026-10-12") inside try/except and print the
#       first line of the error; then keep ONE row per customer_id (Window + row_number,
#       ordered by city) and merge again on "2026-10-12"

eve = spark.read.format("delta").load(scd2_path).filter("customer_id = 5").orderBy("valid_from")
eve.drop("row_hash").show()
assert eve.count() == 2
print("✅ task 4b")
# ANCHOR_END: t4b_starter


# %% Task 5 — point-in-time join
# ANCHOR: t5_starter
orders = (spark.createDataFrame([
    (101, 1, "2026-10-03", 50.0),
    (102, 1, "2026-10-07", 30.0),
    (103, 1, "2026-10-11", 20.0),
    (104, 2, "2026-10-03", 10.0),
    (105, 2, "2026-10-06", 15.0),
], "order_id INT, customer_id BIGINT, order_date STRING, amount DOUBLE")
    .withColumn("order_date", sf.to_date("order_date")))

dim2 = spark.read.format("delta").load(scd2_path)

# TODO: as_of = orders joined to the dim2 version valid ON the order date
#       (o.order_date >= d.valid_from AND o.order_date < d.valid_to) → order_id, order_date, amount, city, tier
as_of = ...
as_of.orderBy("order_id").show()

# TODO: revenue_then (by city at order time) vs revenue_now (orders ⋈ is_current rows, by city)
revenue_now = ...

result = {r["order_id"]: (r["city"], r["tier"]) for r in as_of.collect()}
assert result == {
    101: ("Kyiv", "gold"), 102: ("Dnipro", "gold"), 103: ("Kyiv", "gold"),
    104: ("Lviv", "silver"), 105: ("Lviv", "gold"),
}, result
assert revenue_now.filter("city = 'Dnipro'").count() == 0      # Dnipro's revenue "disappears"
print("✅ task 5")
# ANCHOR_END: t5_starter


# %% Task 6 — SCD Type 3 (only the previous value)
# ANCHOR: t6_starter
def scd3_merge(updates, path, effective_date):
    # TODO: first load → write with previous_city = NULL, city_changed_on = NULL (typed!)
    #       MERGE on customer_id with TWO whenMatchedUpdate clauses (first match wins):
    #         1. city changed (null-safe: NOT (t.city <=> s.city)) → previous_city = t.city, city = s.city,
    #            city_changed_on = eff, name, tier
    #         2. only name or tier changed → name, tier (keep previous_city)
    #       not matched → insert with NULL previous_city / city_changed_on
    ...


for df, day in [(day1, D1), (day2, D2), (day3, D3)]:
    scd3_merge(df, scd3_path, day)

dim3 = spark.read.format("delta").load(scd3_path).orderBy("customer_id")
dim3.show()
alice = dim3.filter("customer_id = 1").first()
assert (alice["city"], alice["previous_city"]) == ("Kyiv", "Dnipro")
bob = dim3.filter("customer_id = 2").first()
assert (bob["tier"], bob["previous_city"]) == ("gold", None)
print("✅ task 6")
# ANCHOR_END: t6_starter


# %% Task 7 — SCD Type 4: a Type-1 table + its change feed as history
# ANCHOR: t7_starter
# TODO: 1. spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
#       2. apply day1..3 with scd1_merge(df.withColumn("updated_on", <day as date>), scd4_path)
#       3. unset the conf again
#       4. history = change feed of scd4_path from version 0, only insert + update_postimage,
#          columns customer_id, name, city, tier, updated_on, table_version (= _commit_version)
#       5. write history (overwrite) to scd4_hist_path

current4 = spark.read.format("delta").load(scd4_path)
hist4 = spark.read.format("delta").load(scd4_hist_path).orderBy("customer_id", "table_version")
hist4.show()
assert current4.count() == 5
assert [r["city"] for r in hist4.filter("customer_id = 1").collect()] == ["Kyiv", "Dnipro", "Kyiv"]
assert hist4.filter("customer_id = 3").count() == 1
print("✅ task 7")
# ANCHOR_END: t7_starter


# %% Task 8 — SCD Type 6 as a view over SCD2
# ANCHOR: t8_starter
dim2 = spark.read.format("delta").load(scd2_path)
# TODO: dim6 = dim2 + column current_city (the city of that customer's is_current row) on EVERY row
dim6 = ...
# TODO: report = revenue by (historical city, current_city) using the point-in-time join on dim6
report = ...
report.show()

assert {r["current_city"] for r in dim6.filter("customer_id = 1").collect()} == {"Kyiv"}
assert report.filter("city = 'Dnipro' AND current_city = 'Kyiv'").first()["revenue"] == 30.0
print("✅ task 8")
# ANCHOR_END: t8_starter


# %% Task 9 (bonus) — full snapshots: soft-delete customers that disappeared
# ANCHOR: t9_starter
def scd2_merge_snapshot(snapshot, path, effective_date):
    # TODO: copy scd2_merge and add one clause:
    #   .whenNotMatchedBySourceUpdate(condition="t.is_current = true",
    #                                 set={"is_current": "false", "valid_to": <effective_date as date>})
    ...


current_now = (spark.read.format("delta").load(scd2_path)
               .filter("is_current AND customer_id <> 3")
               .select("customer_id", "name", "city", "tier"))
scd2_merge_snapshot(current_now, scd2_path, "2026-10-15")

carol = spark.read.format("delta").load(scd2_path).filter("customer_id = 3").collect()
assert len(carol) == 1 and carol[0]["is_current"] is False
assert spark.read.format("delta").load(scd2_path).filter("is_current").count() == 4
print("✅ task 9")
# ANCHOR_END: t9_starter


# %% Task 10 — CDC stream → SCD2 (Chapter 7 meets Chapter 8)
# ANCHOR: t10_starter
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


def scd2_from_cdc(batch_df, batch_id):
    # TODO:
    # 1. ev = newest event per id (like latest_per_id) + add_hash(..., ["name", "city"])
    # 2. new_versions = ev LEFT JOIN current rows on id, keep events that are not 'D' and either
    #    have no current row, or differ in hash AND are newer (e.ts > c.valid_from);
    #    select NULL AS merge_key + e.*
    # 3. staged = ev with id AS merge_key, unionByName(new_versions)
    # 4. MERGE ON t.id = s.merge_key AND t.is_current = true:
    #      whenMatchedUpdate(s.ts > t.valid_from AND (s.op = 'D' OR hash differs)) → expire at s.ts
    #      whenNotMatchedInsert(condition "s.merge_key IS NULL") → new version from s.ts, open-ended
    ...


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

dim_s = spark.read.format("delta").load(stream_dim_path).orderBy("id", "valid_from")
dim_s.drop("row_hash").show(truncate=False)
assert dim_s.count() == 6
assert {r["id"]: r["city"] for r in dim_s.filter("is_current").collect()} == {1: "Dnipro", 4: "Kyiv", 5: "Kyiv"}
assert dim_s.filter("id IN (2, 3) AND is_current").count() == 0
print("✅ task 10")
# ANCHOR_END: t10_starter

# %%
spark.stop()
