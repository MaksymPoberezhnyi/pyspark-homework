"""Smoke test: run `python labs/check_env.py` (or inside the lab container).

Creates a Delta table, MERGEs into it, reads its Change Data Feed and runs a
one-shot streaming query. If you see "Environment OK" you're ready for the book.
"""
import os
import shutil

from common import DATA_DIR, get_spark

spark = get_spark("check-env")

import pyspark.sql.functions as sf  # noqa: E402
from delta.tables import DeltaTable  # noqa: E402

path = os.path.join(DATA_DIR, "_check_env", "t")
chk = os.path.join(DATA_DIR, "_check_env", "chk")
shutil.rmtree(os.path.dirname(path), ignore_errors=True)

(DeltaTable.create(spark)
    .location(path)
    .addColumn("id", "BIGINT")
    .addColumn("v", "STRING")
    .property("delta.enableChangeDataFeed", "true")
    .execute())

spark.range(3).withColumn("v", sf.lit("a")).write.format("delta").mode("append").save(path)

src = spark.createDataFrame([(1, "b"), (5, "z")], "id BIGINT, v STRING")
(DeltaTable.forPath(spark, path).alias("t")
    .merge(src.alias("s"), "t.id = s.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

rows = {r.id: r.v for r in spark.read.format("delta").load(path).collect()}
assert rows == {0: "a", 1: "b", 2: "a", 5: "z"}, rows

cdf = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 0).load(path)
assert cdf.filter("_change_type = 'update_postimage'").count() == 1

q = (spark.readStream.format("delta").load(path)
     .writeStream.format("delta")
     .option("checkpointLocation", chk)
     .trigger(availableNow=True)
     .start(path + "_copy"))
q.awaitTermination()
assert spark.read.format("delta").load(path + "_copy").count() == 4

print("\nEnvironment OK ✅  (Spark", spark.version, ")")
spark.stop()
