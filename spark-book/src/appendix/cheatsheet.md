# Cheat sheet

## Incremental file ingestion (open-source Spark)

```python
(spark.readStream.format("json").schema(schema).load(landing)
    .withColumn("_ingested_at", sf.current_timestamp())
    .withColumn("_source_file", sf.col("_metadata.file_name"))
    .writeStream.format("delta")
    .option("checkpointLocation", f"{chk}/bronze")     # one per query, never shared, never deleted
    .option("mergeSchema", "true")
    .trigger(availableNow=True)                        # or processingTime="1 minute"
    .start(bronze_path))
```

## The same with Auto Loader (Databricks)

```python
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{chk}/schema")
    .option("cloudFiles.schemaHints", "id BIGINT, ts TIMESTAMP")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")   # rescue | failOnNewColumns | none
    .load(landing) ...)
```

## CDC MERGE inside foreachBatch

```python
def apply_cdc(batch_df, batch_id):
    w = Window.partitionBy("id").orderBy(sf.col("ts").desc())
    latest = batch_df.withColumn("_rn", sf.row_number().over(w)).filter("_rn = 1").drop("_rn")   # rule 1
    (DeltaTable.forPath(batch_df.sparkSession, silver).alias("t")
        .merge(latest.alias("s"), "t.id = s.id")
        .whenMatchedDelete("s.op = 'D' AND s.ts > t.ts")                                          # rules 2+3
        .whenMatchedUpdate("s.op <> 'D' AND s.ts > t.ts", set={...})
        .whenNotMatchedInsert("s.op <> 'D'", values={...})
        .execute())

bronze_stream.writeStream.foreachBatch(apply_cdc).option("checkpointLocation", ...).start()
```

## Change Data Feed

```python
# enable on create:  DeltaTable.create(spark)...property("delta.enableChangeDataFeed", "true")
# or for every new table:  spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 0).load(path)
# _change_type: insert | update_preimage | update_postimage | delete ; _commit_version ; _commit_timestamp
```

## SCD Type 1

```python
.merge(src.alias("s"), "t.key = s.key")
.whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash")
.whenNotMatchedInsertAll()
```

## SCD Type 2: the staged union

```python
changed = (upd.alias("u").join(current.alias("c"), "key-condition")
              .filter("u.row_hash <> c.row_hash")
              .select(sf.lit(None).cast(key_type).alias("merge_key"), "u.*"))
staged = upd.select(sf.col("key").alias("merge_key"), "*").unionByName(changed)

(target.alias("t").merge(staged.alias("s"), "t.key = s.merge_key AND t.is_current = true")
    .whenMatchedUpdate("t.row_hash <> s.row_hash", set={"is_current": "false", "valid_to": "s.eff"})
    .whenNotMatchedInsert(values={..., "valid_from": "s.eff", "valid_to": "cast('9999-12-31' as date)",
                                  "is_current": "true"})
    # full snapshots only:
    .whenNotMatchedBySourceUpdate("t.is_current = true", set={"is_current": "false", "valid_to": ...})
    .execute())
```

## Point-in-time join

```python
facts.join(dim, (f.key == d.key) & (f.date >= d.valid_from) & (f.date < d.valid_to))
```

## Checks for every SCD2 table

| Check | Query idea |
|---|---|
| one current row per key | `groupBy(key).agg(sum(is_current))` ≠ 1 → fail |
| contiguous versions | `lead(valid_from)` over key ordered by `valid_from` ≠ `valid_to` → fail |
| no empty intervals | `valid_to <= valid_from` → fail |
| no fan-out in reports | `count()` of facts before and after the dimension join must match |
