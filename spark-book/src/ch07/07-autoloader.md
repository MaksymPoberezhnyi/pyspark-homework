# The real Auto Loader

<span class="tag">5 min · read only</span>

On Databricks, Try it 1 becomes the code below. It's still PySpark: only the source format changes.
**It won't run on your local cluster**, because `cloudFiles` ships only with the Databricks runtime.
Read it side by side with your own `run_bronze`.

```python
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{chk}/bronze_schema")   # where inferred schema versions live
    .option("cloudFiles.inferColumnTypes", "true")                 # JSON: infer ints/timestamps, not all strings
    .option("cloudFiles.schemaHints", "id BIGINT, ts TIMESTAMP")   # pin the types you care about
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")     # the default
    .option("cloudFiles.maxFilesPerTrigger", 1000)                 # bound the first, backlog-sized batch
    .load(landing)
    .withColumn("_ingested_at", sf.current_timestamp())
    .withColumn("_source_file", sf.col("_metadata.file_path"))
    .writeStream
    .option("checkpointLocation", f"{chk}/bronze")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("bronze_customers_cdc"))
```

## What you built by hand vs what Auto Loader gives you

| You did it by hand | Auto Loader |
|---|---|
| wrote `cdc_schema` yourself | **schema inference**, stored in `schemaLocation`; `schemaHints` pins types |
| Try it 5a: diffed the schema manually | **schema evolution**: on a new column the stream fails **once**, the schema is saved, and the restart continues (`addNewColumns`) |
| Try it 5b: data silently dropped | **`_rescued_data`**: unknown fields and type mismatches are kept as JSON |
| the checkpoint tracks the file list | same, in a RocksDB store that scales to **millions** of files |
| Spark lists the folder every micro-batch | optional **file notification mode** (`cloudFiles.useNotifications`): S3/ADLS events plus a queue, so no directory listing |
| `drop_batch` renames atomically | same rule: writers must not expose half-written files |

## Even less code: declarative CDC

On Databricks you'd often not hand-write Try it 3–5 at all. **Lakeflow Declarative Pipelines**
(formerly Delta Live Tables) has a declarative CDC API, `create_auto_cdc_flow()` in Python, formerly
`apply_changes()`. You give it a key, a sequence column (Try it 4's `ts`), a delete condition, and
`stored_as_scd_type=1` or `2`. It handles dedup, ordering, deletes and even SCD Type 2, which is
Chapter 8.

Knowing how to build it by hand is what lets you debug it when it misbehaves: *"why did this update
disappear?"* is almost always rule 1 or rule 3 from the MERGE section.

<div class="realworld">

**Try it on real Auto Loader:** the free [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
runs `cloudFiles`. Upload the `landing/` folder from this lab to a Unity Catalog volume, swap the
reader for the one above, and compare: in particular, what happens when you drop `batch4.json` with
`addNewColumns` vs `rescue`.

</div>
