# Applying changes with foreachBatch + MERGE

<span class="tag">20 min</span>

## Why `foreachBatch`?

Streaming sinks know three output modes, `append`, `update` and `complete`, and none of them can
*delete a row* or *update one in place* in a Delta table. `foreachBatch` is the escape hatch. Spark
hands you every micro-batch as an ordinary DataFrame plus its `batch_id`, and inside the function you
can run any batch code you like, including a Delta `MERGE`:

```python
def apply_changes(batch_df, batch_id):
    ...   # normal batch PySpark: joins, windows, MERGE, writes to several tables

stream_df.writeStream.foreachBatch(apply_changes).option("checkpointLocation", ...).start()
```

The micro-batch is processed **at least once**. If the job dies after the MERGE but before the
checkpoint commit, the same batch is replayed on restart. That's harmless when the function is
**idempotent**, and a key-based MERGE with a "newer than" guard is.

## The three rules of a CDC MERGE

| Rule | Why | How |
|---|---|---|
| **1. One source row per key** | MERGE fails if 2 source rows match 1 target row, and inserts duplicates if they match none | `row_number()` over `Window.partitionBy(key).orderBy(ts.desc())`, keep `== 1` |
| **2. One clause per operation** | `D` → delete, other ops → update or insert | `whenMatchedDelete`, `whenMatchedUpdate`, `whenNotMatchedInsert`, each with a `condition` on `op` |
| **3. Never go back in time** | late or replayed events must not overwrite newer state | add `s.ts > t.ts` to the matched clauses |

Rule 1 handles disorder *inside* one micro-batch. Rule 3 handles disorder *between* micro-batches.
You need both, and Try it 4 shows why.

The PySpark MERGE API you need:

```python
(DeltaTable.forPath(spark, path).alias("t")
    .merge(source.alias("s"), "t.id = s.id")
    .whenMatchedDelete(condition="...")
    .whenMatchedUpdate(condition="...", set={"col": "s.col", ...})
    .whenNotMatchedInsert(condition="...", values={"col": "s.col", ...})
    .execute())
```

Clauses of the same kind are checked **in order**, and the first one whose condition holds wins.

<div class="tryit">

## Try it 3: CDC into silver <span class="tag">12 min</span>

The silver table is created for you with `DeltaTable.createIfNotExists(...)`, with
**Change Data Feed** switched on for Try it 6. Implement:

1. `latest_per_id(df)`: keep the newest event per `id`, and drop `_ingested_at` and `_source_file`.
2. `apply_cdc_v1(batch_df, batch_id)`: MERGE `latest_per_id(batch_df)` into silver on `t.id = s.id`:
   matched and `op = 'D'` → delete. Matched and `op <> 'D'` → update `name`, `city`, `ts`.
   Not matched and `op <> 'D'` → insert.
3. `run_silver(fn)`: stream **from the bronze Delta table**
   (`spark.readStream.format("delta").load(bronze_path)`) with `.foreachBatch(fn)`,
   checkpoint `f"{chk}/silver"`, `availableNow`. Await it and return the query.

```python
{{#include ../../labs/ch07_lab.py:t3_starter}}
```

<details>
<summary>Hint: latest_per_id</summary>

```python
w = Window.partitionBy("id").orderBy(sf.col("ts").desc())
df.withColumn("_rn", sf.row_number().over(w)).filter("_rn = 1").drop("_rn", ...)
```

`drop()` ignores names that don't exist, so the same function also works on data without audit columns.

</details>

<details>
<summary>Hint: inside foreachBatch, which SparkSession?</summary>

Use `batch_df.sparkSession`, the session the micro-batch belongs to. In a notebook the global `spark`
works too, but code you move into a job or a library shouldn't depend on a global.

</details>

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/ch07_solutions.py:t3_solution}}
```

</details>
</div>

<div class="pause">

**Pause: why is Carol not in silver, and never was?** <span class="tag">2 min</span>

Bronze has Carol's insert (batch 1) *and* her delete (batch 2). Silver never had a Carol row.

<details>
<summary>Answer</summary>

Bronze already held both batch1 and batch2 when you first ran `run_silver`, so `availableNow` read
both bronze commits in **one** micro-batch. `latest_per_id` kept only Carol's newest event, the
`D`, and a delete with no matching target row does nothing. Bob's `I` and `U` likewise collapsed into
one `U`, which was *inserted* because Bob wasn't in silver yet. That's why the insert clause
accepts any non-delete op and not just `op = 'I'`.

Takeaway: **micro-batch boundaries are not business boundaries.** Your logic must give the same result
whether the events arrive in one batch or in ten.

</details>
</div>

<div class="tryit">

## Try it 4: late and out-of-order events <span class="tag">6 min</span>

`batch3` contains an update for Alice with a timestamp **older** than the one silver already has:
`Lviv` at 2026-10-01 15:00 against `Dnipro` at 2026-10-02 10:00. With `apply_cdc_v1`, Alice would
move back to Lviv. Write `apply_cdc_v2` = v1 plus: update or delete only when `s.ts > t.ts`. Then
ingest `batch3` into bronze and run silver with v2.

You can swap the function between runs. The checkpoint stores *what was read*, not your code.

```python
{{#include ../../labs/ch07_lab.py:t4_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/ch07_solutions.py:t4_solution}}
```

</details>

**Experiment:** what does `s.ts > t.ts` do when an event is replayed with the *same* timestamp?
Would `>=` be better or worse? (Hint: idempotency, and the capstone, whose timestamps only have
whole-second precision.)

</div>

<div class="realworld">

**At work** you rarely trust a wall-clock `ts` alone. Use the source's own ordering when it has one:
Debezium's `source.lsn`, DMS's transaction sequence, a Kafka offset, or a version counter. Clocks on
different source servers drift, and log positions don't.

</div>
