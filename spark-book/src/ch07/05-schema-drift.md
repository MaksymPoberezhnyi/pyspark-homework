# Schema drift

<span class="tag">15 min</span>

Sources change without asking you. A new app release adds `email`, a vendor renames `zip` to
`postal_code`, or a number starts arriving as `"12.5"` in quotes. There are three possible outcomes,
and only one is acceptable:

| What the pipeline does | Consequence |
|---|---|
| **Silently drops the new field** | data loss nobody notices for weeks. The default with a fixed streaming schema! |
| **Crashes and stays down** | loud, but every change needs a human at 2 a.m. |
| **Detects, evolves deliberately, continues** | ✅ what Auto Loader's `addNewColumns` and `_rescued_data` automate |

In this section you'll do the third option by hand, so you know what Auto Loader does for you.

<div class="tryit">

## Try it 5a + 5b: detect the drift, then watch the data vanish <span class="tag">4 min</span>

`batch4` has a new field, `email`.

- **5a:** batch-read `landing/batch4.json` *with* schema inference and compute
  `new_cols` = columns not in `cdc_schema`.
- **5b:** read the same file with the old `cdc_schema` and print `.columns`. `email` is gone, with no
  error and no warning.

```python
{{#include ../../labs/ch07_lab.py:t5ab_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/ch07_solutions.py:t5ab_solution}}
```

</details>
</div>

<div class="tryit">

## Try it 5c: evolve bronze <span class="tag">3 min</span>

Build `cdc_schema_v2` = the old fields plus `StructField("email", StringType())`. Careful:
`StructType.add()` **mutates in place**, so build a new `StructType`. Then run
`run_bronze(cdc_schema_v2)`. Because the writer has `mergeSchema = true`, the bronze table gains the
column, and old rows get `NULL`.

```python
{{#include ../../labs/ch07_lab.py:t5c_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/ch07_solutions.py:t5c_solution}}
```

</details>
</div>

<div class="tryit">

## Try it 5d: evolve silver explicitly <span class="tag">8 min</span>

MERGE won't add columns by itself (unless you switch on auto-merge, discussed below). Write:

1. `evolve_target_schema(batch_df, path)`: find columns in the batch that are missing from the
   target (except `op`) and add them. The pure-PySpark way to say `ALTER TABLE … ADD COLUMNS` is to
   append an **empty** DataFrame that has only those fields, with `mergeSchema = true`. Delta commits a
   schema change and no data files.
2. `apply_cdc_v3`: `latest_per_id`, then `evolve_target_schema`, then build the `set` and `values` dicts
   **dynamically** from the batch's columns, then the same MERGE conditions as v2. Use a
   **fresh** `DeltaTable.forPath(...)` *after* the evolution, because a stale handle still has the old schema.

```python
{{#include ../../labs/ch07_lab.py:t5d_starter}}
```

<details>
<summary>Hint: an empty DataFrame with a given schema</summary>

`spark.createDataFrame([], StructType(missing_fields))`, then
`.write.format("delta").mode("append").option("mergeSchema", "true").save(path)`.

</details>

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/ch07_solutions.py:t5d_solution}}
```

</details>
</div>

<div class="pause">

**Why explicit evolution instead of `spark.databricks.delta.schema.autoMerge.enabled = true`?**

Auto-merge also works: MERGE then adds any new source column to the target. But it adds *every* new
column, silently, including the `tmp_debug_flag` a source developer forgot to remove. The explicit
function gives you one place to **log** the change, **validate** it (an allow-list, naming rules), or
**reject** it and alert someone. On the job you'll see both. Know which one your pipeline uses, and why.

</div>

## What Auto Loader does here

Auto Loader automates exactly what you just did:

- **Inference plus a stored schema.** On the first run it samples files, infers a schema and stores it in
  `cloudFiles.schemaLocation`. Every restart reuses it, so there is no inference per micro-batch.
- **`schemaHints`** pin the types you care about (`"id BIGINT, ts TIMESTAMP"`). That's your
  `cdc_schema`, but only for the columns that matter.
- **`schemaEvolutionMode`**:

| Mode | On a new column | Your hand-made equivalent |
|---|---|---|
| `addNewColumns` (default) | stores the new schema, **fails the stream once**, and the restart picks it up | 5a + 5c, plus a job retry |
| `rescue` | schema never changes; unknown fields go into `_rescued_data` as JSON | keeping the raw line (the capstone does this) |
| `failOnNewColumns` | stops and waits for a human | `assert not new_cols` |
| `none` | ignores new columns | 5b: the silent drop |

- **`_rescued_data`** also catches **type mismatches**, for example `"price": "12,50"` when the schema
  says `DOUBLE`. Instead of a `NULL` you can't explain, you get the original value.
