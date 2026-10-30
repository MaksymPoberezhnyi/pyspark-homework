# Incremental ingestion & checkpoints

<span class="tag">15 min</span> <span class="tag">workbook: labs/ch07_lab.py</span>

## How a streaming file source works

`spark.readStream.format("json").load(folder)` turns a folder into an **unbounded table**: each new
file is new rows. Each micro-batch, Spark:

1. lists the folder,
2. subtracts the files it has already processed, as recorded in the **checkpoint**,
3. reads the new files as a normal DataFrame and hands them to your sink,
4. commits the batch to the checkpoint, so those files will never be offered again.

```text
_checkpoints/bronze/
├── metadata          ← query id (a UUID)
├── sources/0/        ← file log: WHICH files each batch read      ← the "Auto Loader memory"
├── offsets/          ← what each batch was going to read (write-ahead)
└── commits/          ← which batches finished
```

Two rules that surprise everyone the first time:

- **Streaming file sources need a schema up front** (`.schema(...)`). Spark won't infer it on every
  micro-batch, because the schema could silently change between batches. Auto Loader solves this with
  inference plus a *stored* schema, as you'll see in section 7.7.
- **The trigger decides when, not what.** `trigger(availableNow=True)` means "process everything
  that's there, then stop". It turns a stream into an incremental batch job you can schedule hourly,
  and it's the most common way Auto Loader runs in production. `processingTime="1 minute"` keeps
  the query running and polls every minute.

## Lab setup

Run the setup cell of `labs/ch07_lab.py`. It builds a Spark session and resets the lab folders. The
"source system" is a function, `drop_batch()`, that writes a JSON-lines file into `landing/`:

<details>
<summary>What's in <code>lab07_data.py</code> (paths, the fake source system, 4 batches of events)</summary>

```python
{{#include ../../labs/lab07_data.py:all}}
```

</details>

```python
{{#include ../../labs/ch07_lab.py:setup}}
```

<div class="tryit">

## Try it 1: a "local Auto Loader" into bronze <span class="tag">7 min</span>

Write `run_bronze(schema)`. It starts a streaming query that:

1. reads JSON from `landing` with `spark.readStream` and `.schema(schema)`,
2. adds two **audit columns**: `_ingested_at` = `sf.current_timestamp()` and
   `_source_file` = `sf.col("_metadata.file_name")`, a hidden column every file source has,
3. writes Delta to `bronze_path` in `append` mode with `checkpointLocation = f"{chk}/bronze"`,
4. sets `.option("mergeSchema", "true")` on the writer (Try it 5 needs it),
5. uses `.trigger(availableNow=True)`, calls `q.awaitTermination()` and returns `q`.

```python
{{#include ../../labs/ch07_lab.py:t1_starter}}
```

<details>
<summary>Hint</summary>

`readStream ... .load(landing)` gives a streaming DataFrame. Add the columns with `withColumn`, then
`.writeStream.format("delta").outputMode("append").option(...).trigger(...).start(bronze_path)`.
`start()` returns a `StreamingQuery` immediately, so block with `awaitTermination()`.

</details>

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/ch07_solutions.py:t1_solution}}
```

</details>
</div>

Look at the `_source_file` column in the output. Audit columns cost nothing at ingest, and they're
how you answer *"which file did this bad row come from?"* at 2 a.m.

<div class="tryit">

## Try it 2a: what the checkpoint remembers <span class="tag">3 min</span>

Drop `batch2` and call `run_bronze(cdc_schema)` again. Print `q.lastProgress["numInputRows"]`.
How many rows were read *in this run*?

```python
{{#include ../../labs/ch07_lab.py:t2a_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/ch07_solutions.py:t2a_solution}}
```

Three rows: only `batch2.json`. `batch1.json` is listed in `sources/0/` of the checkpoint, so it
isn't offered again.

</details>
</div>

<div class="tryit">

## Try it 2b: break the checkpoint on purpose <span class="tag">5 min</span>

Run the experiment against a **separate** table so bronze stays intact. Write `run_experiment()`.
It streams `landing → exp_path` with checkpoint `exp_chk` and returns the row count of `exp_path`.
Then run it twice, delete the checkpoint folder with `shutil.rmtree(exp_chk)`, and run it once more.

```python
{{#include ../../labs/ch07_lab.py:t2b_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/ch07_solutions.py:t2b_solution}}
```

</details>

**Experiment:** reset the experiment with `shutil.rmtree(exp_path)` and `shutil.rmtree(exp_chk)`, then
run `run_experiment(max_files=1)`. Inspect the query's `recentProgress`. You'll need to return `q` to
get at it. How many micro-batches did `availableNow` run? This is how you keep the *first* run over a
backlog of 100 000 files from blowing up the driver: Auto Loader's equivalent is
`cloudFiles.maxFilesPerTrigger`.

</div>

<div class="pause">

**Pause: explain it out loud** <span class="tag">2 min</span>

- Why did the third run produce **duplicates** (14 rows)?
- In Try it 3 you'll write into silver with a `MERGE` on `id`. Would the duplicates from a lost
  checkpoint hurt silver? Why not?

<details>
<summary>Answer</summary>

The checkpoint is the only record of which files were read. A new checkpoint means a new query id, so
the Delta sink can't tell these are the same batches, and every file is appended again.
**An append sink is only exactly-once while its checkpoint survives.**

A `MERGE` on the key is **idempotent**: merging the same event twice leaves the same final row. That's
why the idiom "append to bronze, MERGE into silver" is safe to replay. Even if bronze had duplicates,
silver would not. It is also why the guard you add in Try it 4 matters: idempotent MERGE needs a rule
for "this event is not newer than what I have".

</details>
</div>
