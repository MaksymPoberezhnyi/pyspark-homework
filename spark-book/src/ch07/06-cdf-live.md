# Change Data Feed & running it live

<span class="tag">10 min</span>

## CDC *out*: the Change Data Feed

Silver is the current state, and other tables want to *consume* it incrementally: a gold aggregate, a
search index, another team's pipeline. Rescanning all of silver every time is the same mistake as the
nightly full reload. Delta's **Change Data Feed** (CDF) records the row-level effect of every commit
when the table has `delta.enableChangeDataFeed = true`:

| `_change_type` | Meaning |
|---|---|
| `insert` | a row was added |
| `update_preimage` | the row *before* an update |
| `update_postimage` | the row *after* an update |
| `delete` | a row was removed |

Each change row also carries `_commit_version` and `_commit_timestamp`. Read it in batch, or as a
stream so each downstream run picks up only new changes:

```python
spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 0).load(path)
spark.readStream.format("delta").option("readChangeFeed", "true").load(path)
```

So silver is a CDC *consumer* (of bronze) and a CDC *producer* (for everything after it).

<div class="tryit">

## Try it 6: read silver's change feed <span class="tag">5 min</span>

Read silver's change feed from version 0 and answer:

- Which ids were **deleted**? Only Bob. Why is Carol not there? (You answered this in the pause after Try it 3.)
- What's Dan's latest `update_postimage`?

```python
{{#include ../../labs/ch07_lab.py:t6_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/ch07_solutions.py:t6_solution}}
```

</details>

**Experiment:** the late Alice event in batch 3 did *not* produce an update in the change feed.
Where would it show up if you had used `apply_cdc_v1`? This is also why the hash or "newer than"
conditions matter downstream: every no-op update you skip is one fewer fake change for every consumer.

</div>

## From "run once" to "always on"

So far every query used `availableNow`: catch up, then stop. Switch the trigger to
`processingTime="5 seconds"` and the same code becomes a long-running service. Nothing else changes:
same checkpoints, same functions. That's the main promise of Structured Streaming.

Useful handles on a running query:

| Handle | Gives you |
|---|---|
| `q.status` | what it's doing right now (`"Waiting for data to arrive"`, `"Processing new data"`) |
| `q.lastProgress` / `q.recentProgress` | per-batch metrics: `numInputRows`, `batchId`, durations |
| `spark.streams.active` | every query running in this session |
| `q.stop()` | stop gracefully. The checkpoint stays valid |
| Spark UI → *Structured Streaming* tab | input rate vs processing rate over time ([localhost:4040](http://localhost:4040)) |

<div class="tryit">

## Try it 7: watch the pipeline react <span class="tag">5 min</span>

Start bronze **and** silver as long-running queries (`processingTime="5 seconds"`, the **same**
checkpoints as before, and no `awaitTermination`). Drop a new file that moves Eve to Odesa, and poll
silver until she has moved. Then look at `recentProgress` and stop both queries.

```python
{{#include ../../labs/ch07_lab.py:t7_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/ch07_solutions.py:t7_solution}}
```

</details>

While it runs, open [localhost:4040](http://localhost:4040) → *Structured Streaming*. Then drop another
batch of your own, for example a delete for Dan, and watch silver react.

</div>
