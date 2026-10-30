# Chapter 7 — Auto Loader & Change Data Capture

<span class="tag">≈ 90 min</span> <span class="tag">builds on: DataFrames, Delta basics, MERGE</span>

By the end of this chapter you will have built, on your own machine, the pipeline that sits at
the front of most lakehouses:

```text
 source system ──files──▶ landing/ ──stream──▶ BRONZE ──stream + MERGE──▶ SILVER ──change feed──▶ downstream
                          (JSON)              raw events,                current state,
                                              append-only                1 row per key
```

| Section | What you do | Time |
|---|---|---|
| [Pre-check](./01-precheck.md) | 3 questions: what do you already know? | 5 min |
| [Why incremental ingestion and CDC?](./02-why.md) | the problem, the vocabulary, where you meet it at work | 10 min |
| [Incremental ingestion & checkpoints](./03-ingest.md) | Try it 1–2: a "local Auto Loader", then break its checkpoint | 15 min |
| [Applying changes with foreachBatch + MERGE](./04-cdc-merge.md) | Try it 3–4: dedup, deletes, late events | 20 min |
| [Schema drift](./05-schema-drift.md) | Try it 5: a new column appears in the source | 15 min |
| [Change Data Feed & running it live](./06-cdf-live.md) | Try it 6–7: CDC *out* of silver, a continuously running pipeline | 10 min |
| [The real Auto Loader](./07-autoloader.md) | what Databricks adds on top of what you built | 5 min |
| [Capstone: live Wikipedia edits](./08-capstone.md) | the same concepts on a real, live event stream | 25 min |
| [Review](./09-review.md) | review questions + a second quiz | 5 min |

Your workbook for this chapter is **`labs/ch07_lab.py`**. Open it next to the book and run the cells as
you go. The reference solutions are in `solutions/ch07_solutions.py`, and the same solutions are
hidden under every *Try it* box. Peek only after you've tried.

<div class="warn">

**Auto Loader is a Databricks feature.** `format("cloudFiles")` does not exist in open-source Spark.
You'll build the same mechanics with open-source Structured Streaming (a file source plus a checkpoint),
which is what Auto Loader is built on. The [last section](./07-autoloader.md) maps every piece you built
to its Auto Loader option, so the code transfers directly to work.

</div>
