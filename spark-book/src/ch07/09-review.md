# Review

<span class="tag">5 min</span>

## Review questions

Answer each one out loud before opening it. If you can explain all six without peeking, you have
the chapter.

<details>
<summary>1. What exactly is stored in a streaming checkpoint, and what happens if you delete it?</summary>

The query id, the **offsets** of every batch (for a file source: which files each batch read), and
which batches **committed**. Stateful queries also keep their aggregation or dedup state there. Delete
it and the query starts over as a new query: a file source rereads every file, and an append sink
writes duplicates (Try it 2b). Treat checkpoints like data: back them up, and never share one between
two queries.

</details>

<details>
<summary>2. Why is foreachBatch + MERGE called "effectively exactly-once" when foreachBatch itself is at-least-once?</summary>

A batch can be replayed if the job dies between the MERGE commit and the checkpoint commit. A MERGE on
the key with a `s.ts > t.ts` guard gives the same final state whether a batch is applied once or
twice, so the *effect* is exactly-once. Anything non-idempotent inside `foreachBatch` breaks this:
plain appends, sending emails, or the capstone's `events_seen + n` counter, which double-counts on a
replay. Delta can make writes idempotent with the `txnAppId` and `txnVersion` writer options
(use `batch_id` as the version).

</details>

<details>
<summary>3. Why do we need latest_per_id AND the s.ts > t.ts guard? Don't they do the same thing?</summary>

No. `latest_per_id` orders events **within** a micro-batch: MERGE needs one source row per key.
The guard orders a new event against the state **already in the target**, from earlier batches. Remove
the first and Bob's insert and update break the MERGE. Remove the second and the late Alice event
moves her back to Lviv.

</details>

<details>
<summary>4. availableNow vs processingTime="1 minute" vs continuous: when do you use each?</summary>

- **`availableNow`**: scheduled incremental batch. The job catches up, stops, and costs nothing between
  runs. Most Auto Loader jobs work this way (hourly or every 15 minutes).
- **`processingTime`**: an always-on cluster with latency of seconds to minutes. Use it when freshness
  justifies a cluster that never sleeps.
- **Continuous processing / real-time mode**: millisecond latency with many restrictions. Rarely needed
  for lakehouse ingestion, which is usually happy with seconds to minutes.

</details>

<details>
<summary>5. Bronze keeps every event forever. Why not MERGE straight from landing into silver?</summary>

Bronze is your **replay log**. When the silver logic turns out to be wrong (a missing guard, a wrong
key, a new column you want to backfill), you rebuild silver from bronze. Landing files may be deleted
by the source, moved by lifecycle rules, or too many to list. Bronze is also where you debug:
`_source_file` and `_ingested_at` tell you where a row came from and when.

</details>

<details>
<summary>6. Your upstream team now sends a full snapshot every day instead of CDC events. What changes?</summary>

Deletes aren't events any more. A key that's *missing* from today's snapshot is a delete. A MERGE
with `whenNotMatchedBySourceDelete()` handles that, and you'll use its sibling
`whenNotMatchedBySourceUpdate()` for SCD2 in the next chapter. Also stop treating every row as a
change. Compare a hash of the attributes, or every unchanged row becomes a fake update in the change feed.

</details>

## Post-check

Three new questions, at the same difficulty as the pre-check.

```quiz
{
  "id": "ch07-postcheck",
  "title": "Chapter 7 post-check",
  "questions": [
    {
      "q": "A writer copies large CSV files into a landing folder with a plain `cp`. A streaming file source sometimes ingests only part of a file. What's the right fix?",
      "options": [
        "Set `maxFilesPerTrigger` to 1, so each micro-batch waits for the previous file to finish copying before it lists the folder again.",
        "Switch the trigger from `processingTime` to `availableNow`, which only reads files whose size has stopped changing between two listings.",
        "Have the writer copy to a name starting with `.` or `_`, then rename it; the file source skips such names, so it only sees complete files.",
        "Enable `mergeSchema` on the sink, so the remaining part of the file is appended as soon as the next micro-batch picks it up."
      ],
      "answer": 2,
      "explain": "The file source has no notion of a file that's still being written. Once it has listed a file, that file is done. An atomic rename from a hidden name is the standard contract (`drop_batch` and the collector both do it). On object stores, uploads are atomic per object, which is why the problem mostly appears on local and NFS-style folders."
    },
    {
      "q": "Silver is updated with `foreachBatch` + MERGE. Inside the same function you also append each micro-batch to an `audit_log` Delta table with a plain `.write.mode('append')`. After a crash and restart, what can happen?",
      "options": [
        "Silver ends up correct because the MERGE is idempotent, but `audit_log` may contain the replayed batch twice.",
        "Nothing: the checkpoint guarantees `foreachBatch` runs exactly once per `batch_id`, so both writes happen exactly once.",
        "Silver may contain duplicate rows, because MERGE inserts unmatched rows again when a batch is replayed after a crash.",
        "Both tables are rolled back to the last checkpointed batch, because Delta ties `foreachBatch` writes to the streaming commit."
      ],
      "answer": 0,
      "explain": "`foreachBatch` is at-least-once. The idempotent MERGE absorbs a replay, but a plain append does not. The fix is to make the append idempotent with `.option('txnAppId', <stable id>).option('txnVersion', batch_id)`: Delta skips a (txnAppId, txnVersion) pair it has already committed."
    },
    {
      "q": "A downstream gold job reads silver's Change Data Feed. Silver's MERGE updates every matched row, even when no value changed. What does the gold job see?",
      "options": [
        "Nothing extra: CDF compares pre- and post-images and suppresses updates whose values are identical.",
        "Only the `update_postimage` rows, because CDF writes a pre-image only when at least one column value differs.",
        "A single `merge` change row per commit, which the consumer has to expand into row-level changes itself.",
        "An `update_preimage`/`update_postimage` pair for every matched row, including the ones where nothing actually changed."
      ],
      "answer": 3,
      "explain": "CDF records what the MERGE did, not what changed semantically. That's why real pipelines add `condition='s.ts > t.ts'` or a row-hash comparison to `whenMatchedUpdate`: it saves file rewrites in silver and fake changes for every consumer. Chapter 8 starts exactly there."
    }
  ]
}
```

**Next:** [Chapter 8: Slowly Changing Dimensions](../ch08/README.md). You'll take the MERGE you just
mastered and use it to keep *history* instead of throwing it away.
