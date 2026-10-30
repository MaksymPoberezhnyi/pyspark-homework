# Pre-check: what do you think you know?

<span class="tag">5 min</span>

Answer from what you know **right now**. Don't look things up: the point is to find the gaps
this chapter should fill. Your answers are saved in this browser. Come back after the chapter and
see whether you would still pick the same ones.

```quiz
{
  "id": "ch07-precheck",
  "title": "Chapter 7 pre-check",
  "intro": "Every option describes something Spark could plausibly do. Exactly one is what it actually does.",
  "questions": [
    {
      "q": "A streaming query reads JSON files from a folder and appends them to a Delta table. It has processed 10 files. You delete its checkpoint directory and start it again with the same code. What happens?",
      "options": [
        "It resumes after the 10th file, because the Delta sink's transaction log records which source files it has already committed.",
        "It reads all 10 files again and appends them a second time, because a new checkpoint means a new query id that the Delta sink has never seen.",
        "It refuses to start, because the target table's transaction log still references the streaming query id from the old checkpoint.",
        "It only reads files whose modification time is newer than the target table's latest commit timestamp, so nothing is duplicated."
      ],
      "answer": 1,
      "explain": "The checkpoint is the only place that remembers which files were read. The Delta sink is idempotent per (query id, batch id), and a fresh checkpoint gets a fresh query id, so every file is read and written again. The 'transaction log remembers the files' option is tempting because Delta does store a transaction id. It just can't recognise the new query as the old one."
    },
    {
      "q": "Inside `foreachBatch` you MERGE each micro-batch into a silver table on `id`. One micro-batch contains two events for `id = 7`: an insert and, a minute later, an update. What happens if you MERGE the batch as-is, without deduplicating?",
      "options": [
        "Delta applies the two source rows in the order they appear in the file, so the final row holds the values of the later update.",
        "The MERGE fails at analysis time whenever the source has duplicate keys, whether or not those keys already exist in the target table.",
        "Delta keeps whichever of the two source rows has the larger value in the merge-condition column and silently discards the other one.",
        "If `id = 7` already exists in silver the MERGE fails because two source rows match one target row; if it doesn't, both rows are inserted as duplicates."
      ],
      "answer": 3,
      "explain": "MERGE has no notion of event order. Two source rows matching the same target row is an error (`DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`). Two unmatched source rows are simply two inserts. That's why every CDC MERGE first keeps one row per key: the newest by event timestamp or sequence."
    },
    {
      "q": "On Databricks, an Auto Loader stream runs with `cloudFiles.schemaEvolutionMode = addNewColumns` (the default). A file arrives with a column the stored schema doesn't have. What happens?",
      "options": [
        "The stream stops with an error after writing the new schema to the schema location; when the job restarts, it continues with the new column.",
        "The stream adds the column to its schema between two micro-batches and keeps running without interruption or restart.",
        "The unknown field is written into `_rescued_data` and the stored schema stays unchanged until someone edits the schema hints.",
        "The new field is dropped silently, because only `failOnNewColumns` and `rescue` modes look at fields outside the stored schema."
      ],
      "answer": 0,
      "explain": "Fail, evolve, restart: Auto Loader updates the schema in `schemaLocation` and then throws `UnknownFieldException`, so the stream restarts with a new plan. That's why production jobs run with retries. The `_rescued_data` option describes `rescue` mode, and the silent drop describes `none`."
    }
  ]
}
```

No score to worry about. Keep your wrong answers in mind: each one maps to a *Try it* in this chapter.
