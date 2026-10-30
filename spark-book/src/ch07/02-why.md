# Why incremental ingestion and CDC?

<span class="tag">10 min</span>

## The problem: full reloads don't scale

The simplest way to get data out of an operational system is a **full extract**: every night, dump
the whole `customers` table to a file and overwrite your copy. It works until:

- the table has 400 million rows and only 50 000 changed today, but you move and rewrite all 400 million,
- the business wants the data every 5 minutes instead of every 24 hours,
- someone asks *"what did this customer look like last Tuesday?"* The overwrite threw that away,
- the source team says the nightly dump is hurting their production database.

The answer is to move only **what changed**, as soon as it changed. That idea has two halves,
and they are the two halves of this chapter:

| Half | Question it answers | Tool you'll use |
|---|---|---|
| **Incremental ingestion** | *Which files have I not processed yet?* | Structured Streaming file source + checkpoint (≈ Auto Loader) |
| **Change Data Capture (CDC)** | *Given a stream of changes, what is the current state?* | `foreachBatch` + Delta `MERGE` |

## What CDC data looks like

With CDC you don't receive the table. You receive the stream of **events that changed it**:

```text
op  id  name   city     ts
I   1   Alice  Kyiv     2026-10-01 10:00     ← insert
I   2   Bob    Lviv     2026-10-01 10:00
U   2   Bob    Kharkiv  2026-10-01 12:00     ← update: full new row image
D   3   —      —        2026-10-02 11:00     ← delete: only the key matters
```

Your job is to fold that stream into a table that holds the **current state** of every entity:
one row per `id`, with deleted ones gone. Three things make this harder than it looks, and each one
gets its own *Try it*:

1. **Several events for one key arrive together.** Bob was inserted and updated in the same file.
2. **Events arrive late or out of order.** A retry delivers yesterday's update after today's.
3. **The source schema changes.** One day every event has an `email` field.

## The medallion layout you'll build

| Layer | Holds | Written by | Why it exists |
|---|---|---|---|
| **landing/** | files exactly as the source sent them | the source system | the hand-off point; you don't own it |
| **bronze** | every event ever received, plus audit columns, append-only | a streaming file reader | replay: if silver logic was wrong, rebuild silver from bronze |
| **silver** | current state, one row per key | streaming `foreachBatch` + `MERGE` | what analysts and downstream jobs query |
| **change feed** | the row-level changes silver went through | Delta itself (CDF) | lets the *next* table consume silver incrementally too |

<div class="realworld">

### Where you'll meet this at work

- **Database replication with Debezium / AWS DMS / Fivetran.** A tool reads the Postgres WAL or the
  MySQL binlog and writes `op`-tagged JSON/Parquet files to S3 every few minutes. That is *exactly*
  this chapter's landing folder. DMS even names the column `Op` with values `I`/`U`/`D`.
- **SaaS exports.** Salesforce, HubSpot or Zendesk "changed since" APIs land hourly files of modified
  records. They give you no deletes, so you learn to treat a missing record differently from a deleted one.
- **IoT & app telemetry.** Devices push small JSON files continuously. You can't reread the bucket
  each time, so the checkpoint is what makes ingestion cheap.
- **Partner file drops.** A bank or a logistics partner drops CSVs onto SFTP or blob storage at
  random times. *Exactly-once* matters here: a payment ingested twice is a real incident.
- **Keeping a search index or cache in sync.** The silver table's change feed feeds Elasticsearch
  or a feature store with only the rows that changed.

</div>

<div class="pause">

**Pause and predict** <span class="tag">1 min</span>

Before you write any code, decide what you think the answers are. You'll check them in the next three sections.

1. If you reprocess a file twice, does bronze get duplicates? Does silver?
2. Bob's insert and update are in the *same file*. If you MERGE both rows, what goes wrong?
3. Where would you put the logic that ignores an update older than what you already have?

</div>
