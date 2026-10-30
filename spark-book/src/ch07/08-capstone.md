# Capstone: live Wikipedia edits

<span class="tag">25 min</span> <span class="tag">workbook: labs/capstone07_lab.py</span>

Time to apply everything to data you didn't design. Wikimedia publishes **every edit, page creation
and log action on every Wikipedia, as it happens**, on a free public stream:
`https://stream.wikimedia.org/v2/stream/recentchange`. English Wikipedia alone produces several events
per second.

That stream is a CDC feed of the world's largest encyclopedia:

| Wikipedia event | CDC op | Key |
|---|---|---|
| `type = "new"`: a page was created | `I` | page `title` |
| `type = "edit"`: a new revision | `U` | page `title` |
| `type = "log"`, `log_type = log_action = "delete"`: an admin deleted the page | `D` | page `title` |
| moves, protections, uploads, … | ignored in this capstone | |

You'll build: **landing files → bronze (with a home-made schema location and rescued data) → silver
"current state of every article" via MERGE → questions answered from silver and its change feed**,
and you'll handle a schema change while the pipeline is running.

## Step 0: start the "source system" <span class="tag">2 min</span>

The collector (`tools/wiki_collector.py`) reads the stream and writes a new JSON-lines file every
20 seconds, atomically, into the lab's landing folder (`data/wiki/landing/`, or `data-docker/…` on the cluster). Open a **terminal** in the book folder and leave it running:

```bash
# laptop
python tools/wiki_collector.py --out data/wiki/landing --minutes 15 --drift-after-files 6

# local docker cluster: run it inside the lab container
# (writes to data-docker/wiki/landing, the cluster's data folder)
docker compose -f docker/docker-compose.yml exec lab \
    python3 tools/wiki_collector.py --minutes 15 --drift-after-files 6
```

`--drift-after-files 6` simulates a source-system upgrade: after about 2 minutes, every event gets a new
nested field, `collector`. You'll need it in step C7.

<div class="warn">

**Behind a corporate proxy or offline?** Add `--fake`. The collector then generates synthetic events
with the same shape: new pages, edits, deletes, moves, a few late events. Everything below works the
same, only the page titles are less interesting. Other wikis: `--wiki nlwiki`, `--wiki dewiki`, `--wiki ukwiki`.

</div>

## C1: explore before you build <span class="tag">3 min</span>

Wait for about 3 files, then batch-read the folder with inference and look before you design anything.

```python
{{#include ../../labs/capstone07_lab.py:c1_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/capstone07_wikipedia.py:c1_solution}}
```

</details>

Things to notice: `length` and `revision` are **nested structs**, `timestamp` is **unix seconds**, and
log events carry `log_params`, which is *sometimes an array and sometimes an object*. JSON inference
gives up on that conflict and calls it `string`. That's a real-world argument for keeping the raw line.

## C2: your own `schemaLocation` and `schemaHints` <span class="tag">4 min</span>

Mimic Auto Loader: infer from the **first two files only**, apply **hints** for the columns your logic
depends on, and store the result as `_schemas/schema_v0.json`. Every restart loads the latest version
instead of re-inferring. You write `apply_hints`. The versioned storage is given.

```python
{{#include ../../labs/capstone07_lab.py:c2_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/capstone07_wikipedia.py:c2_solution}}
```

</details>

Why hint `log_type` and `log_action`? If the first two files happened to contain no delete events,
inference wouldn't produce those columns, and `to_cdc` would fail with a missing-column error hours
later. Hints make the schema your code needs a **contract** instead of luck.

## C3: bronze that never loses data <span class="tag">4 min</span>

Instead of `format("json")`, read the files as **text**: one JSON document per line, kept verbatim in
`_raw`. Then parse it with `from_json(value, schema)`. Nothing is lost even when the schema is
incomplete, and you can compute an Auto Loader-style **rescued data** column. `_rescued_keys` holds the
top-level keys the schema doesn't know about:

```python
sf.array_except(sf.expr("json_object_keys(_raw)"), sf.array(*[sf.lit(n) for n in known_names]))
```

```python
{{#include ../../labs/capstone07_lab.py:c3_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/capstone07_wikipedia.py:c3_solution}}
```

</details>

## C4: silver, the current state of every article <span class="tag">5 min</span>

Map events to CDC ops (`to_cdc`), then write the MERGE. This is Try it 3 and 4 again, with two
real-world twists:

- **Timestamps have whole-second precision**, so two events for one page can tie. Break ties on
  `rev_id`, and use `>=` instead of `>` in the "newer than" guard.
- **A counter**: `events_seen = t.events_seen + s.n_events`. Think about what that does to
  idempotency. (See the review questions.)

```python
{{#include ../../labs/capstone07_lab.py:c4_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/capstone07_wikipedia.py:c4_solution}}
```

</details>

## C5: run it and watch <span class="tag">3 min</span>

Both queries run continuously with a 10-second trigger while the collector keeps dropping files.
Keep [localhost:4040](http://localhost:4040) → *Structured Streaming* open next to it.

```python
{{#include ../../labs/capstone07_lab.py:c5_starter}}
```

## C6: ask the data questions <span class="tag">4 min</span>

Use batch reads of bronze and silver while the streams keep running. Delta gives you a consistent
snapshot, even mid-write.

1. The 10 most active articles right now.
2. Bots vs humans: what share of edits do bots make?
3. Edits per minute.
4. The 5 biggest single edits, in bytes added.
5. Silver's change feed: counts per `_change_type`, and which articles were deleted.

```python
{{#include ../../labs/capstone07_lab.py:c6_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/capstone07_wikipedia.py:c6_solution}}
```

</details>

## C7: schema drift while the pipeline is running <span class="tag">4 min</span>

After the collector's 6th file, every event contains a `collector` object. Your bronze stream
doesn't break: the new key shows up in `_rescued_keys`, and the data is still in `_raw`.
Now do what `addNewColumns` does: **stop, evolve the stored schema (v1), and restart on the same
checkpoints.**

```python
{{#include ../../labs/capstone07_lab.py:c7_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/capstone07_wikipedia.py:c7_solution}}
```

</details>

<div class="pause">

**Reflect** <span class="tag">2 min</span>

- Rows ingested *before* the evolution have `NULL` in `collector.*`. How would you backfill them
  without rereading the landing files? (Hint: `_raw`.)
- Silver doesn't care about `collector`. Why did we still restart the silver query?

<details>
<summary>Answers</summary>

- `from_json(_raw, new_schema)` over the old bronze rows, written back with a MERGE on
  `(_source_file, meta.id)`, or simply by rebuilding silver from bronze. The raw line is your insurance.
- A Delta streaming source fixes its schema when the query starts. When bronze's schema changes under
  a running reader, that reader stops with a schema-change error, by design, so a human or a retry
  decides. On Databricks you'd let the job's retry policy restart it.

</details>
</div>

When you're done, stop the collector with Ctrl-C and run the last cell to stop all queries.
