# Making SCD2 robust, and using it

<span class="tag">12 min</span>

## Pipelines get rerun

Jobs fail halfway and are retried. Somebody reruns last Tuesday's load "just to be sure". A backfill
replays a month. An SCD2 job that isn't **idempotent** turns every rerun into duplicate versions,
and they're hard to clean up because each one looks legitimate.

<div class="tryit">

## Try it 4a: rerun the same load <span class="tag">2 min</span>

Run `scd2_merge(day3, ...)` a second time. The row count must stay at 8. If your Try it 3 is right,
you don't need to change anything. Before you run it, explain *why* it will hold.

```python
{{#include ../../labs/ch08_lab.py:t4a_starter}}
```

<details>
<summary>Why it holds</summary>

After the first day-3 run, every day-3 customer's *current* row has the same hash as the incoming row.
So `changed` is empty (no NULL-keyed copies, so no inserts), and the keyed copies all match with equal
hashes (no updates). Hash-based change detection makes SCD2 idempotent.

</details>
</div>

<div class="tryit">

## Try it 4b: two rows for one key <span class="tag">4 min</span>

The source sends the same customer **twice** in one load. Run your `scd2_merge` on `day4_dirty`
inside `try/except` and read the error. A red Java stack trace in the output is expected. Then fix
the *input*: keep one row per `customer_id` and merge again on `2026-10-12`.

```python
{{#include ../../labs/ch08_lab.py:t4b_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/ch08_solutions.py:t4b_solution}}
```

</details>

Notice the deterministic dedup. `orderBy(...).dropDuplicates([...])` looks like it keeps the first
row, but Spark doesn't guarantee which duplicate survives. A window with `row_number()` does. In real
pipelines, order by the source's event timestamp or sequence, exactly like `latest_per_id` in Chapter 7.

</div>

## Why SCD2 exists: point-in-time joins

The whole point of keeping versions is to join each fact to **the version that was valid when the
fact happened**. That's a *range join*:

```python
o.customer_id == d.customer_id
& (o.order_date >= d.valid_from)
& (o.order_date <  d.valid_to)        # exclusive end: exactly one version matches
```

<div class="tryit">

## Try it 5: revenue then vs revenue now <span class="tag">6 min</span>

Join each order to the customer version valid on the order date. Then compare revenue by city **at
the time of the order** with revenue by **current** city (a join to `is_current` rows only). Which
city's revenue disappears in the second report?

```python
{{#include ../../labs/ch08_lab.py:t5_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/ch08_solutions.py:t5_solution}}
```

</details>

Neither report is wrong. They answer different questions. *"Which city generated our revenue?"* →
at order time: Dnipro 30. *"Where are our revenue-generating customers now?"* → current: no Dnipro.
SCD2 lets you answer both, and Type 6 (two sections on) puts both in one row.

</div>

<div class="realworld">

**Performance note for big tables:** range joins can't use a plain hash join on the date condition.
Spark hash-joins on `customer_id` and then filters, which is fine as long as customers have a handful
of versions each. For very large dimensions, Databricks has a range-join optimization hint, and on
plain Spark it can help to add a coarse equality key (for example the year of `valid_from`) when
versions are dense.

</div>
