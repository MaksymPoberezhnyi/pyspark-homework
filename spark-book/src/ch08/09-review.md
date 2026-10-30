# Review

<span class="tag">5 min</span>

## Review questions

<details>
<summary>1. When is SCD1 the right choice, not just the lazy one?</summary>

When the old value was never true: corrections, typos, a bad data-entry fix. Also for attributes
that are only ever used operationally, never in analysis, where history has a cost (storage, query
complexity) and no reader. "Nobody reports on it historically" is a *weak* reason, because someone
eventually will, and you can't recover history you didn't keep.

</details>

<details>
<summary>2. Why can't a single MERGE clause both expire and insert? How does the staged union get around it?</summary>

Each source row fires at most one clause. A changed key needs two actions, so the staged source
contains it twice: the keyed copy matches the current row and **expires** it, and the NULL-keyed copy
can't match anything and falls into **insert**. Unchanged and new keys appear once.

</details>

<details>
<summary>3. What breaks if the source contains two rows for one key? Where should dedup happen?</summary>

If the key exists in the target, the MERGE fails: several source rows match one target row. If it
doesn't, both are inserted as two "current" versions. Dedup the **input**, deterministically, by the
source's own ordering (event time, sequence, LSN), before staging. Never rely on `dropDuplicates`
to pick the right row.

</details>

<details>
<summary>4. Your SCD2 job ran twice for the same day. What guarantees there are no duplicate versions?</summary>

Hash-based change detection. After the first run, the current version's hash equals the incoming
row's, so nothing is staged for insert and nothing matches for update. With per-row effective dates
(the capstone), the `s.eff > t.valid_from` guard also makes re-deliveries of *older* data no-ops.

</details>

<details>
<summary>5. A fact table is joined to an SCD2 dim without the date condition. What happens to row counts?</summary>

Every fact is multiplied by the number of versions of its key, so totals are inflated. Always join with
`valid_from <= fact_date < valid_to`, or filter `is_current` for current-state reports. A cheap guard
in any pipeline is to assert the fact count before and after the join.

</details>

<details>
<summary>6. How do Chapter 7 and Chapter 8 combine in production?</summary>

Auto Loader (or a file stream) lands CDC events in bronze. A stream on bronze runs `foreachBatch`:
dedup per key, then an SCD2 MERGE using each event's own timestamp as its effective date, with deletes
expiring the current version (Try it 10). The SCD2 table's change feed feeds the gold models. On
Databricks, `create_auto_cdc_flow(stored_as_scd_type=2)` packages exactly this pattern.

</details>

## Post-check

```quiz
{
  "id": "ch08-postcheck",
  "title": "Chapter 8 post-check",
  "questions": [
    {
      "q": "Your SCD2 dimension uses `valid_to = NULL` for current rows instead of `9999-12-31`. An analyst writes the point-in-time join as `order_date >= valid_from AND order_date < valid_to`. What do they get?",
      "options": [
        "Correct results: Spark treats a NULL upper bound in a range comparison as unbounded, so current versions still match.",
        "Duplicate rows for current customers, because the NULL comparison matches both the current and the latest expired version.",
        "An analysis error, because Spark rejects comparisons between a DATE column and a column that contains NULL values.",
        "Every order whose matching version is the current one silently drops out, because `order_date < NULL` is NULL, never true."
      ],
      "answer": 3,
      "explain": "Comparisons with NULL are NULL, and a join keeps only rows where the condition is true. With NULL as the end date, every query needs `(valid_to IS NULL OR order_date < valid_to)`, and the queries where someone forgets it lose exactly the most recent data. That's the main argument for the far-future end date."
    },
    {
      "q": "A Type 2 MERGE uses the condition `t.customer_id = s.merge_key` but forgot `AND t.is_current = true`. Day 3 sends a real change for a customer who already has two versions. What can go wrong?",
      "options": [
        "Nothing: `whenMatchedUpdate` only fires on rows whose hash differs, and historical rows always have the same hash as the incoming row.",
        "The expired historical version can match too and get its `valid_to` overwritten, so closed intervals stretch and overlap.",
        "The MERGE fails because one source row matches several target rows, which Delta rejects just like duplicate source keys.",
        "The NULL-keyed copy now matches the historical version, so the new version is never inserted and the change is lost."
      ],
      "answer": 1,
      "explain": "One source row matching *several* target rows is allowed. Only several source rows matching *one* target row is an error. So the historical rows match, and any whose hash differs gets expired again with a new `valid_to`. The 'nothing goes wrong' option is the trap: historical rows have *different* hashes from the new value most of the time, and the same hash only when an attribute moves back."
    },
    {
      "q": "You're asked to show each order with the customer's region at the time of the order AND the customer's region today, in one query against one dimension. Which design fits best?",
      "options": [
        "Type 6: SCD2 versions that also carry the current region, stored or computed by joining the dimension to its own current rows.",
        "Type 3: keep `region` and `previous_region` columns, so both values are always available on the single row per customer.",
        "Type 4: join the orders to the history table for the old region and to the current table for today's region in two queries.",
        "Type 1 with Delta time travel: read the dimension `versionAsOf` the order date for the old region and the latest version for today's."
      ],
      "answer": 0,
      "explain": "Type 6 is exactly 'historical value + current value on the same version row'. Type 3 has only one previous value, so a customer who moved twice loses the region that was valid at older orders. Time travel works by table version, which is load time, not order time, and it disappears after VACUUM."
    }
  ]
}
```

**You made it.** Go back to the [Chapter 7 pre-check](../ch07/01-precheck.md) and the
[Chapter 8 pre-check](./01-precheck.md). Would you still pick the same answers? Then use the
[cheat sheet](../appendix/cheatsheet.md) the next time you write one of these at work.
