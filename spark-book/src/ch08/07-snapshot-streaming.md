# Snapshots and streaming SCD2

<span class="tag">10 min</span>

## When the source sends full snapshots

Some sources send the **complete** current state every day instead of a changeset. Then a customer
who is *missing* from the snapshot has been deleted. Nobody sends you a delete event. Delta's MERGE
has a clause for exactly that: **`whenNotMatchedBySourceUpdate`**, for target rows that *no* source
row matched (its sibling is `whenNotMatchedBySourceDelete`). In SCD2 you don't delete. You
**soft-delete**: close the current version.

<div class="tryit">

## Try it 9 (bonus): soft-delete customers who disappeared <span class="tag">4 min</span>

Copy `scd2_merge` into `scd2_merge_snapshot` and add one clause:

```python
.whenNotMatchedBySourceUpdate(
    condition="t.is_current = true",        # why is this condition essential?
    set={"is_current": "false", "valid_to": f"cast('{effective_date}' as date)"})
```

Test it with a snapshot of every current customer *except* Carol (id 3), merged on `2026-10-15`.

```python
{{#include ../../labs/ch08_lab.py:t9_starter}}
```

<details>
<summary>Solution, and why the condition is essential</summary>

```python
{{#include ../../solutions/ch08_solutions.py:t9_solution}}
```

The merge condition includes `t.is_current = true`, so **every historical row is "not matched by
source"**. Without the condition, the clause would rewrite `valid_to` on all of them, on every run.

</details>
</div>

## Chapter 7 meets Chapter 8: a CDC stream that maintains an SCD2 dimension

In Chapter 7 the CDC stream *overwrote* silver (Type 1). Swap the MERGE inside `foreachBatch` for an
SCD2 MERGE, and the same stream keeps full history. Three differences from the batch `scd2_merge`:

| Batch `scd2_merge` | Streaming `scd2_from_cdc` |
|---|---|
| one effective date per load | **each event's own `ts`** is its effective date (timestamps, not dates) |
| no deletes | `op = 'D'` **expires** the current version and inserts nothing |
| every load is newer than the table | late events: expire or insert **only if `s.ts > t.valid_from`** |
| new keys inserted by their keyed copy | **only** NULL-keyed copies insert (`condition="s.merge_key IS NULL"`), and new keys get a NULL copy too |

<div class="tryit">

## Try it 10: streaming SCD2 from CDC events <span class="tag">6 min</span>

Use Chapter 7's three batches of events (insert, update, delete, a late update), one micro-batch per
file, into an SCD2 dimension with timestamp validity.

```python
{{#include ../../labs/ch08_lab.py:t10_starter}}
```

<details>
<summary>Hint: which events become a NEW version?</summary>

A left join of the deduplicated events to the current rows. Keep events where `op <> 'D'` AND
(there is no current row OR (the hash differs AND `e.ts > c.valid_from`)).

</details>

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/ch08_solutions.py:t10_solution}}
```

</details>
</div>

<div class="pause">

**Two limitations worth saying out loud** <span class="tag">2 min</span>

1. **Bob's first version (Lviv) is missing.** His insert and update arrived in the same micro-batch,
   and dedup kept only the newest. If every intermediate state matters, don't collapse. Apply the
   events of a key *in order*, for example by iterating over `row_number()` ranks with one MERGE per
   rank, or by building each key's versions with window functions (`lead(ts)` as `valid_to`) and
   MERGE-ing whole histories.
2. **The late Alice update was ignored**, not *inserted into the past*. Strictly, Alice was in Lviv
   from 10-01 15:00 until 10-02 10:00, and a perfect SCD2 would split her first version in two. That's
   called **late-arriving dimension changes**. The usual fix is to rebuild the affected keys' histories
   from bronze, which you keep for exactly this reason.

On Databricks, `create_auto_cdc_flow(..., stored_as_scd_type=2)` handles both, given a sequence column.

</div>
