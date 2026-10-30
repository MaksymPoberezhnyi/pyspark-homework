# Chapter 8 — Slowly Changing Dimensions

<span class="tag">≈ 90 min</span> <span class="tag">builds on: Chapter 7's MERGE patterns</span>

Chapter 7 built a table holding the **current state** of every entity, and threw the past away.
This chapter is about keeping it. It's the culmination of the batch MERGE logic: every pattern you
learned (dedup, conditions, hashing, idempotency) comes back, combined.

| Section | What you do | Time |
|---|---|---|
| [Pre-check](./01-precheck.md) | 3 questions | 5 min |
| [What problem SCD solves](./02-why.md) | dimensions, the SCD types, real-world uses | 10 min |
| [SCD Type 1](./03-scd1.md) | Try it 1: overwrite, but only real changes | 8 min |
| [SCD Type 2: design and the staged union](./04-scd2.md) | Try it 2–3: the most important MERGE in data engineering | 20 min |
| [Making SCD2 robust, and using it](./05-scd2-robust.md) | Try it 4–5: retries, dirty input, point-in-time joins | 12 min |
| [Types 3, 4 and 6](./06-scd346.md) | Try it 6–8 | 10 min |
| [Snapshots and streaming SCD2](./07-snapshot-streaming.md) | Try it 9–10: soft deletes; Chapter 7 meets Chapter 8 | 10 min |
| [Capstone: NYC restaurant grades](./08-capstone.md) | real public data, 3+ years of history replayed into an SCD2 table | 25 min |
| [Review](./09-review.md) | review questions + a second quiz | 5 min |

Workbook: **`labs/ch08_lab.py`**. Reference solutions: `solutions/ch08_solutions.py`, also hidden
under every *Try it*.
