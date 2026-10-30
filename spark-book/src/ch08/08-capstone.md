# Capstone: NYC restaurant grades

<span class="tag">25 min</span> <span class="tag">workbook: labs/capstone08_lab.py</span>

New York City's Department of Health inspects every restaurant about once or twice a year and gives it
a **letter grade** (A, B or C) that must hang in the window. The full inspection history is public on
[NYC Open Data](https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j),
updated daily, no key needed.

It's a textbook slowly changing dimension:

| Attribute | Changes when… | SCD treatment here |
|---|---|---|
| `grade` | after an inspection | **Type 2**: the point of the exercise |
| `dba` (the "doing business as" name) | rebrands, *and* data-entry corrections | Type 2 here. Discuss below whether that's right |
| `cuisine_description`, `zipcode` | rarely | Type 2 |
| `camis` | never: it's the restaurant's permanent id | the key |
| `boro` | never | carried along (Type 0) |

You'll replay several years of grades **month by month**, as if you'd run a monthly load since the
beginning, into a Type 2 dimension, then check it and ask it questions only SCD2 can answer.

## Step 0: get the data <span class="tag">2 min</span>

```bash
python tools/get_nyc_inspections.py                      # Manhattan, grades since 2022 (≈ 1–2 min)
python tools/get_nyc_inspections.py --boro all           # all five boroughs, bigger
python tools/get_nyc_inspections.py --fake               # offline: synthetic data, same columns and quirks

# local docker cluster
docker compose -f docker/docker-compose.yml exec lab python3 tools/get_nyc_inspections.py
```

The file lands in `data/nyc/inspections.csv` (`data-docker/nyc/` on the cluster).

## C1: profile before you model <span class="tag">3 min</span>

```python
{{#include ../../labs/capstone08_lab.py:c1_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/capstone08_nyc.py:c1_solution}}
```

</details>

What you should find: the file is **violation-level**. One inspection with four violations is four
rows that all carry the same grade. And occasionally one restaurant gets two different grades on the
same day. Neither fact is in the dataset's name. Profiling first is how you avoid a dimension with
4× the versions it should have.

## C2: one row per grading <span class="tag">3 min</span>

Reduce the file to **one row per restaurant per `grade_date`**, deterministically.

```python
{{#include ../../labs/capstone08_lab.py:c2_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/capstone08_nyc.py:c2_solution}}
```

</details>

## C3: a reusable SCD2 merge <span class="tag">6 min</span>

Generalise Try it 3 into `scd2_merge_rows(updates, path, key, tracked, eff_col)`. The new part:
**every row has its own effective date** (its `grade_date`), not one date per load. That also means
you must ignore a change that isn't newer than the current version.

```python
{{#include ../../labs/capstone08_lab.py:c3_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/capstone08_nyc.py:c3_solution}}
```

</details>

## C4: replay history month by month <span class="tag">3 min · mostly waiting</span>

The loop is given. Each month's changeset is the latest grading per restaurant within that month,
merged with `scd2_merge_rows`. Expect a few seconds per month on a laptop. Watch the job list in
[localhost:4040](http://localhost:4040), or see the work spread over two workers on the docker cluster
([localhost:8080](http://localhost:8080)).

```python
{{#include ../../labs/capstone08_lab.py:c4_starter}}
```

## C5: the checks every SCD2 table should pass <span class="tag">3 min</span>

Write the three checks you'd put in any SCD2 pipeline's test suite: **one current row per key**,
**contiguous versions** (each `valid_to` equals the next `valid_from`), and **no empty intervals**.

```python
{{#include ../../labs/capstone08_lab.py:c5_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/capstone08_nyc.py:c5_solution}}
```

</details>

## C6: questions only SCD2 can answer <span class="tag">5 min</span>

1. Grade distribution at a past date vs today.
2. Restaurants that fell straight from **A** to **C**.
3. How long a grade lasts, on average and at the median.
4. Restaurants that changed their name.
5. Cuisines with the highest share of C grades, today vs in the past.

```python
{{#include ../../labs/capstone08_lab.py:c6_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/capstone08_nyc.py:c6_solution}}
```

</details>

## C7: retries and late data <span class="tag">2 min</span>

```python
{{#include ../../labs/capstone08_lab.py:c7_starter}}
```

<details>
<summary>Solution</summary>

```python
{{#include ../../solutions/capstone08_nyc.py:c7_solution}}
```

</details>

<div class="pause">

**Discuss with your trainer** <span class="tag">3 min</span>

- Look at Q4's output. Which name changes are **rebrands** (Type 2 is right) and which are
  **corrections** like spacing, punctuation or "NYC" suffixes (Type 1 would be right)? How could a
  pipeline tell them apart automatically, and should it try?
- Delta **time travel** (`versionAsOf`) also shows "the table in the past". Why can't it replace this
  SCD2 table for Q1? (Hint: whose time is it, the load's or the inspection's? And what happens after `VACUUM`?)
- Re-delivering the first month changed nothing. Is that *correct*, or just *safe*?

</div>
