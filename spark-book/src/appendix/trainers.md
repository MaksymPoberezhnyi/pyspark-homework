# Notes for trainers

## Where this came from

The book is built on the two task notebooks in this repository, `1-08-autoloader-cdc.md` and
`1-09-scd.md`. Their tasks, data and asserts are kept as the backbone. The originals are untouched.

| Original | In this book | What changed |
|---|---|---|
| 1-08 tasks 1–6 | Ch 7 Try it 1–6 | split into one section per concept; pre-check, concepts, real-world uses and pause questions added; `drop_batch` writes atomically; silver created with `DeltaTable.createIfNotExists`; explicit evolution uses an empty `mergeSchema` append instead of SQL `ALTER` (PySpark only) |
| 1-08 "the real Autoloader" | Ch 7 §7 | kept, updated to `create_auto_cdc_flow`, plus `maxFilesPerTrigger` |
| 1-08 review questions | Ch 7 Review | now with model answers, plus a 3-question post-check |
| (new) | Ch 7 Try it 7 | the same pipeline with `processingTime`, live |
| (new) | Ch 7 Capstone | live Wikipedia EventStreams → bronze with home-made schemaLocation, hints and rescued keys → silver MERGE → CDF; schema drift mid-run |
| 1-09 tasks 1–6 + bonus | Ch 8 Try it 1–6, 9 | 4b dedup made deterministic (a window instead of `orderBy` + `dropDuplicates`); Type 3 uses null-safe `<=>` |
| (new) | Ch 8 Try it 7, 8 | Type 4 via the Change Data Feed, Type 6 as a view |
| 1-09 review Q6 | Ch 8 Try it 10 | the hinted "CDC stream → SCD2" is now implemented and tested |
| (new) | Ch 8 Capstone | NYC Open Data restaurant grades, replayed monthly into SCD2, with quality checks and analysis |

## Self-test before a session

```bash
python labs/check_env.py
python solutions/ch07_solutions.py          # prints ✅ task 1 … ✅ task 7
python solutions/ch08_solutions.py          # prints ✅ task 1 … ✅ task 10
python tools/wiki_collector.py --out data/wiki/landing --minutes 3 --fake &&  python solutions/capstone07_wikipedia.py
python tools/get_nyc_inspections.py --fake && python solutions/capstone08_nyc.py
```

## Fitting the time box

| Session length | Do | Skip or assign as homework |
|---|---|---|
| 3 h | everything | — |
| 2.5 h | everything except the experiments | Ch 7 Try it 7; Ch 8 Try it 8 |
| 2 h | pre-checks, concepts, Ch 7 Try it 1–6, Ch 8 Try it 1–5, both capstones up to C5 | Ch 7 Try it 7; Ch 8 Try it 6–10; capstones C6–C7 |

The capstones need network access to `stream.wikimedia.org` and `data.cityofnewyork.us`. Test
from the training network beforehand. Both tools have `--fake`, which produces data of the same shape.

## Editing the book

- Code shown in the book is **included** from `labs/` and `solutions/` with mdBook anchors
  (`# ANCHOR: name` … `# ANCHOR_END: name`). Change the `.py` file and the book follows.
- Quizzes are fenced ```` ```quiz ```` blocks containing JSON (`q`, `options`, zero-based `answer`,
  `explain`), rendered by `theme/quiz.js`. Keep options similar in length and plausibility. The
  pre-check is only useful when the right answer can't be spotted from its shape.
- Callouts are `<div class="tryit">`, `<div class="pause">`, `<div class="realworld">` and
  `<div class="warn">` (styles in `theme/quiz.css`). Leave a blank line after the opening tag so the
  markdown inside renders.
