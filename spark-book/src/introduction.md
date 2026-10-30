# Introduction

This book is a hands-on workbook for two connected topics in Apache Spark data engineering:

- **Chapter 7: Auto Loader & Change Data Capture.** Ingest files incrementally with checkpoints,
  survive schema drift, and fold a stream of insert/update/delete events into current state with
  `foreachBatch` + Delta `MERGE`.
- **Chapter 8: Slowly Changing Dimensions.** Keep history instead of overwriting it: Type 1 and 2 in
  depth, plus Types 3, 4 and 6, all implemented with Delta `MERGE`.

All code is **PySpark** with **Delta Lake**, running on your own machine, either in a single process
(`local[*]`) or on a small standalone cluster in Docker. No cloud account is needed.

## How every chapter works

| Step | What it looks like | Why |
|---|---|---|
| **1. Pre-check** | 3 multiple-choice questions | find out what you *think* you know. Wrong answers are expected and useful |
| **2. Concepts** | the problem, the vocabulary, real-world uses | so the code has something to hang on |
| **3. Try it** | <span class="tag">blue boxes</span> starter code, hints, hidden solutions, asserts that print ✅ | the actual learning happens here |
| **Pause** | <span class="tag">amber boxes</span> predict, explain out loud, then reveal | turns "it ran" into "I know why" |
| **4. Capstone** | the same concepts on real, public data | proves it transfers beyond toy rows |
| **Review** | review questions + a second quiz | check yourself against the pre-check |

## Time plan: about 3 hours

| Block | Time |
|---|---|
| Setup (one-off) | 10–15 min |
| Chapter 7, lessons + Try it 1–7 | 65 min |
| Chapter 7 capstone: live Wikipedia edits | 25 min |
| Break | |
| Chapter 8, lessons + Try it 1–10 | 65 min |
| Chapter 8 capstone: NYC restaurant grades | 25 min |

In a tight 2 hours, skip Try it 7 (Ch 7), Try it 8–10 (Ch 8) and C6–C7 of both capstones.
They're marked as optional extensions in the trainer notes.

## The files

```text
spark-book/
├── src/                 ← this book (mdBook sources)
├── labs/                ← YOUR workbooks: run these while reading
│   ├── common.py            get_spark(): Delta-enabled session, local or cluster
│   ├── ch07_lab.py          Chapter 7 tasks
│   ├── capstone07_lab.py    Chapter 7 capstone
│   ├── ch08_lab.py          Chapter 8 tasks
│   └── capstone08_lab.py    Chapter 8 capstone
├── solutions/           ← reference solutions, each runnable end-to-end as a self-test
├── tools/
│   ├── wiki_collector.py    live Wikipedia edit stream → landing files
│   └── get_nyc_inspections.py   NYC Open Data downloader
├── docker/              ← quick local cluster: master + 2 workers + JupyterLab
└── data/                ← everything the labs write (git-ignored, safe to delete)
```

The `.py` workbooks use the `# %%` cell format. **VS Code** runs them as notebooks out of the box, and
**JupyterLab** opens them as notebooks through `jupytext` (right-click → *Open With → Notebook*).
The book shows the same code you see in the workbook, because it is included from those files, so
book and workbook can't drift apart.

Ready? → [Setup](./setup.md)
