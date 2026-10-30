# Setup: a local Spark cluster in 10 minutes

Pick **one** option. Both run the same code: `labs/common.py` decides from the `SPARK_MASTER`
environment variable whether to start an in-process Spark or connect to the cluster.

| | Option A: laptop (`local[*]`) | Option B: docker standalone cluster |
|---|---|---|
| What runs | one JVM: driver + executors | master + 2 workers + a JupyterLab driver, in 4 containers |
| Start-up | fastest | ~1 min once the image is built |
| Feels like | a notebook | a real cluster: separate executors, master UI, shared storage |
| Needs | Python 3.10/3.11, Java 17 | Docker Desktop (4 GB RAM for Docker) |

## Option A: laptop

```bash
# 1. Java 17 (Spark 3.5 runs on 8/11/17; 17 is the safe choice)
brew install openjdk@17                    # macOS; Linux: apt install openjdk-17-jdk

# 2. a virtualenv with the pinned versions
cd spark-book
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt            # pyspark 3.5.3 + delta-spark 3.2.1 + jupyterlab + jupytext

# 3. smoke test: Delta write, MERGE, change feed, one streaming query
python labs/check_env.py                   # → "Environment OK ✅"

# 4. work
cd labs && jupyter lab
```

`common.py` sets `JAVA_HOME` automatically for Homebrew's `openjdk@17` if it isn't set. The
first `get_spark()` downloads the Delta jars (~5 MB) from Maven Central, and later runs use the cache.

## Option B: quick local cluster (docker compose)

```bash
cd spark-book/docker
docker compose up -d --build               # first build ≈ 3–5 min, later starts ≈ 30 s
docker compose exec lab python3 labs/check_env.py     # → "Environment OK ✅"
```

| URL | What |
|---|---|
| [localhost:8888](http://localhost:8888) | JupyterLab, opened on the book folder (no token) |
| [localhost:8080](http://localhost:8080) | Spark master: workers, running applications |
| [localhost:4040](http://localhost:4040) | the driver UI: jobs, stages, the **Structured Streaming** tab |

How it's wired, and why it matters for this book:

```text
 ┌─────────── docker network "spark" ────────────┐
 │  lab (driver, JupyterLab) ──▶ spark-master:7077│
 │        ▲                         │             │
 │        └── executors ◀── spark-worker-1, -2    │
 └────────────────────────────────────────────────┘
   all four containers mount the book folder at /workspace
```

Executors write the Delta files and the driver writes the checkpoints, so **all of them must see
the same paths**. On a real cluster that's S3, ADLS or GCS. Here it's the shared `/workspace`
mount. If you ever see "file not found" on the cluster but not on the laptop, it's always this.

Stop it with `docker compose down`. The cluster writes to **`data-docker/`** in the book folder, and the
laptop setup writes to `data/`. They're kept apart because the containers run as root, and root-owned
files would otherwise break `reset_lab()` in laptop mode.

## Building and reading this book

```bash
brew install mdbook                        # or: cargo install mdbook  (or a binary from GitHub releases)
cd spark-book
mdbook serve --open                        # http://localhost:3000, live-reloads on edit
```

Quiz answers are stored in your browser's local storage. Use *Reset answers* under a quiz to try again.

## Working conventions

- Run each workbook's **setup cell first**. It resets that lab's data folder, so you can start over any time.
- Every *Try it* ends with `assert`s. ✅ means done. An `AssertionError` prints what it got.
- Stuck for more than 5 minutes? Open the **hint**, then the **solution**. Read it, close it, and
  write it yourself.
- Trainers: `python solutions/ch07_solutions.py` and `python solutions/ch08_solutions.py` run every
  task end to end as a self-test (≈ 2–3 min each).
