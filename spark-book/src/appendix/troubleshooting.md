# Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Java gateway process exited before sending its port number` | no Java, or the wrong Java | install Java 17, `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` on macOS |
| `ModuleNotFoundError: delta` | wrong venv or kernel | `pip install -r requirements.txt` in the venv your Jupyter kernel uses |
| `java.lang.ClassNotFoundException: ...DeltaSparkSessionExtension` | Delta jars not loaded: a session was created *before* `get_spark()` | restart the kernel and call `get_spark()` first |
| `IllegalArgumentException: Schema must be specified when creating a streaming source DataFrame` | `readStream.format("json")` without `.schema(...)` | pass a schema (that's the point of Try it 1) |
| `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` | two source rows for one key | dedup the source per key first (Ch 7 rule 1, Ch 8 Try it 4b) |
| A stream reads nothing after you changed its code | the checkpoint says those files are done | expected. To reprocess, use a **new** checkpoint *and* a fresh target (or a MERGE sink) |
| `Detected schema change` / `DELTA_SCHEMA_CHANGED` from a Delta stream | the source table's schema changed while the query ran | restart the query. It picks up the new schema (capstone 7, C7) |
| A streaming query keeps running after an error in a cell | a long-running query lives on in the session | `for q in spark.streams.active: q.stop()` |
| Lab asserts fail after re-running cells out of order | the lab state is half from an old run | re-run the workbook's **setup cell** (it resets the lab), then the tasks in order |
| Docker: `Initial job has not accepted any resources` | workers not registered, or asking for more memory than they have | check [localhost:8080](http://localhost:8080). Lower `SPARK_EXECUTOR_MEMORY` |
| Docker: file not found on executors only | a path outside `/workspace` | keep all data under the book folder (`DATA_DIR`) |
| `wiki_collector.py`: 403 or connection errors | corporate proxy or firewall | `--fake`, or set `HTTPS_PROXY` for your network |
| `get_nyc_inspections.py`: HTTP 429 | Socrata rate limit for anonymous users | wait a minute, or set `SOCRATA_APP_TOKEN` (free at data.cityofnewyork.us) |
