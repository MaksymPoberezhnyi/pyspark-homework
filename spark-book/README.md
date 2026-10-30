# Hands-on Spark: Auto Loader, CDC & Slowly Changing Dimensions

An interactive [mdBook](https://rust-lang.github.io/mdBook/) workbook with PySpark + Delta Lake labs,
running on a local Spark (laptop or a docker standalone cluster). It takes 2–3 hours.

```bash
# read the book
mdbook serve --open                      # http://localhost:3000

# run the labs: laptop
pip install -r requirements.txt && python labs/check_env.py && (cd labs && jupyter lab)

# …or on a local cluster (master + 2 workers + JupyterLab)
cd docker && docker compose up -d --build   # JupyterLab on http://localhost:8888
```

- `src/`: book chapters (pre-checks, concepts, Try-its, capstones, reviews)
- `labs/`: the workbooks learners run (`# %%` cells; VS Code or JupyterLab + jupytext)
- `solutions/`: reference solutions, each runnable end-to-end as a self-test
- `tools/`: real-world data: the live Wikipedia edit stream, NYC restaurant inspections (`--fake` for offline)
- `docker/`: the local cluster

See *Notes for trainers* in the book for the mapping to the original 1-08 / 1-09 notebooks.
