"""Shared helpers for every lab in the book.

    from common import get_spark, DATA_DIR
    spark = get_spark("my-lab")

`get_spark()` gives you a Delta-enabled SparkSession.

* Laptop mode (default): `local[*]` — driver and executors in one JVM.
* Cluster mode: set SPARK_MASTER=spark://spark-master:7077 (the docker compose
  file in docker/ does this for you) — a real standalone cluster with a master
  and two workers, sharing the repo folder as /workspace.
"""
import os
import sys

# Everything the labs write (landing files, Delta tables, checkpoints) lives here.
BOOK_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
DATA_DIR = os.environ.get("BOOK_DATA", os.path.join(BOOK_ROOT, "data"))


def _guess_java_home():
    if os.environ.get("JAVA_HOME"):
        return
    for candidate in (
        "/opt/homebrew/opt/openjdk@17",                         # Apple-silicon Homebrew
        "/usr/local/opt/openjdk@17",                            # Intel Homebrew
        "/usr/lib/jvm/java-17-openjdk-amd64",                   # Debian/Ubuntu
        "/usr/lib/jvm/java-17-openjdk-arm64",
    ):
        if os.path.isdir(candidate):
            os.environ["JAVA_HOME"] = candidate
            return


def get_spark(app_name="spark-book", shuffle_partitions=4):
    _guess_java_home()
    # Driver and executors must use the same Python.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    import pyspark
    from delta import configure_spark_with_delta_pip

    master = os.environ.get("SPARK_MASTER", "local[*]")
    builder = (
        pyspark.sql.SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Tiny lab data: the default of 200 shuffle partitions would be pure overhead.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if master.startswith("spark://"):
        # Executors live in other containers: they must be able to call back to the driver.
        builder = (
            builder
            .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "lab"))
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1g"))
            .config("spark.executor.cores", os.environ.get("SPARK_EXECUTOR_CORES", "1"))
        )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark {spark.version} on {master} — UI: {spark.sparkContext.uiWebUrl}")
    return spark
