# One image for master, workers and the Jupyter "lab" driver, so Python and
# Spark versions match everywhere (a hard requirement for PySpark).
FROM apache/spark:3.5.3-python3

USER root
# pip picks the newest versions that support the image's Python.
RUN python3 -m pip install --no-cache-dir \
        pyspark==3.5.3 \
        delta-spark==3.2.1 \
        jupyterlab \
        jupytext \
        requests \
    && mkdir -p /workspace

# Pre-fetch the Delta jars into the image so a session starts without a download.
RUN python3 -c "import pyspark; from delta import configure_spark_with_delta_pip as c; \
s = c(pyspark.sql.SparkSession.builder.master('local[1]')).getOrCreate(); s.stop()"

ENV PATH="/opt/spark/bin:${PATH}" \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3
WORKDIR /workspace
