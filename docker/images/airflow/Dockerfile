# ============================================================================
#  Image Airflow du projet DataLake Météo
#  Airflow 2.9.3 (Python 3.11) + Java 17 + PySpark (SparkSubmitOperator) +
#  providers Spark + utilitaires HDFS (requests/WebHDFS)
# ============================================================================
FROM apache/airflow:2.9.3-python3.11

USER root

# Java est requis par PySpark (spark-submit lancé depuis Airflow)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# PySpark + provider Spark (SparkSubmitOperator) + WebHDFS (requests)
RUN pip install --no-cache-dir \
    "pyspark==3.5.1" \
    "apache-airflow-providers-apache-spark==4.7.1" \
    "requests==2.31.0" \
    "pandas==2.1.4" \
    "pyarrow==15.0.2"

ENV PYTHONPATH=/opt/project/scripts

USER airflow
