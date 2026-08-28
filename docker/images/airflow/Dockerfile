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
ENV PYTHONPATH=/opt/project/scripts

# IMPORTANT : l'image officielle Airflow REFUSE pip en root
# ("You are running pip as root. Please use 'airflow' user to run pip!").
# On repasse donc sur l'utilisateur airflow AVANT d'installer les paquets
# Python ; ils atterrissent dans /home/airflow/.local, deja dans le PATH.
USER airflow

# PySpark + provider Spark (SparkSubmitOperator) + WebHDFS (requests)
RUN pip install --no-cache-dir \
    "pyspark==3.5.1" \
    "apache-airflow-providers-apache-spark==4.7.1" \
    "requests==2.31.0" \
    "pandas==2.1.4" \
    "pyarrow==15.0.2" \
    "numpy==1.26.4" \
    "scikit-learn==1.3.2" \
    "xgboost==2.0.3" \
    "joblib==1.3.2"
