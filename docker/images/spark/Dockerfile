# ============================================================================
#  Image Spark du projet DataLake Météo
#  Apache Spark 3.5.1 + dépendances ML (XGBoost, scikit-learn...) + config zstd
# ============================================================================
FROM apache/spark:3.5.1

USER root

# Dépendances Python nécessaires aux jobs (Silver/Gold/ML/GenAI)
RUN pip install --no-cache-dir \
    pandas==2.0.3 \
    numpy==1.24.4 \
    scikit-learn==1.3.2 \
    xgboost==2.0.3 \
    pyarrow==15.0.2 \
    requests==2.31.0 \
    joblib==1.3.2

# Les scripts du projet (hdfs_utils, etc.) doivent être importables par les drivers
ENV PYTHONPATH=/opt/project/scripts

# Compression Parquet Zstd niveau 22 + overwrite dynamique des partitions
COPY configs/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf

USER spark
