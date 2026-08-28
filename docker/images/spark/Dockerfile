# ============================================================================
#  Image Spark du projet DataLake Météo
#  Apache Spark 3.5.1 + dépendances ML (XGBoost, scikit-learn...) + config zstd
#  Python 3.11 compile depuis les sources : les executors doivent tourner en
#  3.11 pour correspondre au driver Airflow 3.11 (sinon PYTHON_VERSION_MISMATCH,
#  car apache/spark:3.5.1 embarque Python 3.8). deadsnakes ne fournit plus
#  python3.11 pour Ubuntu 20.04 (focal), d'où la compilation.
# ============================================================================
FROM apache/spark:3.5.1

USER root

# Dépendances de compilation + téléchargement des sources Python 3.11.9
RUN apt-get update && apt-get install -y --no-install-recommends build-essential libssl-dev libffi-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev libncursesw5-dev liblzma-dev tk-dev ca-certificates curl && rm -rf /var/lib/apt/lists/*

# Compilation -> /usr/local/bin/python3.11 (altinstall : n'écrase pas python3 = 3.8,
# réservé aux scripts utilitaires export_web.py / verify_medallion.py).
RUN curl -sS https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz -o /tmp/python.tgz && tar -xzf /tmp/python.tgz -C /tmp && cd /tmp/Python-3.11.9 && ./configure --prefix=/usr/local --enable-shared LDFLAGS="-Wl,-rpath /usr/local/lib" && make -j"$(nproc)" && make altinstall && ldconfig && rm -rf /tmp/Python-3.11.9 /tmp/python.tgz

# pip pour Python 3.11
RUN curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py && python3.11 /tmp/get-pip.py --no-cache-dir && rm -f /tmp/get-pip.py

# Dépendances Python des jobs (Silver/Gold/ML/GenAI) pour le Python 3.11 des executors
RUN python3.11 -m pip install --no-cache-dir pandas==2.0.3 numpy==1.24.4 scikit-learn==1.3.2 xgboost==2.0.3 pyarrow==15.0.2 requests==2.31.0 joblib==1.3.2

# Mêmes dépendances pour le Python 3.8 du systeme (scripts utilitaires sur spark-master)
RUN pip install --no-cache-dir pandas==2.0.3 numpy==1.24.4 scikit-learn==1.3.2 xgboost==2.0.3 pyarrow==15.0.2 requests==2.31.0 joblib==1.3.2

# Les scripts du projet (hdfs_utils, etc.) doivent être importables par les drivers
ENV PYTHONPATH=/opt/project/scripts

# Compression Parquet Zstd niveau 22 + overwrite dynamique des partitions
COPY configs/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf

USER spark
