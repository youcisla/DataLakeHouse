# ============================================================================
#  Image Python de base du projet DataLake Météo
#  Utilisée par : kafka-producer (ingestion temps réel) et streamlit (dashboard)
# ============================================================================
FROM python:3.11-slim

RUN pip install --no-cache-dir \
    requests==2.31.0 \
    kafka-python==2.0.2 \
    pandas==2.1.4 \
    numpy==1.26.2 \
    pyarrow==15.0.2 \
    scikit-learn==1.3.2 \
    xgboost==2.0.3 \
    joblib==1.3.2 \
    streamlit==1.33.0 \
    plotly==5.18.0 \
    streamlit-autorefresh==1.0.1 \
    pytest==8.0.0

WORKDIR /opt/project
ENV PYTHONPATH=/opt/project/scripts

CMD ["python", "--version"]
