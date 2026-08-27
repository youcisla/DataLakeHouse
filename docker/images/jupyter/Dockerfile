# ============================================================================
#  Image Jupyter du projet DataLake Météo
#  Lecture des tables Gold via WebHDFS (requests) + Pandas / Matplotlib / Seaborn
# ============================================================================
FROM jupyter/base-notebook:latest

USER root
RUN pip install --no-cache-dir \
    pandas==2.1.4 \
    matplotlib==3.8.2 \
    seaborn==0.13.0 \
    requests==2.31.0 \
    pyarrow==15.0.2
USER jovyan
