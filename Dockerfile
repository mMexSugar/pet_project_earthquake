FROM apache/airflow:3.1.2

USER airflow
RUN pip install --no-cache-dir duckdb==0.10.3
