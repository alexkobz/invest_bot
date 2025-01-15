FROM apache/airflow:2.10.4
USER airflow
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
USER root
COPY .env .env
COPY api_model_python api_model_python
COPY dbt dbt
