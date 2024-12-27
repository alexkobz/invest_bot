FROM apache/airflow:2.10.4
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY .env .env
COPY api_model_python api_model_python
COPY clients clients
COPY dbt dbt
COPY exceptions exceptions
COPY src src
