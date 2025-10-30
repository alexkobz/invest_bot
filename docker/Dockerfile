FROM apache/airflow:3.1.1
USER airflow
# install Python deps
COPY ../requirements.txt .
RUN pip install --no-cache-dir -r ./requirements.txt

USER root
WORKDIR /opt/airflow
#ENV PYTHONPATH=/opt/airflow
COPY ../.env ./.env
RUN mkdir "logs"
