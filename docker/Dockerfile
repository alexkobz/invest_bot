FROM apache/airflow:2.10.5
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
         vim \
         curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
# install Python deps
COPY ../requirements.txt /opt/airflow/requirements.txt
RUN pip install --upgrade pip && pip install -r /opt/airflow/requirements.txt
#USER root
USER root
WORKDIR /opt/airflow
COPY ../.env ./.env
