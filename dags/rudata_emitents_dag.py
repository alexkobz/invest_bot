from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import nest_asyncio; nest_asyncio.apply()
import sys
import os

# Add the `scripts` directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from api_model_python.api_rudata_emitents import api_rudata_emitents


with DAG(
    dag_id='rudata_emitents',
    description='A pipeline with downloading emitents from RuData',
    start_date=datetime(2024, 12, 27),
    schedule="0 3 * * *",
    catchup=False,
) as dag:

    t1_start = EmptyOperator(task_id='start')

    t2_api_rudata_emitents = PythonOperator(
        task_id='api_rudata_emitents',
        python_callable=api_rudata_emitents
    )

    t3_stg_rudata_emitents = BashOperator(
        task_id='stg_rudata_emitents',
        bash_command=f"cd /opt/airflow/dbt && dbt run --select stg_rudata_emitents"
    )

    t4_dim_rudata_emitents = BashOperator(
        task_id='dim_rudata_emitents',
        bash_command=f"cd /opt/airflow/dbt && dbt run --select dim_rudata_emitents"
    )

    t5_finish = EmptyOperator(task_id='finish')

    (
        t1_start >>
        t2_api_rudata_emitents >>
        t3_stg_rudata_emitents >>
        t4_dim_rudata_emitents >>
        t5_finish
    )
