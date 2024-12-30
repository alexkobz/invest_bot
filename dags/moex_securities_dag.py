from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import sys
import os

# Add the `scripts` directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from api_model_python.api_moex_securities_trading import api_moex_securities_trading


with DAG(
    dag_id='moex_securities_trading',
    description='A pipeline with downloading MOEX securities',
    start_date=datetime(2024, 12, 27),
    schedule="0 3 * * *",
    catchup=False,
) as dag:

    t1_start = EmptyOperator(task_id='start')

    t2_api_moex_securities_trading = PythonOperator(
        task_id='api_moex_securities_trading',
        python_callable=api_moex_securities_trading
    )

    t3_stg_moex_securities_trading = BashOperator(
        task_id='stg_moex_securities_trading',
        bash_command=f"cd /opt/airflow/dbt && dbt run --select stg_moex_securities_trading"
    )

    t4_dim_moex_securities_trading = BashOperator(
        task_id='dim_moex_securities_trading',
        bash_command=f"cd /opt/airflow/dbt && dbt run --select dim_moex_securities_trading"
    )

    t5_finish = EmptyOperator(task_id='finish')

    (
        t1_start >>
        t2_api_moex_securities_trading >>
        t3_stg_moex_securities_trading >>
        t4_dim_moex_securities_trading >>
        t5_finish
    )
