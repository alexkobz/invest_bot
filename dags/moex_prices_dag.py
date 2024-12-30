from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import sys
import os

# Add the `scripts` directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from api_model_python.api_moex_prices import api_moex_prices


with DAG(
    dag_id='moex_prices',
    description='A pipeline with downloading prices of shares trading on MOEX',
    start_date=datetime(2024, 12, 27),
    # schedule="0 3 * * *",
    catchup=False,
) as dag:

    t1_start = EmptyOperator(task_id='start')

    t2_api_moex_prices = PythonOperator(
        task_id='api_moex_prices',
        python_callable=api_moex_prices
    )

    t3_stg_moex_prices = BashOperator(
        task_id='stg_moex_prices',
        bash_command=f"cd /opt/airflow/dbt && dbt run --select stg_moex_prices"
    )

    t4_dim_moex_securities = BashOperator(
        task_id='dim_moex_prices',
        bash_command=f"cd /opt/airflow/dbt && dbt run --select dim_moex_securities"
    )

    t5_fct_moex_prices = BashOperator(
        task_id='fct_moex_prices',
        bash_command=f"cd /opt/airflow/dbt && dbt run --select fct_moex_prices"
    )

    t6_finish = EmptyOperator(task_id='finish')

    (
        t1_start >>
        t2_api_moex_prices >>
        t3_stg_moex_prices >>
        t4_dim_moex_securities >>
        t5_fct_moex_prices >>
        t6_finish
    )
