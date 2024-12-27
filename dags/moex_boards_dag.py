from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import sys
import os

# Add the `scripts` directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from api_model_python.api_moex_boards import api_moex_boards


with DAG(
    dag_id='moex_boards',
    description='A pipeline with downloading MOEX boards',
    start_date=datetime(2024, 12, 27),
    schedule="0 3 * * *",
    catchup=False,
) as dag:

    t1_start = EmptyOperator(task_id='start')

    t2_api_moex_boards = PythonOperator(
        task_id='api_moex_boards',
        python_callable=api_moex_boards
    )

    t3_stg_moex_boards = BashOperator(
        task_id='stg_moex_boards',
        bash_command=f"cd /opt/airflow/dbt && dbt run --select stg_moex_boards"
    )

    t4_dim_moex_boards = BashOperator(
        task_id='dim_moex_boards',
        bash_command=f"cd /opt/airflow/dbt && dbt run --select dim_moex_boards"
    )

    t5_finish = EmptyOperator(task_id='finish')

    (
        t1_start >>
        t2_api_moex_boards >>
        t3_stg_moex_boards >>
        t4_dim_moex_boards >>
        t5_finish
    )
