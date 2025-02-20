from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

import nest_asyncio; nest_asyncio.apply()

import sys
import os

# Add the `scripts` directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from api_model_python.MoexData import MoexData
from api_model_python.plugins.ReplicationClickHouseOperator import ReplicationClickHouseOperator

with DAG(
    dag_id='moex_securities_info',
    description='A pipeline with downloading info of securities traded on MOEX',
    start_date=datetime(2024, 2, 21),
    schedule="0 3 1 * *",
    catchup=False,
) as dag:

    t1_start = EmptyOperator(task_id='start')

    with TaskGroup('api_moex_securities_info') as t2_moex_securities_info:

        t1_start_moex_securities_info = EmptyOperator(task_id='start_moex_securities_info')

        t2_api_moex_securities_info = PythonOperator(
            task_id='api_moex_securities_info',
            python_callable=MoexData().get_securities_info)

        t3_stg_moex_securities_info = BashOperator(
            task_id='stg_moex_securities_info',
            bash_command=f"cd /opt/airflow/dbt && dbt run --select stg_moex_securities_info",
            trigger_rule="none_skipped")

        t4_hst_moex_securities_info = BashOperator(
            task_id='hst_moex_securities_info',
            bash_command=f"cd /opt/airflow/dbt && dbt run --select hst_moex_securities_info",
            trigger_rule="none_skipped")

        t5_finish_moex_securities_info = EmptyOperator(
            task_id='finish_moex_securities_info',
            trigger_rule='all_done')

        (
            t1_start_moex_securities_info >>
            t2_api_moex_securities_info >>
            t3_stg_moex_securities_info >>
            t4_hst_moex_securities_info >>
            t5_finish_moex_securities_info
        )

    t3_1_replication_moex_securities_info = ReplicationClickHouseOperator(
        task_id='replication_moex_securities_info',
        filename='moex_securities_info')

    t3_2_dim_moex_securities_info = BashOperator(
        task_id='dim_moex_securities_info',
        bash_command=f"cd /opt/airflow/dbt && dbt run --select dim_moex_securities_info",
        trigger_rule='all_done')

    t4_finish = EmptyOperator(task_id='finish')

    t1_start >> t2_moex_securities_info
    t2_moex_securities_info >> t3_1_replication_moex_securities_info
    t2_moex_securities_info >> t3_2_dim_moex_securities_info
    [t3_1_replication_moex_securities_info, t3_2_dim_moex_securities_info] >> t4_finish
