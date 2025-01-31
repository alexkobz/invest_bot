from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import sys
import os


# Add the `scripts` directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from api_model_python.GirboData import GirboData
from api_model_python.plugins.ReplicationClickHouseOperator import ReplicationClickHouseOperator


with DAG(
    dag_id='girbo_fundamentals',
    description='A pipeline with downloading fundamentals.sql from https://bo.nalog.ru/',
    start_date=datetime(2025, 1, 12),
    schedule=None,
    catchup=False,
) as dag:

    t1_start = EmptyOperator(task_id='start')

    girbo_data = GirboData()

    with TaskGroup('organizations_cards') as t2_organizations_cards:

        t1_start_organizations_cards = EmptyOperator(task_id='start_organizations_cards')

        t2_api_girbo_organizations_cards = PythonOperator(
            task_id='api_girbo_organizations_cards',
            python_callable=girbo_data.get_organizations_cards,
            execution_timeout=None)

        t3_stg_girbo_organizations_cards = BashOperator(
            task_id='stg_girbo_organizations_cards',
            bash_command=f"cd /opt/airflow/dbt && dbt run --select stg_girbo_organizations_cards",
            trigger_rule="none_skipped")

        t4_dim_girbo_organizations_cards = BashOperator(
            task_id='dim_girbo_organizations_cards',
            bash_command=f"cd /opt/airflow/dbt && dbt run --select dim_girbo_organizations_cards",
            trigger_rule="none_skipped")

        t5_finish_organizations_cards = EmptyOperator(
            task_id='finish_organizations_cards',
            trigger_rule='all_done')

        (
            t1_start_organizations_cards >>
            t2_api_girbo_organizations_cards >>
            t3_stg_girbo_organizations_cards >>
            t4_dim_girbo_organizations_cards >>
            t5_finish_organizations_cards
        )

    t3_download_fundamentals = PythonOperator(
        task_id='download_fundamentals',
        python_callable=girbo_data.download_fundamentals,
        trigger_rule="none_skipped")

    with TaskGroup('parse_fundamentals') as t4_parse_fundamentals:

        t1_start_parse_fundamentals = EmptyOperator(task_id='start_parse_fundamentals')

        t2_parse_fundamentals = PythonOperator(
            task_id='parse_fundamentals',
            python_callable=girbo_data.parse_fundamentals)

        t3_stg_girbo_fundamentals = BashOperator(
            task_id='stg_girbo_fundamentals',
            bash_command=f"cd /opt/airflow/dbt && dbt run --select stg_girbo_fundamentals")

        t4_hst_girbo_fundamentals = BashOperator(
            task_id='hst_girbo_fundamentals',
            bash_command=f"cd /opt/airflow/dbt && dbt run --select hst_girbo_fundamentals")

        t5_fct_fundamentals = BashOperator(
            task_id='fct_fundamentals',
            bash_command=f"cd /opt/airflow/dbt && dbt run --select fct_fundamentals")

        t6_finish_parse_fundamentals = EmptyOperator(task_id='finish_parse_fundamentals')

        (
            t1_start_parse_fundamentals >>
            t2_parse_fundamentals >>
            t3_stg_girbo_fundamentals >>
            t4_hst_girbo_fundamentals >>
            t5_fct_fundamentals >>
            t6_finish_parse_fundamentals
        )

    t5_replication_girbo_organizations_cards = ReplicationClickHouseOperator(
        task_id='replication_girbo_organizations_cards',
        filename='girbo_organizations_cards')

    t6_replication_fundamentals = ReplicationClickHouseOperator(
        task_id='replication_fundamentals',
        filename='fundamentals')

    t7_finish = EmptyOperator(task_id='finish')

    (
        t1_start >>
        t2_organizations_cards >>
        t3_download_fundamentals >>
        t4_parse_fundamentals >>
        t5_replication_girbo_organizations_cards >>
        t6_replication_fundamentals >>
        t7_finish
    )
