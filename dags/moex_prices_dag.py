from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import nest_asyncio; nest_asyncio.apply()

import sys
import os

# Add the `scripts` directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from api_model_python.MoexData import MoexData
from api_model_python.emitents import get_emitents
from api_model_python.plugins.ReplicationClickHouseOperator import ReplicationClickHouseOperator

with DAG(
    dag_id='moex_prices',
    description='A pipeline with downloading prices of shares trading on MOEX',
    start_date=datetime(2024, 12, 27),
    schedule="0 0 * * *",
    catchup=False,
) as dag:

    t1_start = EmptyOperator(task_id='start')

    with TaskGroup('moex_api') as t2_moex_api:

        t1_start_moex_api = EmptyOperator(task_id='start_moex_api')

        moex_data = MoexData()

        with TaskGroup('moex_securities_trading') as t2_moex_securities_trading:

            t1_start_moex_securities_trading = EmptyOperator(task_id='start_moex_securities_trading')

            t2_api_moex_securities_trading = PythonOperator(
                task_id='api_moex_securities_trading',
                python_callable=moex_data.get_securities_trading)

            t3_stg_moex_securities_trading = BashOperator(
                task_id='stg_moex_securities_trading',
                bash_command=f"cd /opt/airflow/dbt && dbt run --select stg_moex_securities_trading",
                trigger_rule="none_skipped")

            t4_finish_moex_securities_trading = EmptyOperator(
                task_id='finish_moex_securities_trading',
                trigger_rule='all_done')

            (
                t1_start_moex_securities_trading >>
                t2_api_moex_securities_trading >>
                t3_stg_moex_securities_trading >>
                t4_finish_moex_securities_trading
            )

        with TaskGroup('moex_boards') as t2_1_moex_boards:

            t1_start_moex_boards = EmptyOperator(task_id='start_moex_boards')

            t2_api_moex_boards = PythonOperator(
                task_id='api_moex_boards',
                python_callable=moex_data.get_boards)

            t3_stg_moex_boards = BashOperator(
                task_id='stg_moex_boards',
                bash_command=f"cd /opt/airflow/dbt && dbt run --select stg_moex_boards")

            t4_dim_moex_boards = BashOperator(
                task_id='dim_moex_boards',
                bash_command=f"cd /opt/airflow/dbt && dbt run --select dim_moex_boards")

            t5_finish_moex_boards = EmptyOperator(
                task_id='finish_moex_boards',
                trigger_rule='all_done')

            (
                t1_start_moex_boards >>
                t2_api_moex_boards >>
                t3_stg_moex_boards >>
                t4_dim_moex_boards >>
                t5_finish_moex_boards
            )

        with TaskGroup('moex_prices') as t2_2_moex_prices:

            t1_start_moex_prices = EmptyOperator(
                task_id='start_moex_prices',
                trigger_rule='all_done')

            t2_api_moex_prices = PythonOperator(
                task_id='api_moex_prices',
                python_callable=moex_data.get_prices)

            t3_stg_moex_prices = BashOperator(
                task_id='stg_moex_prices',
                bash_command=f"cd /opt/airflow/dbt && dbt run --select stg_moex_prices")

            t4_finish_moex_prices = EmptyOperator(
                task_id='finish_moex_prices',
                trigger_rule='all_done')

            (
                t1_start_moex_prices >>
                t2_api_moex_prices >>
                t3_stg_moex_prices >>
                t4_finish_moex_prices
            )

        t3_finish_moex_api = EmptyOperator(task_id='finish_moex_api')

        (
            t1_start_moex_api >>
            [t2_moex_securities_trading, t2_1_moex_boards >> t2_2_moex_prices] >>
            t3_finish_moex_api
        )

    with TaskGroup('emitents') as t2_emitents:

        t1_start_rudata_emitents = EmptyOperator(task_id='start_emitents')

        t2_api_rudata_emitents = PythonOperator(
            task_id='api_rudata_emitents',
            python_callable=get_emitents)

        t3_stg_rudata_emitents = BashOperator(
            task_id='stg_rudata_emitents',
            bash_command=f"cd /opt/airflow/dbt && dbt run --select stg_rudata_emitents")

        t4_finish_rudata_emitents = EmptyOperator(
            task_id='finish_rudata_emitents',
            trigger_rule='all_done')

        (
            t1_start_rudata_emitents >>
            t2_api_rudata_emitents >>
            t3_stg_rudata_emitents >>
            t4_finish_rudata_emitents
        )

    t3_1_replication_moex_boards = ReplicationClickHouseOperator(
        task_id='replication_moex_boards',
        filename='moex_boards')

    t3_2_dim_emitents = BashOperator(
        task_id='dim_emitents',
        bash_command=f"cd /opt/airflow/dbt && dbt run --select dim_emitents",
        trigger_rule='all_done')

    t4_1_replication_emitents = ReplicationClickHouseOperator(
        task_id='replication_emitents',
        filename='emitents')

    t4_2_dim_moex_securities_trading = BashOperator(
        task_id='dim_moex_securities_trading',
        bash_command=f"cd /opt/airflow/dbt && dbt run --select dim_moex_securities_trading")

    t5_1_replication_moex_securities_trading = ReplicationClickHouseOperator(
        task_id='replication_moex_securities_trading',
        filename='moex_securities_trading')

    t5_2_dim_moex_securities = BashOperator(
        task_id='dim_moex_securities',
        bash_command=f"cd /opt/airflow/dbt && dbt run --select dim_moex_securities")

    t6_1_replication_moex_securities = ReplicationClickHouseOperator(
        task_id='replication_moex_securities',
        filename='moex_securities')

    t6_2_fct_moex_prices = BashOperator(
        task_id='fct_moex_prices',
        bash_command=f"cd /opt/airflow/dbt && dbt run --select fct_moex_prices")

    t7_replication_moex_prices = ReplicationClickHouseOperator(
        task_id='replication_moex_prices',
        filename='moex_prices')

    t8_finish = EmptyOperator(task_id='finish')

    t1_start >> t2_moex_api
    t1_start >> t2_emitents
    [t2_moex_api, t2_emitents] >> t3_1_replication_moex_boards
    [t2_moex_api, t2_emitents] >> t3_2_dim_emitents
    t3_2_dim_emitents >> t4_1_replication_emitents
    t3_2_dim_emitents >> t4_2_dim_moex_securities_trading
    t4_2_dim_moex_securities_trading >> t5_1_replication_moex_securities_trading
    t4_2_dim_moex_securities_trading >> t5_2_dim_moex_securities
    t5_2_dim_moex_securities >> t6_1_replication_moex_securities
    t5_2_dim_moex_securities >> t6_2_fct_moex_prices
    t6_2_fct_moex_prices >> t7_replication_moex_prices >> t8_finish
