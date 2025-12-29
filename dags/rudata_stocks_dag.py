from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from src.sources.Moex.Moex import Moex
from src.sources.Rudata.RuDataMethod import Account, Emitents, MoexStocks
from utils.DbtOperator import DbtOperator
from utils.ReplicationClickHouseOperator import ReplicationClickHouseOperator

with DAG(
    dag_id='rudata_stocks',
    description='Dwonloading data from Rudata: stocks on Moex, russian emitents',
    start_date=datetime(2024, 12, 27),
    schedule="10 0 * * *",
    catchup=False,
) as dag:

    t0_start = EmptyOperator(task_id='start')

    Account()

    with TaskGroup('api') as t2_api:

        t1_api_rudata_stocks = PythonOperator(
            task_id='api_rudata_stocks',
            python_callable=MoexStocks().get_df)

        t2_api_rudata_emitents = PythonOperator(
            task_id='api_rudata_emitents',
            python_callable=Emitents().get_df)

        t1_api_rudata_stocks >> t2_api_rudata_emitents

    with TaskGroup('dbt') as t3_dbt:

        t1_rudata_stocks = DbtOperator(
            task_id='rudata_stocks',
            snapshot='rudata_stocks')

        t2_rudata_stocks = DbtOperator(
            task_id='rudata_emitents',
            snapshot='rudata_emitents')

        t1_rudata_stocks >> t2_rudata_stocks

    finish = EmptyOperator(task_id='finish')

    (
        t0_start >>
        t2_api >>
        t3_dbt >>
        finish
    )
