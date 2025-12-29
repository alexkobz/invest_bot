from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from src.sources.Moex.Moex import Moex
from utils.DbtOperator import DbtOperator
from utils.ReplicationClickHouseOperator import ReplicationClickHouseOperator

with DAG(
    dag_id='moex_stock_shares',
    description='A pipeline with downloading dimension info of shares trading on MOEX',
    start_date=datetime(2024, 12, 27),
    schedule="10 0 * * *",
    catchup=False,
) as dag:

    moex = Moex()

    t1_start = EmptyOperator(task_id='start')

    with TaskGroup('moex_boards') as t2_moex_boards:

        t1_api_moex_boards = PythonOperator(
            task_id='api_moex_boards',
            python_callable=moex.getStockSharesBoards)

        t2_moex_boards_snapshot = DbtOperator(
            task_id='moex_boards_snapshot',
            snapshot="moex_boards")

        t1_api_moex_boards >> t2_moex_boards_snapshot

    with TaskGroup('moex_shares') as t3_moex_stock_shares:

        t1_api_moex_stock_shares = PythonOperator(
            task_id='api_moex_shares',
            python_callable=moex.getHistoryStockSharesSecurities)

        t2_moex_shares_snapshot = DbtOperator(
            task_id='moex_shares_snapshot',
            snapshot="moex_shares")

        t1_api_moex_stock_shares >> t2_moex_shares_snapshot

    finish = EmptyOperator(task_id='finish')

    (
        t1_start >>
        t2_moex_boards >>
        t3_moex_stock_shares >>
        finish
    )
