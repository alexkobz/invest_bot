from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

from utils.ReplicationClickHouseOperator import ReplicationClickHouseOperator
from utils.DbtOperator import DbtOperator
from src.sources.Moex.Moex import Moex


with DAG(
    dag_id='moex_prices',
    description='A pipeline with downloading every trading day prices of shares trading on MOEX',
    start_date=datetime(2024, 12, 27),
    schedule="15 0 * * *",
    catchup=False,
) as dag:

    moex = Moex()

    t1_start = EmptyOperator(task_id='start')

    t2_api_moex_prices = PythonOperator(
        task_id='api_moex_prices',
        python_callable=moex.getHistoryStockSharesSecurities)
    
    t3_hst_moex_prices = DbtOperator(
        task_id='hst_moex_prices',
        model='hst_moex_prices')
    
    t4_fct_moex_prices = DbtOperator(
        task_id='fct_moex_prices',
        model='fct_moex_prices')

    finish = EmptyOperator(task_id='finish')

    (
        t1_start >>
        t2_api_moex_prices >>
        t3_hst_moex_prices >>
        t4_fct_moex_prices >>
        finish
    )
