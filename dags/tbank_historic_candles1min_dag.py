from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from src.sources.Tbank.Candles import Candles1MinYesterday
from utils.DbtOperator import DbtOperator

with DAG(
    dag_id='tbank_historic_candles1min',
    description='A pipeline with downloading historic 1 minute candles from Tbank',
    start_date=datetime(2025, 10, 14),
    schedule="15 0 * * *",
    catchup=False,
) as dag:
    
    t1_start = EmptyOperator(task_id='start')

    t2_api_tbank_historic_candles1min= PythonOperator(
        task_id='api_tbank_historic_candles1min',
        python_callable=Candles1MinYesterday().run)
    
    t3_tbank_historic_candles1min = DbtOperator(
        task_id='dbt_tbank_historic_candles1min',
        model='tbank_historic_candles1min')

    finish = EmptyOperator(task_id='finish')
 
    (
        t1_start >>
        t2_api_tbank_historic_candles1min >>
        t3_tbank_historic_candles1min >>
        finish
    )
