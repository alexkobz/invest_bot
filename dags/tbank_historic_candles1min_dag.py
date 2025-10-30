from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from utils.DbtOperator import DbtOperator
from src.sources.Tbank.Candles import Candles1Min


with DAG(
    dag_id='tbank_historic_candles1min',
    description='A pipeline with downloading historic 1 minute candles from Tbank',
    start_date=datetime(2025, 10, 14),
    # schedule="0 0 * * *",
    schedule=None,
    catchup=False,
) as dag:
    
    t1_start = EmptyOperator(task_id='start')

    t2_api_tbank_historic_candles1min= PythonOperator(
        task_id='api_tbank_historic_candles1min',
        python_callable=Candles1Min().run)
    
    t3_tbank_historic_candles1min = DbtOperator(
        task_id='tbank_historic_candles1min',
        model='fct_tbank_historic_candles1min')

    # t4_replication_tbank_candles1min = ReplicationClickHouseOperator(
    #     task_id='replication_tbank_candles1min',
    #     filename='tbank_candles1min')

    t5_finish = EmptyOperator(task_id='finish')
 
    (
        t1_start >>
        t2_api_tbank_historic_candles1min >>
        t3_tbank_historic_candles1min >>
        # t4_replication_tbank_candles1min >>
        t5_finish
    )
