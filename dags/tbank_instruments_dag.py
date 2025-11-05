from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from utils.DbtOperator import DbtOperator
from src.sources.Tbank.Instruments import Bonds, Etfs, Shares


with DAG(
    dag_id='tbank_instruments',
    description='A pipeline with downloading dimension info of instruments from Tbank',
    start_date=datetime(2025, 10, 14),
    schedule="10 0 * * *",
    catchup=False,
) as dag:
    

    t1_start = EmptyOperator(task_id='start')

    with TaskGroup('bonds') as t2_bonds:

        t1_api_tbank_bonds = PythonOperator(
            task_id='api_tbank_bonds',
            python_callable=Bonds().run)
    
        t2_tbank_bonds = DbtOperator(
            task_id='tbank_bonds',
            snapshot='tbank_bonds')

        t1_api_tbank_bonds >> t2_tbank_bonds

    with TaskGroup('etfs') as t3_etfs:

        t1_api_tbank_etfs = PythonOperator(
            task_id='api_tbank_etfs',
            python_callable=Etfs().run)
    
        t2_tbank_etfs = DbtOperator(
            task_id='tbank_etfs',
            snapshot='tbank_etfs')

        t1_api_tbank_etfs >> t2_tbank_etfs

    with TaskGroup('shares') as t4_shares:

        t1_api_tbank_shares = PythonOperator(
            task_id='api_tbank_shares',
            python_callable=Shares().run)
    
        t2_tbank_shares = DbtOperator(
            task_id='tbank_shares',
            snapshot='tbank_shares')
        
        t1_api_tbank_shares >> t2_tbank_shares

    t5_finish = EmptyOperator(task_id='finish')

    (
        t1_start >>
        t2_bonds >>
        t3_etfs >>
        t4_shares >>
        t5_finish
    )
