from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from utils.DbtOperator import DbtOperator
from src.sources.CBR.KeyRate import KeyRate
from src.sources.CBR.GetCursOnDate import GetCursOnDate
from src.sources.CBR.DragMetDynamic import DragMetDynamic
from src.sources.CBR.Ruonia import Ruonia
from src.sources.CBR.Bliquidity import Bliquidity
from src.sources.CBR.MainInfoXML import MainInfoXML


with DAG(
    dag_id='cbr',
    description='A pipeline with downloading cbr main indicators',
    start_date=datetime(2024, 12, 27),
    schedule="15 0 * * *",
    catchup=False,
) as dag:

    t1_start = EmptyOperator(task_id='start')

    with TaskGroup('KeyRate') as t2_KeyRate:
        t1_api_KeyRate = PythonOperator(
            task_id='api_KeyRate',
            python_callable=KeyRate().run)

        t2_dbt_KeyRate = DbtOperator(
            task_id='dbt_KeyRate',
            model='key_rate')

        t1_api_KeyRate >> t2_dbt_KeyRate

    with TaskGroup('GetCursOnDate') as t3_GetCursOnDate:
        t1_api_GetCursOnDate = PythonOperator(
            task_id='api_GetCursOnDate',
            python_callable=GetCursOnDate().run)

        t2_dbt_GetCursOnDate = DbtOperator(
            task_id='dbt_GetCursOnDate',
            model='rub_rate')

        t1_api_GetCursOnDate >> t2_dbt_GetCursOnDate

    with TaskGroup('DragMetDynamic') as t4_DragMetDynamic:
        t1_api_DragMetDynamic = PythonOperator(
            task_id='api_DragMetDynamic',
            python_callable=DragMetDynamic().run)

        t2_dbt_DragMetDynamic = DbtOperator(
            task_id='dbt_DragMetDynamic',
            model='precious_metals')

        t1_api_DragMetDynamic >> t2_dbt_DragMetDynamic

    with TaskGroup('Ruonia') as t5_Ruonia:
        t1_api_Ruonia = PythonOperator(
            task_id='api_Ruonia',
            python_callable=Ruonia().run)

        t2_dbt_ruonia = DbtOperator(
            task_id='dbt_Ruonia',
            model='ruonia')

        t1_api_Ruonia >> t2_dbt_ruonia

    with TaskGroup('Bliquidity') as t6_Bliquidity:
        t1_api_Bliquidity = PythonOperator(
            task_id='api_Bliquidity',
            python_callable=Bliquidity().run)

        t2_dbt_Bliquidity = DbtOperator(
            task_id='dbt_Bliquidity',
            model='bank_liquidity')

        t1_api_Bliquidity >> t2_dbt_Bliquidity

    with TaskGroup('MainInfoXML') as t7_MainInfoXML:
        t1_api_MainInfoXML = PythonOperator(
            task_id='api_MainInfoXML',
            python_callable=MainInfoXML().run)

        t2_dbt_MainInfoXML = DbtOperator(
            task_id='dbt_MainInfoXML',
            model='inflation')

        t1_api_MainInfoXML >> t2_dbt_MainInfoXML

    finish = EmptyOperator(task_id='finish')

    (
            t1_start >>
            t2_KeyRate >>
            t3_GetCursOnDate >>
            t4_DragMetDynamic >>
            t5_Ruonia >>
            t6_Bliquidity >>
            t7_MainInfoXML >>
            finish
    )
