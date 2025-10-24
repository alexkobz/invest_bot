from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from src.airflow.DbtOperator import DbtOperator
from src.sources.CBR.KeyRate import KeyRate
from src.sources.CBR.GetCursOnDate import GetCursOnDate
from src.sources.CBR.DragMetDynamic import DragMetDynamic
from src.sources.CBR.Ruonia import Ruonia


with DAG(
    dag_id='cbr',
    description='A pipeline with downloading cbr main indicators',
    start_date=datetime(2024, 12, 27),
    # schedule="0 0 * * *",
    schedule=None,
    catchup=False,
) as dag:

    t1_start = EmptyOperator(task_id='start')

    with TaskGroup('KeyRate') as t2_KeyRate:
        t1_api_KeyRate = PythonOperator(
            task_id='api_KeyRate',
            python_callable=KeyRate().run)

        t2_cbr_KeyRate = DbtOperator(
            task_id='KeyRate',
            model='KeyRate')

        t1_api_KeyRate >> t2_cbr_KeyRate

    with TaskGroup('GetCursOnDate') as t3_GetCursOnDate:
        t1_api_GetCursOnDate = PythonOperator(
            task_id='api_GetCursOnDate',
            python_callable=GetCursOnDate().run)

        t2_cbr_GetCursOnDate = DbtOperator(
            task_id='GetCursOnDate',
            model='GetCursOnDate')

        t1_api_GetCursOnDate >> t2_cbr_GetCursOnDate

    with TaskGroup('DragMetDynamic') as t4_DragMetDynamic:
        t1_api_DragMetDynamic = PythonOperator(
            task_id='api_DragMetDynamic',
            python_callable=DragMetDynamic().run)

        t2_cbr_DragMetDynamic = DbtOperator(
            task_id='DragMetDynamic',
            model='DragMetDynamic')

        t1_api_DragMetDynamic >> t2_cbr_DragMetDynamic

    with TaskGroup('Ruonia') as t5_Ruonia:
        t1_api_Ruonia = PythonOperator(
            task_id='api_Ruonia',
            python_callable=Ruonia().run)

        t2_cbr_ruonia = DbtOperator(
            task_id='Ruonia',
            snapshot='Ruonia')

        t1_api_Ruonia >> t2_cbr_ruonia

    t6_finish = EmptyOperator(task_id='finish')

    (
            t1_start >>
            t2_KeyRate >>
            t3_GetCursOnDate >>
            t4_DragMetDynamic >>
            t5_Ruonia >>
            t6_finish
    )
