import datetime
import json
import pendulum
import pandas as pd
import logging
import csv

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

POSTGRES_CONN = "postgres_conn"


with DAG(
    dag_id='growth_etl',
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    template_searchpath=['/opt/ingest_scripts/','/opt/source_data/']
) as dag:
    start = EmptyOperator(
        task_id='start',
    )
    test_script = BashOperator(
        task_id = 'test_bash',
        bash_command = 'ls -la /opt/ingest_scripts/'
    )
    pre_ingest_scripts = PostgresOperator(
        task_id = 'create_schemas',
        postgres_conn_id = POSTGRES_CONN,
        sql = "/create_schemas.sql"
    )
    ingest_data = PostgresOperator(
        task_id = 'ingest_data',
        postgres_conn_id = POSTGRES_CONN,
        sql = "/ingest_data.sql"
    )
    
start >> pre_ingest_scripts >> test_script >> ingest_data