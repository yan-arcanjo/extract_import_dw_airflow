from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import yaml
from global_modules.operators import SqlServerOperator


def get_config():
    with open("config.yml") as stream:
        return yaml.safe_load(stream)

yaml_data = get_config()
dw_truncate_stage = yaml_data["dw_commands"]["truncate_stage"]
dw_insert_bronze = yaml_data["dw_commands"]["insert_bronze"]

tz = pendulum.timezone("America/Sao_Paulo")

DAG_ID = 'extract_import_dag'

default_args = {
    "owner": "yan.arcanjo",
    "start_date": pendulum.datetime(year=2022, month=8, day=18).astimezone(tz),
    "email_on_failure": False,
    "email_on_success": False,
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=5),
    "retries": 1,
}

with DAG(
    DAG_ID,
    description="Carga de dados teste",
    default_args=default_args,
    schedule="20 9,18 * * *",
    catchup=False,
    tags=["extract","sql_server", "postgresql"],
    max_active_runs=1,
) as dag:
     
    truncate_stage = PostgresOperator(
        task_id='truncate_stage',
        sql=dw_truncate_stage['sql'],
        postgres_conn_id=dw_truncate_stage['conn'],
        params={'source': dw_truncate_stage['source']}
    )

    extraction = SqlServerOperator(
        task_id='extraction',
        source_conn_id='source_conn',
        sql_path='sql_files/extract_query.sql',
        filename='extration_file.csv',
        target_conn_id='target_conn',
        target_table='database.stage.table'
    )   

    insert_stage_bronze = PostgresOperator(
        task_id='insert_stage_bronze',
        sql=dw_insert_bronze['sql'],
        postgres_conn_id=dw_insert_bronze['conn'],
        params={'source': dw_insert_bronze['source'], 'target': dw_insert_bronze['target']}
    )

    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="extract_import_silver",  # Nome da DAG a ser acionada
    )

    (truncate_stage >> extraction >> insert_stage_bronze >> trigger_dag)


