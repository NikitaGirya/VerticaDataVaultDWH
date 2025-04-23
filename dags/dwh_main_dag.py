from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.utils.task_group import TaskGroup

import boto3
from datetime import datetime, timedelta


def fetch_s3_file(bucket: str, key: str):

    AWS_ENDPOINT_URL = Variable.get('AWS_ENDPOINT_URL')
    AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')

    session = boto3.session.Session()

    s3_client = session.client(
        service_name='s3',
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    try:
        s3_client.download_file(
            Bucket=bucket,
            Key=f'{key}.csv',
            Filename=f'/data/{key}.csv'
        )
    except Exception as e:
        print(f"Error downloading file {key}: {e}")
        raise


default_args = {
    'owner': 'ngirya',
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'dwh_main_dag',
    default_args=default_args,
    schedule_interval=None,
)

with dag:

    files_tables = ['users', 'groups', 'dialogs', 'group_log']

    with TaskGroup('fetch_s3_file') as fetch_s3_file_group:

        for file in files_tables:

            fetch_s3_file_task = PythonOperator(
                task_id=f'fetch_s3_{file}_file',
                python_callable=fetch_s3_file,
                op_kwargs={'bucket': 'sprint6', 'key': file}
            )

    with TaskGroup('stg_load') as stg_load_group:

        for table in files_tables:

            with open('/lessons/ddl_scripts/stg_load.sql') as f:
                stg_load_sql = f.read().format(table_name=table)

            stg_load_task = VerticaOperator(
                task_id=f'load_{table}',
                vertica_conn_id='VERTICA_CONN',
                sql=stg_load_sql
            )

    with TaskGroup('dwh_hub_load') as dwh_hub_load_group:

        for hub in ['h_dialogs', 'h_groups', 'h_users']:

            with open(f'/lessons/ddl_scripts/{hub}.sql') as f:
                hub_load_sql = f.read()

            hub_load_task = VerticaOperator(
                task_id=f'load_{hub}',
                vertica_conn_id='VERTICA_CONN',
                sql=hub_load_sql
            )

    with TaskGroup('dwh_link_load') as dwh_link_load_group:

        for link in [
            'l_admins',
            'l_groups_dialogs',
            'l_user_message',
            'l_user_group_activity']:

            with open(f'/lessons/ddl_scripts/{link}.sql') as f:
                link_load_sql = f.read()

            link_load_task = VerticaOperator(
                task_id=f'load_{link}',
                vertica_conn_id='VERTICA_CONN',
                sql=link_load_sql
            )

    with TaskGroup('dwh_sat_load') as dwh_sat_load_group:

        for sat in [
            's_admins',
            's_dialog_info',
            's_group_name',
            's_group_private_status',
            's_user_chatinfo',
            's_user_socdem',
            's_auth_history']:

            with open(f'/lessons/ddl_scripts/{sat}.sql') as f:
                sat_load_sql = f.read()

            sat_load_task = VerticaOperator(
                task_id=f'load_{sat}',
                vertica_conn_id='VERTICA_CONN',
                sql=sat_load_sql
            )

fetch_s3_file_group >> stg_load_group
stg_load_group >> dwh_hub_load_group
dwh_hub_load_group >> dwh_link_load_group
dwh_link_load_group >> dwh_sat_load_group