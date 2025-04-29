from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
file_postfix = datetime.now().strftime("%Y%m%d")

default_args = {
    'owner': "Truong Tien Anh",
    'start_date': datetime(2025, 4, 26)
}

dag = DAG(
    dag_id = 'etl_mobile_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['mobile', 'etl', 'pipeline']
)
# extract = PythonOperator(
#     task_id='reddit_extraction',
#     python_callable=file_postfix,
#     op_kwargs={
#         'file_name': f'reddit_{file_postfix}',
#         'subreddit': 'dataengineering',
#         'time_filter': 'day',
#         'limit': 100
#     },
#     dag=dag
# )
# extract