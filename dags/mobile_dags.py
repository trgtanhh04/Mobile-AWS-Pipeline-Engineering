# from datetime import datetime, timedelta
# from airflow import DAG
# import subprocess
# import os
# import sys
# import logging
# from airflow.operators.python import PythonOperator

# # Cấu hình logger
# logger = logging.getLogger('mobile_aws_pipeline')
# logger.setLevel(logging.INFO)

# # Đường dẫn scripts (sử dụng biến môi trường)
# scripts_folder = os.getenv('SCRIPTS_FOLDER', "/opt/airflow/scripts")
# etl_folder = os.getenv('ETL_FOLDER', "/opt/airflow/etls")

# # Cấu hình mặc định cho DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2025, 4, 28),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # Định nghĩa DAG
# dag = DAG(
#     'mobile_aws_pipeline',
#     default_args=default_args,
#     description='A simple mobile AWS pipeline',
#     schedule_interval='@daily',
#     catchup=False,
# )

# dag.doc_md = """
# ### Mobile AWS Pipeline
# This pipeline runs daily to fetch mobile data using the crawler script.
# """

# def run_crawler_data(**kwargs):
#     """
#     Run the crawler script to fetch data.
#     """
#     ti = kwargs['ti']
#     try:
#         logger.info(f"Starting the crawler task at {datetime.now()}")
#         crawler_path = os.path.join(scripts_folder, 'crawl_data.py')
#         logger.info(f"Crawler script path: {crawler_path}")

#         if not os.path.exists(crawler_path):
#             logger.error(f"Crawler script not found at {crawler_path}. Please verify the script location.")
#             raise FileNotFoundError(f"Crawler script not found at {crawler_path}")
        
#         logger.info(f"Running crawler script: {crawler_path}")
#         result = subprocess.run(
#             [sys.executable, crawler_path],
#             stdout=subprocess.PIPE,
#             stderr=subprocess.PIPE,
#             text=True
#         )

#         logger.info("Crawler script stdout:\n" + result.stdout)
#         logger.error("Crawler script stderr:\n" + result.stderr)
#         if result.returncode != 0:
#             logger.error(f"Crawler script failed with return code {result.returncode}")
#             raise Exception(f"Crawler script failed with return code {result.returncode}")
        
#         logger.info(f"Crawler script executed successfully. Output: {result.stdout}")
#         ti.xcom_push(key='crawler_status', value='success')
#     except Exception as e:
#         logger.error(f"Error running crawler script: {e}")
#         ti.xcom_push(key='crawler_status', value='failed')
#         raise


# def run_mobile_etl(**kwargs):
#     """
#     Run the mobile ETL script.
#     """
#     ti = kwargs['ti']
#     crawler_status = ti.xcom_pull(task_ids='run_crawler_data', key='crawler_status')
    
#     if crawler_status == 'success':
#         try:
#             logger.info(f"Starting the mobile ETL task at {datetime.now()}")
#             etl_path = os.path.join(etl_folder, 'mobile_etl.py')
#             logger.info(f"ETL script path: {etl_path}")

#             if not os.path.exists(etl_path):
#                 logger.error(f"ETL script not found at {etl_path}. Please verify the script location.")
#                 raise FileNotFoundError(f"ETL script not found at {etl_path}")
            
#             logger.info(f"Running ETL script: {etl_path}")
#             result = subprocess.run(
#                 [sys.executable, etl_path],
#                 stdout=subprocess.PIPE,
#                 stderr=subprocess.PIPE,
#                 text=True
#             )

#             logger.info("ETL script stdout:\n" + result.stdout)
#             logger.error("ETL script stderr:\n" + result.stderr)
#             if result.returncode != 0:
#                 logger.error(f"ETL script failed with return code {result.returncode}")
#                 raise Exception(f"ETL script failed with return code {result.returncode}")
            
#             logger.info(f"ETL script executed successfully. Output: {result.stdout}")
#         except Exception as e:
#             logger.error(f"Error running ETL script: {e}")
#             raise
#     else:
#         logger.warning("Crawler task failed. Skipping the ETL task.")

# # Tạo task cho DAG
# crawler_task = PythonOperator(
#     task_id='run_crawler_data',
#     python_callable=run_crawler_data,
#     dag=dag
# )

# mobile_etl_task = PythonOperator(
#     task_id='run_mobile_etl',
#     python_callable=run_mobile_etl,
#     dag=dag
# )

# crawler_task >> mobile_etl_task

from datetime import datetime, timedelta
from airflow import DAG
import subprocess
import os
import sys
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

# Airflow Logging
logger = LoggingMixin().log

# Environment Variables
scripts_folder = os.getenv('SCRIPTS_FOLDER', "/opt/airflow/scripts")
etl_folder = os.getenv('ETL_FOLDER', "/opt/airflow/etls")

if not os.path.exists(scripts_folder):
    logger.error(f"Scripts folder not found: {scripts_folder}")
    raise FileNotFoundError(f"Scripts folder not found: {scripts_folder}")

if not os.path.exists(etl_folder):
    logger.error(f"ETL folder not found: {etl_folder}")
    raise FileNotFoundError(f"ETL folder not found: {etl_folder}")

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 28),
    'retries': 3,  # Increased retries
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'mobile_aws_pipeline',
    default_args=default_args,
    description='A simple mobile AWS pipeline',
    schedule_interval='@daily',
    catchup=False,
)

dag.doc_md = """
### Mobile AWS Pipeline
This pipeline runs daily to fetch mobile data using the crawler script.
"""

def run_crawler_data(**kwargs):
    """
    Run the crawler script to fetch data.
    """
    ti = kwargs['ti']
    try:
        logger.info("Starting the crawler task...")
        crawler_path = os.path.join(scripts_folder, 'crawl_data.py')

        if not os.path.exists(crawler_path):
            logger.error(f"Crawler script not found at {crawler_path}.")
            raise FileNotFoundError(f"Crawler script not found at {crawler_path}")
        
        result = subprocess.run(
            [sys.executable, crawler_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        logger.info("Crawler script stdout:\n" + result.stdout)
        logger.error("Crawler script stderr:\n" + result.stderr)
        if result.returncode != 0:
            logger.error(f"Crawler script failed with return code {result.returncode}")
            raise Exception(f"Crawler script failed with return code {result.returncode}")
        
        logger.info("Crawler script executed successfully.")
        ti.xcom_push(key='crawler_status', value='success')
    except Exception as e:
        logger.error(f"Error running crawler script: {e}")
        ti.xcom_push(key='crawler_status', value='failed')
        raise


def run_mobile_etl(**kwargs):
    """
    Run the mobile ETL script.
    """
    ti = kwargs['ti']
    crawler_status = ti.xcom_pull(task_ids='run_crawler_data', key='crawler_status')
    
    if crawler_status == 'success':
        try:
            logger.info("Starting the mobile ETL task...")
            etl_path = os.path.join(etl_folder, 'mobile_etl.py')

            if not os.path.exists(etl_path):
                logger.error(f"ETL script not found at {etl_path}.")
                raise FileNotFoundError(f"ETL script not found at {etl_path}")
            
            result = subprocess.run(
                [sys.executable, etl_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            logger.info("ETL script stdout:\n" + result.stdout)
            logger.error("ETL script stderr:\n" + result.stderr)
            if result.returncode != 0:
                logger.error(f"ETL script failed with return code {result.returncode}")
                raise Exception(f"ETL script failed with return code {result.returncode}")
            
            logger.info("ETL script executed successfully.")
        except Exception as e:
            logger.error(f"Error running ETL script: {e}")
            raise
    else:
        logger.warning("Crawler task failed. Skipping the ETL task.")

# DAG Tasks
crawler_task = PythonOperator(
    task_id='run_crawler_data',
    python_callable=run_crawler_data,
    dag=dag
)

mobile_etl_task = PythonOperator(
    task_id='run_mobile_etl',
    python_callable=run_mobile_etl,
    dag=dag
)

crawler_task >> mobile_etl_task