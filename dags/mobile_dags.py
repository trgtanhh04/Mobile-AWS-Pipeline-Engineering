from datetime import datetime, timedelta
from airflow import DAG
import subprocess
import os
import sys
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from utils.constants import KAFKA_BOOTSTRAP_SERVERS, TOPIC_PHONE_DATA, AWS_BUCKET_NAME, AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY
from etls.aws_etl import connect_to_s3, create_bucket_if_not_exits, upload_to_s3

# Airflow Logging
logger = LoggingMixin().log

# Environment Variables
scripts_folder = os.getenv('SCRIPTS_FOLDER', "/opt/airflow/scripts")
etl_folder = os.getenv('ETL_FOLDER', "/opt/airflow/etls")
data_folder = os.getenv('DATA_FOLDER', "/opt/airflow/data")

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 28),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# Define DAG
dag = DAG(
    'mobile_aws_pipeline',
    default_args=default_args,
    description='A simple mobile AWS pipeline',
    schedule_interval='@daily',
    catchup=False
)

dag.doc_md = """
### Mobile AWS Pipeline
This pipeline runs daily to fetch mobile data using the crawler script.
"""

def run_crawler_data(**kwargs):
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
            text=True,
            timeout=1800  # Timeout for the crawler script
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
    ti = kwargs['ti']
    crawler_status = ti.xcom_pull(task_ids='run_crawler_data', key='crawler_status')
    
    if not crawler_status:
        logger.error("No status found for 'crawler_status'. Skipping ETL task.")
        raise ValueError("Crawler task did not push any status to XCom.")

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
                text=True,
                timeout=1800  # Timeout for the ETL script
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

def run_aws_etl(**kwargs):
    try:
        logger.info("Starting the AWS ETL task...")
        s3 = connect_to_s3()
        create_bucket_if_not_exits(s3, AWS_BUCKET_NAME)
        
        # Upload file to S3
        local_file_path = os.path.join(data_folder, 'ouput/raw_data.csv')
        upload_to_s3(s3, local_file_path, AWS_BUCKET_NAME, 'raw/data.csv')
        
        logger.info("AWS ETL task completed successfully.")
    except Exception as e:
        logger.error(f"Error in AWS ETL task: {e}")
        raise

# DAG Tasks
crawler_task = PythonOperator(
    task_id='run_crawler_data',
    python_callable=run_crawler_data,
    execution_timeout=timedelta(minutes=30),
    dag=dag
)

mobile_etl_task = PythonOperator(
    task_id='run_mobile_etl',
    python_callable=run_mobile_etl,
    execution_timeout=timedelta(minutes=30),
    dag=dag
)

aws_etl_task = PythonOperator(
    task_id='run_aws_etl',
    python_callable=run_aws_etl,
    execution_timeout=timedelta(minutes=30),
    dag=dag
)

crawler_task >> mobile_etl_task >> aws_etl_task