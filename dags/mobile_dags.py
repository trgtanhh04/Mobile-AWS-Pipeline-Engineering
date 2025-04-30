from datetime import datetime, timedelta
from airflow import DAG
import subprocess
import os
import sys
import logging
from airflow.operators.python import PythonOperator

# Cấu hình logger
logger = logging.getLogger('mobile_aws_pipeline')
logger.setLevel(logging.INFO)

# Đường dẫn scripts (sử dụng biến môi trường)
scripts_folder = os.getenv('SCRIPTS_FOLDER', "/opt/airflow/scripts")

# Cấu hình mặc định cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Định nghĩa DAG
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
        logger.info(f"Starting the crawler task at {datetime.now()}")
        crawler_path = os.path.join(scripts_folder, 'crawl_data.py')
        logger.info(f"Crawler script path: {crawler_path}")

        if not os.path.exists(crawler_path):
            logger.error(f"Crawler script not found at {crawler_path}. Please verify the script location.")
            raise FileNotFoundError(f"Crawler script not found at {crawler_path}")
        
        logger.info(f"Running crawler script: {crawler_path}")
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
        
        logger.info(f"Crawler script executed successfully. Output: {result.stdout}")
        ti.xcom_push(key='crawler_status', value='success')
    except Exception as e:
        logger.error(f"Error running crawler script: {e}")
        ti.xcom_push(key='crawler_status', value='failed')
        raise

# Tạo task cho DAG
crawler_task = PythonOperator(
    task_id='run_crawler_data',
    python_callable=run_crawler_data,
    dag=dag
)