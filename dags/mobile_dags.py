from datetime import datetime, timedelta
from airflow import DAG
import subprocess
import os
import logging
from airflow.operators.python import PythonOperator

# Cấu hình logger
logger = logging.getLogger('mobile_aws_pipeline')
logger.setLevel(logging.INFO)

# Biến toàn cục
file_postfix = datetime.now().strftime("%Y%m%d")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 28),  # Ngày trong quá khứ
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# dag_folder = os.path.dirname(os.path.abspath(__file__))
# project_folder = os.path.dirname(dag_folder)
# scripts_folder = os.path.join(project_folder, 'scripts')

scripts_folder = "/opt/airflow/scripts"

# Định nghĩa DAG
dag = DAG(
    'mobile_aws_pipeline',
    default_args=default_args,
    description='A simple mobile AWS pipeline',
    schedule_interval='@daily',  # Dễ đọc hơn
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
    try:
        crawler_path = os.path.join(scripts_folder, 'crawl_data.py')
        if not os.path.exists(crawler_path):
            raise FileNotFoundError(f"Crawler script not found at {crawler_path}")
        
        logger.info(f"Running crawler script: {crawler_path}")
        result = subprocess.run(
            ['python', crawler_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode != 0:
            logger.error(f"Crawler script failed with error: {result.stderr}")
            raise RuntimeError(f"Crawler script failed with error: {result.stderr}")
        
        logger.info(f"Crawler script executed successfully, output: {result.stdout}")
    except Exception as e:
        logger.error(f"Error running crawler script: {e}")
        raise

# Tạo task cho DAG
crawler_task = PythonOperator(
    task_id='run_crawler_data',
    python_callable=run_crawler_data,
    dag=dag
)