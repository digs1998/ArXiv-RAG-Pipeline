# dags/test_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

dag = DAG(
    'test_imports_dag',
    default_args=default_args,
    description='Test DAG to debug imports',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
)

def test_basic_python(**context):
    """Test basic Python functionality"""
    print("✅ Basic Python task working")
    print(f"Python version: {sys.version}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Python path: {sys.path}")
    return "success"

def test_file_access(**context):
    """Test file system access"""
    print("Testing file system access...")
    
    # Check if ingestion folder exists
    ingestion_path = "/opt/airflow/"
    if os.path.exists(ingestion_path):
        print(f"✅ {ingestion_path} exists")
        files = os.listdir(ingestion_path)
        print(f"Files in ingestion: {files}")
    else:
        print(f"❌ {ingestion_path} does not exist")
    
    # Check data folder
    data_path = "/opt/airflow/data"
    if os.path.exists(data_path):
        print(f"✅ {data_path} exists")
        files = os.listdir(data_path)
        print(f"Files in data: {files}")
    else:
        print(f"❌ {data_path} does not exist")
    
    return "success"

def test_imports(**context):
    """Test importing your modules"""
    print("Testing imports...")
    sys.path.append('/opt/airflow')
    
    try:
        import ingestion
        print("✅ ingestion module imported")
    except Exception as e:
        print(f"❌ Failed to import ingestion: {e}")
        return "failed"
    
    try:
        from ingestion import pipeline
        print("✅ ingestion.pipeline imported")
    except Exception as e:
        print(f"❌ Failed to import ingestion.pipeline: {e}")
    
    try:
        from ingestion import db
        print("✅ ingestion.db imported")
    except Exception as e:
        print(f"❌ Failed to import ingestion.db: {e}")
    
    return "success"

# Define tasks
test_python_task = PythonOperator(
    task_id='test_basic_python',
    python_callable=test_basic_python,
    dag=dag,
)

test_files_task = PythonOperator(
    task_id='test_file_access',
    python_callable=test_file_access,
    dag=dag,
)

test_imports_task = PythonOperator(
    task_id='test_imports',
    python_callable=test_imports,
    dag=dag,
)

bash_test_task = BashOperator(
    task_id='bash_test',
    bash_command='echo "Bash task working" && ls -la /opt/airflow/ && python --version',
    dag=dag,
)

# Set dependencies
test_python_task >> bash_test_task >> test_files_task >> test_imports_task