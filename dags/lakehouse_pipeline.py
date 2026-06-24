from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Enterprise Standard: Default DAG Arguments
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': True, # Alerts the team on failure
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'fintech_risk_lakehouse_etl',
    default_args=default_args,
    description='Automated execution of Silver, QC, and Gold layers',
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['fintech', 'lakehouse', 'critical'],
) as dag:

    # The static path mounted inside the Airflow Docker container
    CONTAINER_SCRIPT_DIR = '/opt/airflow/project'

    # 1. Silver Layer Task
    run_silver = BashOperator(
        task_id='process_silver_layer',
        bash_command=f'cd {CONTAINER_SCRIPT_DIR} && python silver_cleansing.py',
    )

    # 2. Data Quality Gate Task
    run_quality_gate = BashOperator(
        task_id='data_quality_gate',
        bash_command=f'cd {CONTAINER_SCRIPT_DIR} && python quality_gate.py',
    )

    # 3. Gold Layer Task
    run_gold = BashOperator(
        task_id='process_gold_layer',
        bash_command=f'cd {CONTAINER_SCRIPT_DIR} && python gold_aggregations.py',
    )

    # Enterprise Standard: Define the strict execution order
    run_silver >> run_quality_gate >> run_gold