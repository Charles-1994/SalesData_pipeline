from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='batch_upload_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    description="DAG for manually triggering batch_upload",
) as dag:

    # Task to install packages
    install_packages = BashOperator(
        task_id='install_packages',
        bash_command='pip install -r /workspaces/Sales_Data_app/requirements.txt',
    )

    # Task to run tests
    run_tests = BashOperator(
        task_id='run_tests',
        bash_command='pytest /workspaces/Sales_Data_app/tests',
    )

        # Task to run the batch_upload pipeline
    batch_upload = BashOperator(
        task_id='run_batch_upload',
        bash_command='batch_upload',  # This runs the entry point defined in setup.py
    )

    # Define task dependencies
    install_packages >> run_tests >> batch_upload
