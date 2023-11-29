import os
import subprocess
import shlex
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 27), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl-rekdat',
    default_args=default_args,
    description='ETL for stock historical data',
    schedule_interval='30 06 * * *',
)

def run_script():
    venv_activation_script = "/home/aufarhmn/Documents/Semester 5/Rekayasa Data/stock-historical-data/etlrekdat/bin/activate"
    script_path = "/home/aufarhmn/Documents/Semester 5/Rekayasa Data/stock-historical-data/main.py"

    venv_activation_script_quoted = shlex.quote(venv_activation_script)
    script_path_quoted = shlex.quote(script_path)

    command = f"source {venv_activation_script_quoted} && python3 {script_path_quoted}"

    if os.name == 'posix':
        subprocess.run(command, shell=True, executable="/bin/bash")
    elif os.name == 'nt':
        subprocess.run(command, shell=True, executable="cmd.exe")

run_etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_script,
    dag=dag,
)

run_etl_task 
