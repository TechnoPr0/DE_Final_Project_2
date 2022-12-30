from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(dag_id = "load_stocks", start_date=datetime(2022, 12, 26), schedule='00 9 * * *') as dag:
    bash_task = BashOperator (
        task_id = "start_script", 
        bash_command="python3  /opt/airflow/dags/tasks/main.py")
    data_output = BashOperator(
        task_id = "output", 
        bash_command = "python3 /opt/airflow/dags/tasks/data_output.py")

bash_task >> data_output

    