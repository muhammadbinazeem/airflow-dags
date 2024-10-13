from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'azeem',
    'start_date': datetime(2024, 10, 13),
    'catchup': False
}

dag = DAG(
    dag_id='hello_world',
    default_args = default_args,
    schedule=timedelta(days=1)
)

t1 = BashOperator(
    task_id='hello_world', 
    bash_command='echo "hello world"',
    dag=dag
)

t2 = BashOperator(
    task_id='hello_dml',
    bash_command='echo "hello azeem"',
    dag=dag
)

t1 >> t2