# DAG qui commence par une tâche pouvant échouer de manière aléatoire.
# Si la tâche échoue, une tâche task2 doit être exécutée.
# Si la tâche est réussie, une tâche task3 doit être exécutée.
# Dans tous les cas, une tâche task4 doit être lancée après.

import random
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime

def successful_task():
    print('success')


def failed_task():
    raise Exception('This task did not work!')


def random_fail_task():
    random.seed()
    a = random.random() 
    if a < .9:
        raise Exception('This task randomly failed')

with DAG(
    dag_id='fork1_dag',
    tags=['tutorial', 'datascientest'],
    schedule_interval=datetime.timedelta(seconds=10),
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1)
    },
    catchup=False
) as my_dag:

    task1 = PythonOperator(
        task_id='task1',
        python_callable=random_fail_task
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=successful_task,
        trigger_rule='all_failed'
    )

    task3 = PythonOperator(
        task_id='task3',
        python_callable=successful_task,
        trigger_rule='all_success'
    )

    task4 = PythonOperator(
        task_id='task4',
        python_callable=successful_task,
        trigger_rule='all_done'
    )

    task1 >> [task2, task3]
    [task2, task3] >> task4