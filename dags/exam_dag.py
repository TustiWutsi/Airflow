from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import datetime
import requests
import os
import json

def request_api(city_name):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
    "q": city_name,
    "appid": '89a03ead634e44c5afea080d6eaed1d7',
    "units": "metric"  # pour des températures en °C
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        current_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        os.makedirs('/opt/airflow/raw_files', exist_ok=True)
        with open(f'/opt/airflow/raw_files/{city_name}_{current_datetime}.json', 'w') as f:
            json.dump(response.json(), f)
    else:
        print(f"Erreur {response.status_code} : {response.text}")

cities = json.loads(Variable.get("cities"))

with DAG(
    dag_id='exam_dag',
    schedule_interval=None,
    tags=['tutorial', 'datascientest'],
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0)
    },
    catchup=False
) as my_dag:

    api_request_tasks = []
    for city in cities:
        task = PythonOperator(
            task_id=f'request_for_{city}',
            python_callable=request_api,
            op_kwargs={'city_name': city}
        )
        api_request_tasks.append(task)