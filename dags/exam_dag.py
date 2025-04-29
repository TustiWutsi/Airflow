from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import datetime
import requests
import os
import json
import pandas as pd

def request_api(city_names):
    api_responses = []
    for city_name in city_names:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
        "q": city_name,
        "appid": '89a03ead634e44c5afea080d6eaed1d7',
        "units": "metric"  # pour des températures en °C
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            api_responses.append(response.json())
        else:
            print(f"Erreur {response.status_code} : {response.text}")
    current_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    os.makedirs('/app/raw_files', exist_ok=True)
    with open(f'/app/raw_files/{current_datetime}.json', 'w') as f:
        json.dump(api_responses, f)

def transform_data_into_csv(n_files=None, filename='data.csv'):
    parent_folder = '/app/raw_files'
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]
    print()

    dfs = []

    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data_temp = json.load(file)
        for data_city in data_temp:
            dfs.append(
                {
                    'temperature': data_city['main']['temp'],
                    'city': data_city['name'],
                    'pression': data_city['main']['pressure'],
                    'date': f.split('.')[0]
                }
            )

    df = pd.DataFrame(dfs)

    print('\n', df.head(10))
    os.makedirs('/app/clean_data', exist_ok=True)
    df.to_csv(os.path.join('/app/clean_data', filename), index=False)

cities = json.loads(Variable.get("cities"))

with DAG(
    dag_id='exam_dag',
    schedule_interval=None, #datetime.timedelta(seconds=20)
    tags=['tutorial', 'datascientest'],
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0)
    },
    catchup=False
) as my_dag:


    api_request_task = PythonOperator(
            task_id=f'api_request',
            python_callable=request_api,
            op_kwargs={'city_names': cities}
        )

    agg_data_task_dashboard = PythonOperator(
            task_id=f'aggregate_data_dashboard',
            python_callable=transform_data_into_csv,
            op_kwargs={'n_files': 20}
        )

    agg_data_task_model = PythonOperator(
            task_id=f'aggregate_data_model',
            python_callable=transform_data_into_csv,
            op_kwargs={'filename': 'fulldata.csv'}
        )

    api_request_task >> [agg_data_task_dashboard, agg_data_task_model]