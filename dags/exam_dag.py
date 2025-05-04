from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from train_model import prepare_data, compute_model_score, train_and_save_model
import datetime
import requests
import os
import json
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor

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

def prepare_data_dict(path_to_data):
    X, y = prepare_data(path_to_data)
    return X.to_dict(), y.to_dict()

def compute_and_push_model_score(task_instance, model, model_name):
    X, y = prepare_data('/app/clean_data/fulldata.csv')
    task_instance.xcom_push(
        key=f"{model_name}_result",
        value=compute_model_score(model, X, y)
    )

def train_and_save_best_model(task_instance):
    score_lr = task_instance.xcom_pull(
            key="linear_regression_result",
            task_ids='model_tasks.cross_val_linear'
        )
    score_dt = task_instance.xcom_pull(
            key="decision_tree_result",
            task_ids='model_tasks.cross_val_decision_tree'
        )
    score_rf = task_instance.xcom_pull(
            key="random_forest_result",
            task_ids='model_tasks.cross_val_random_forest'
        )
    best_score = min(score_lr, score_dt, score_rf)
    X, y = prepare_data('/app/clean_data/fulldata.csv')
    os.makedirs('/app/clean_data', exist_ok=True)
    if best_score == score_lr:
        train_and_save_model(
            LinearRegression(),
            X,
            y,
            '/app/clean_data/best_model.pickle'
        )
    elif best_score == score_dt:
        train_and_save_model(
            DecisionTreeRegressor(),
            X,
            y,
            '/app/clean_data/best_model.pickle'
        )
    else:
        train_and_save_model(
            RandomForestRegressor(),
            X,
            y,
            '/app/clean_data/best_model.pickle'
        )


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

    with TaskGroup("model_tasks") as model_tasks:
        compute_linear_regression_score = PythonOperator(
        task_id='cross_val_linear',
        python_callable=compute_and_push_model_score,
        op_kwargs={
            'model': LinearRegression(),
            'model_name': 'linear_regression'
        }
        )

        compute_decision_tree_regression_score = PythonOperator(
        task_id='cross_val_decision_tree',
        python_callable=compute_and_push_model_score,
        op_kwargs={
            'model': DecisionTreeRegressor(),
            'model_name': 'decision_tree'
        }
        )

        compute_random_forest_regression_score = PythonOperator(
        task_id='cross_val_random_forest',
        python_callable=compute_and_push_model_score,
        op_kwargs={
            'model': RandomForestRegressor(),
            'model_name': 'random_forest'
        }
        )       

    train_save_task = PythonOperator(
            task_id=f'train_and_save_best_model_task',
            python_callable=train_and_save_best_model
        ) 

    api_request_task >> [agg_data_task_dashboard, agg_data_task_model]
    agg_data_task_model >> model_tasks
    model_tasks >> train_save_task