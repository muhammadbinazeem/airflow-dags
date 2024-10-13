from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def get_data(**kwargs):
    import requests
    import pandas as pd

    url = 'https://raw.githubusercontent.com/airscholar/ApacheFlink-SalesAnalytics/main/output/new-output.csv'
    responce = requests.get(url)

    if responce.status_code == 200:
        df = pd.read_csv(url, header=None, name=['Category', 'Price', 'Quality'])

        print('shape of data frame : ', df.shape)

        json_data = df.to_json(orient='records')
        kwargs['ti'].xcom_push(key='data', value=json_data)
    else:
        raise Exception(f'Failed to get data, HTTP status code : {responce.status_code}')

def preview_data(**kwargs):
    import pandas as pd
    import json
    
    output_data = kwargs['ti'].xcom_pull(key='data', task_ids='get_data')
    print(output_data)
    if output_data:
        output_data = json.loads(output_data)
    else:
        raise ValueError('No data received from xcom!')

    df = pd.DataFrame(output_data)
    df['Total'] = df['Price'] * df['Quality']
    df = df.groupby('Category', as_index=False).agg({'Quality': 'sum', 'Total': 'sum'})
    df = df.sort_values(by='Total', ascending=False)
    print(df[['Category', 'Total']].head(20))

default_args = {
    'owner': 'azeem',
    'start_date': datetime(2024, 10, 13),
    'catchup': False
}

dag = DAG(
    dag_id='fetch_and_preview',
    default_args = default_args,
    schedule=timedelta(days=1)
)

get_data_from_url = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag
)

preview_data_from_url = PythonOperator(
    task_id='preview_data',
    python_callable=preview_data,
    dag=dag
)

get_data_from_url >> preview_data_from_url