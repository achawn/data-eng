import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

def CSVToJson():
    df=pd.read_CSV('data.csv')
    for i,r in df.iterrows():
        print(r['name'])
    df.to_JSON('fromAirflow.JSON',orient='records')

default_args = {
    'owner': 'andrew',
    'start_date': dt.datetime(2020, 3, 18),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG(
    'CSVToJsonDAG',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5)
) as dag:

    starting=BashOperator(
        task_id='starting',
        bash_command='echo "reading csv..."'
    )

    csvjson=PythonOperator(
        task_id='convertCSVtoJson',
        python_callable=CSVToJson
    )

    starting >> csvjson
