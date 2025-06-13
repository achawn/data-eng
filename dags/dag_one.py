import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import zipfile
import os

def CSVToJson():
    with zipfile.ZipFile('data/crime_data_2020_present.zip', 'r') as zip:
        for file in zip.namelist():
            print(f"- {file}")
        with zip.open('Users/andrewhawn/Downloads/Crime_Data_from_2020_to_Present.csv') as data_file:
            df=pd.read_csv(data_file)

    return df
    #df=pd.read_CSV('crime_data_2020_present.csv')
    # for i,r in df.iterrows():
    #     print(r['name'])
    # df.to_JSON('fromAirflow.JSON',orient='records')

default_args = {
    'owner': 'andrew',
    'start_date': dt.datetime(2020, 3, 18),
    #'retries': 1,
    'max_active_runs': 1,
    'catchup': False
    #'retry_delay': dt.timedelta(minutes=5),
}

with DAG(
    'CSVToJsonDAG',
    #default_args=default_args,
    start_date=dt.datetime(2022, 12, 1),
    catchup=False,
    max_active_runs=1,
    schedule="0 12 * * *"
) as dag:

    starting=BashOperator(
        task_id='starting',
        bash_command='echo "reading csv..."'
    )

    # list_dir=BashOperator(
    #     task_id='listing',
    #     bash_command='ls -a'
    # )
    def list_call():
        return os.listdir()
    list_dir=PythonOperator(
        task_id='listing',
        python_callable=list_call
    )

    csvjson=PythonOperator(
        task_id='convertCSVtoJson',
        python_callable=CSVToJson
    )

    starting >> list_dir >> csvjson
