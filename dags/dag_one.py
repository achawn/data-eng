import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import zipfile
import os
import sqlite3

def CSVToJson():
    conn=sqlite3.connect('sqlite/db.db')
    if pd.read_sql_query('SELECT COUNT(*) FROM crime', conn).iloc[0, 0] > 0:
        return
    with zipfile.ZipFile('data/crime_data_2020_present.zip', 'r') as zip:
        for file in zip.namelist():
            print(f"- {file}")
        with zip.open('Users/andrewhawn/Downloads/Crime_Data_from_2020_to_Present.csv') as data_file:
            df=pd.read_csv(data_file)

    df.to_sql(name='crime', con=conn, if_exists='replace', index=False)
    return df.columns.tolist()

def describe():
    conn=sqlite3.connect('sqlite/db.db')
    return pd.read_sql_query('SELECT * FROM crime', conn).describe()

default_args = {
    'owner': 'andrew',
    'start_date': dt.datetime(2020, 3, 18),
    'max_active_runs': 1,
    'catchup': False
}

with DAG(
    'CSVToJsonDAG',
    start_date=dt.datetime(2022, 12, 1),
    catchup=False,
    max_active_runs=1,
    schedule="0 12 * * *"
) as dag:

    starting=BashOperator(
        task_id='starting',
        bash_command='echo "reading csv..."'
    )

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

    describe=PythonOperator(
        task_id='describeData',
        python_callable=describe
    )

    starting >> list_dir >> csvjson >> describe
