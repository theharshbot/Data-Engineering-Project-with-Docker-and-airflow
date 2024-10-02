import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


#sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from pipeline import extract_wikipedia_data, transform_wikipedia_data, write_wikipedia_data


dag=DAG(
    dag_id="football_wikipedia_flow",
    default_args={
        "owner": "airflow",
        "start_date":datetime(2024,1,1)
    },
    schedule_interval=None,
    catchup=False
)


extract_task = PythonOperator(
    task_id= "extract_data_from_wikipedia",
    python_callable=extract_wikipedia_data,
    #provide_context= True,
    op_kwargs={"url":"https://en.wikipedia.org/wiki/List_of_Premier_League_clubs"},
    dag=dag
)


transform_task = PythonOperator(
    task_id="transform_wikipedia_data",
    python_callable=transform_wikipedia_data,
    #provide_context=True,
    dag=dag
)


write_task = PythonOperator(
    task_id="write_wikipedia_data",
    #provide_context=True,
    python_callable=write_wikipedia_data,
    #op_kwargs={"file_name": "wikipedia_football_clubs.csv"},
    dag=dag
)



extract_task >> transform_task >> write_task

