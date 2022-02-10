
import shutil
import time
from datetime import datetime,timedelta
# from pprint import pprint

from airflow import DAG
from airflow.decorators import task

from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

from proccess_data import dim_product
from proccess_data import fact_review

with DAG(
    dag_id='workflowetl',
    schedule_interval="@daily",    
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['workflowetl'],
) as dag:

    meta_Musical_Instruments = PythonOperator(
        task_id="meta_Musical_Instruments",
        python_callable = dim_product
        )

    reviews_Musical_Instruments = PythonOperator(
        task_id="reviews_Musical_Instruments",
        python_callable = fact_review
        )

    # email = EmailOperator(
    # 	task_id = 'send_email',
    # 	to='elnaz.ghasemi64@gmail.com',
    # 	subject='Daily report generated',
    # 	html_content=""" <h1>Congratulations! Your store reports are ready.</h1> """,
    #     	)

    meta_Musical_Instruments >> reviews_Musical_Instruments # >> email



      