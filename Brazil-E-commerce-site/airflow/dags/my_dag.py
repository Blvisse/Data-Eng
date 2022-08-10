from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

dag=DAG('test_bash',description='Testing airflow bash command',schedule_interval='10 * * * *',start_date=datetime(2020,1,1),catchup=False)

python_task=PythonOperator(task_id='python_task',python_callable=lambda:print('Hello World now'),dag=dag)

python_task_2=PythonOperator(task_id='python_task_2',python_callable=lambda:print('Hello World now 2'),dag=dag)


python_task >> python_task_2 