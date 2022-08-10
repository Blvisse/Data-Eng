from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


with DAG('late_shipment',description='Returns list of orders where the seller missed the carrier delivery deadline',schedule_interval='10 * * * *',start_date=datetime(2020,1,1),catchup=False) as dag:
    requirements_operator=BashOperator(task_id='requirements_operator',bash_command='pip install -r /opt/airflow/dags/scripts/requirements.txt')
    s3_download_operator=BashOperator(task_id='s3_download_operator',bash_command='python3 /opt/airflow/dags/scripts/s3_download.py')
    spark_missed_deadline_operator =BashOperator(task_id='spark_missed_deadline_operator',bash_command='python3 /opt/airflow/dags/scripts/spark_missed_deadline.py')
    
    requirements_operator>>s3_download_operator >> spark_missed_deadline_operator
    
    s3_upload_operator=BashOperator(task_id='s3_upload_operator',bash_command='python3 python3 /opt/airflow/dags/scripts/s3_upload.py')
    
    spark_missed_deadline_operator >> s3_upload_operator
    
    
    