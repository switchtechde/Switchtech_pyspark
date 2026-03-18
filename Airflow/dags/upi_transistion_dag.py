#Dag Should Run hourly 
from airflow import DAG 
from airflow.operators import PythonOperator
from airflow.operators import EmptyOperator
from airflow.operators import BashOperator
from datetime import datetime

from Utils.upi_utils import check_for_file

with DAG(
    dag_id = "Upi_transition",
    schedule_interval = '0 * * * *', #or @hourly
    max_retries = 2,
    start_date = datetime(2025,1,5),
) as dag :
    
    File_check = PythonOperator(
        task_id = "File_check",
        python_callable = check_for_file,
        op_kwargs = {"bucket_name": "your-bucket-name"},
        xcom_push=True
    )

    Skipfile = EmptyOperator(
        task_id = "SkipFile",
    )
    
    RawLayer_load = BashOperator(
        task_id ="RawLayer_load",
        bash_command = "spark-submit C:\Users\joshi\OneDrive\Desktop\Airflow\Upi_raw.py",
        op_kwargs = {"RAW_LAYER_FILE_PATH": "{{ task_instance.xcom_pull(task_ids='File_check', key='RAW_LAYER_FILE_PATH') }}"},
        xcom_pull=True
    )

"""

File_check >> RawLayer_load
File_check >> Skipfile"""