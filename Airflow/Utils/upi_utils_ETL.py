import logging
from google.cloud import storage
from matplotlib.style import context 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_for_file(bucket_name):
    task_instance = context['task_instance']
    client = storage.Client()
    bucket_name = bucket_name
    File_path = f"gs://{bucket_name}/path/upi_transistion/parquet/*.parquet"
    File_path= "XYZ"
    task_to_execute = ""

    sql= f"SELECT * FROM airflow_backend.data_load_audit WHERE file_path = '{File_path}'"

    with client.query(sql) as query_job:
        logger.info(f"Executing query: {sql}")
        results = query_job.execute(sql)
        query_results = results.fetchone()  # 1 or 0 
        if query_results:
            logger.info(f"File found: {query_results}")
            task_to_execute = "SkipFile"
        else:
            task_to_execute = "RawLayer_load"
    
    task_instance.xcom_push(key="RAW_LAYER_FILE_PATH",value=File_path)

    return task_to_execute