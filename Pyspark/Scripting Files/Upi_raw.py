from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructField,StructType,StringType,DataType,TimestampType,IntegerType,DoubleType,BooleanType

spark = SparkSession.builder.appName("Raw_upi").getOrCreate()

# Read files

Schema = StructType([
    StructField("Sr_no",IntegerType(),True),
    StructField("Transisition_id",StringType(),True),
    StructField("Source_upi_id",StringType(),True),
    StructField("Target_upi_id",StringType(),True),
    StructField("Transistion_time", TimestampType(),True),
    StructField("Transistion_date" ,TimestampType(),True),
    StructField("UPi refrence number",StringType(),True),
    StructField("Status",StringType(),True),
    StructField("Sender_bank_name", StringType(),True),
    StructField("Reciver_bank_name" ,StringType(),True),
    StructField("reciver_account_number",IntegerType(),True)
])

file_path= $1

Df1 = spark.read.schema(Schema).parquet(file_path,header = True, Inferschema =  False )

Df1.write.format("bigquery")\
                .option("table","project_id.Raw_layer.Upi_transistion")\
                .option("partitionField","Transistion_date")\
                .option("partitionType","Day")\
                .option("clustering_field","Transisition_id,Source_upi_id")\
                .save()


task_instance = context['task_instance']

sql= f"""insert into airflow_backend.data_load_audit (file_path, task_id, status,Load_type) 
    values ('{file_path}', '{task_instance.task_id}', 'success','raw_layer')"""

task_instance = context['task_instance']
client = storage.Client()

with client.query(sql) as query_job:
    results = query_job.execute(sql)
