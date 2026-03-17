from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructField,StructType,StringType,DataType,TimestampType,IntegerType,DoubleType,BooleanType

spark = SparkSession.builder.appName("Raw_upi").enableHiveSupport().getOrCreate()

# Read files

Schema = StructType([
    StructField("Sr_no",IntegerType(),True),
    StructType("Transisition_id",StringType(),True),
    StructType("Source_upi_id",StringType(),True),
    StructType("Target_upi_id",StringType(),True),
    StructType("Transistion_time", TimestampType(),True),
    StructType("Transistion_date" ,DateTime(),True),
    StructType("UPi refrence number",Stringtype(),True),
    StructType("Status",StringType(),True,),
    StructType("Sender_bank_name", StringType(),True),
    StructType("Reciver_bank_name" ,StringType(),True),
    StructType("reciver_account_number",IntegerType(),True)
])

Df1 = spark.read.schema(Schema).parquet("path",header = True, Inferschema =  False )

Df2 = Df1.withColumn("m_reciver_account_number",
                     concat(lit("XXXX"),
                            substring(col("reciver_account_number"),5,9),
                            lit("XXX")
                     ))

Df3 = Df2.withColumn("Update_date_time",current_timestamp()).\
                    ("Create_date_time"current_timestamp()).\
                    ("Service_provider",lit("Phonepay"))

Df4 =  Df3.filter(col("Status")=="Completed")

Df5 = Df3.filter(col("Status")=="In_progress" or col("Status") == "Failed")

Df4.write.format("bigquery")\
                .option("table","project_id.transfomed_layer.Upi_completed")\
                .option("partitionField","Date_column")\
                .option("partitionType","Day")\
                .option("clustering_field","col1,Col2")\
                .save()

Df5.write.format("bigquery")\
                .option("table","project_id.transfomed_layer.Upi_Failed")\
                .option("partitionField","Date_column")\
                .option("partitionType","Day")\
                .option("clustering_field","col1,Col2")\
                .save()

    