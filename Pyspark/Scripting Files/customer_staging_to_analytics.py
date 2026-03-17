from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AnalyticsProcessing").enableHiveSupport().getOrCreate()

# Read tables
staging_df = spark.table("staging.customer_data")
ref_df = spark.table("raw.country_reference")

# Join
final_df = staging_df.join(
    ref_df,
    staging_df.country_id == ref_df.country_id,
    "left"
)

# Transformation
final_df = final_df.select(
    "id",
    "name",
    "country_name",
    "update_time"
)

# Write to analytics database
final_df.write \
    .mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:mysql://host:3306/analytics_db") \
    .option("dbtable", "customer_analytics") \
    .option("user", "username") \
    .option("password", "password") \
    .save()