# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure Redshift connection properties
hostname = "<redshift-hostname>"
port = "<redshift-port>"
database = "<redshift-database>"
user = "<redshift-user>"
password = "<redshift-password>"
table = "<redshift-table>"

# Configure S3 output path
s3_output_path = "s3://<s3-bucket>/<s3-path>"

# Create SparkSession
spark = SparkSession.builder.appName("Extract_Redshift_Data").getOrCreate()

# Load data from Redshift into PySpark DataFrame
df = spark.read \
  .format("com.databricks.spark.redshift") \
  .option("url", f"jdbc:redshift://{hostname}:{port}/{database}") \
  .option("user", user) \
  .option("password", password) \
  .option("dbtable", table) \
  .load()

# Save the PySpark DataFrame to S3 in Parquet format
df.write \
  .format("parquet") \
  .option("compression", "snappy") \
  .option("path", s3_output_path) \
  .option("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
  .option("spark.hadoop.fs.s3a.access.key", "<aws-access-key>") \
  .option("spark.hadoop.fs.s3a.secret.key", "<aws-secret-key>") \
  .option("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
  .option("spark.hadoop.fs.s3a.fast.upload", "true") \
  .option("spark.sql.parquet.compression.codec", "snappy") \
  .mode("overwrite") \
  .save()

# Stop SparkSession
spark.stop()
