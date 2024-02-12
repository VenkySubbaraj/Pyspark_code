import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Initialize Spark context and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Set your Redshift parameters
jdbc_url = "jdbc:redshift://your-redshift-cluster.amazonaws.com:5439/your_database"
redshift_username = "your_username"
redshift_password = "your_password"
redshift_table = "your_table"

# Read data from Redshift
redshift_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", redshift_table) \
    .option("user", redshift_username) \
    .option("password", redshift_password) \
    .load()

# Convert DataFrame to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(redshift_df, glueContext, "redshift_dynamic_frame")

# Write DynamicFrame to S3
s3_output_path = "s3://your-bucket-name/path/to/output/"
glueContext.write_dynamic_frame.from_options(
    frame = dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": s3_output_path},
    format = "parquet"
)
