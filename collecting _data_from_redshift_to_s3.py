import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Read command line arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Set up the GlueContext and SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define Redshift connection options
redshift_options = {
    "url": "jdbc:redshift://your-redshift-cluster-url:5439/your-database",
    "dbtable": "your_table",
    "user": "your-username",
    "password": "your-password",
    "redshiftTmpDir": "s3://your-s3-temp-directory/"
}

# Define S3 output options
s3_output_path = "s3://your-s3-bucket/your-output-directory/"

# Read data from Redshift
datasource = glueContext.create_dynamic_frame.from_catalog(
    database = "your_glue_catalog_database",
    table_name = "your_glue_catalog_table",
    redshift_tmp_dir = redshift_options["redshiftTmpDir"],
    transformation_ctx = "datasource"
)

# Write data to S3
glueContext.write_dynamic_frame.from_options(
    frame = datasource,
    connection_type = "s3",
    connection_options = {"path": s3_output_path},
    format = "parquet",
    format_options = {"compression": "SNAPPY"},
    transformation_ctx = "datasink"
)

# Commit the job
job.commit()
