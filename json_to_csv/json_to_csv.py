from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, explode_outer, expr
import json
import csv
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

def flatten(df):
    complex_fields = dict([(field.name, field.dataType) for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print("processing")

        if type(complex_fields[col_name]) == StructType:
            expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)
        
        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))

        complex_fields = dict([(field.name, field.dataType) for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])

    return df


spark = SparkSession.builder.appName("example").getOrCreate()
f = open('./Sample_data.json')
data = json.load(f)
json_data = data

df = spark.read.json(spark.sparkContext.parallelize([json_data]))
flattened_df = flatten(df)
columns = flattened_df.columns
output_directory = "output_columns"
flattened_df.write.csv('output.csv', header=True, mode='overwrite')
