from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer

spark = SparkSession.builder.appName("rationalize_struct").getOrCreate()

f = open('./Sample_data.json')
data = json.load(f)
print(data)
json_data = data
df = spark.read.json(spark.sparkContext.parallelize(json_data, 1))
df_batters = df.select(
    col("id"),
    col("type"),
    col("name"),
    explode_outer("batters.batter").alias("batter")
)

df_toppings = df.select(
    col("id"),
    col("type"),
    col("name"),
    explode_outer("topping").alias("topping")
)
df_rationalized = df_batters.select(
    col("id"),
    col("type"),
    col("name"),
    col("batter.id").alias("batter_id"),
    col("batter.type").alias("batter_type")
).join(
    df_toppings.select(
        col("id"),
        col("type"),
        col("name"),
        col("topping.id").alias("topping_id"),
        col("topping.type").alias("topping_type")
    ),
    ["id","type","name"],"outer"
)

df_rationalized.show(truncate=False)
csv_path = "./sample.csv"
df_rationalized.write.csv(csv_path, header=True, mode="overwrite")
csv_path = "./sample.parquet"
df_rationalized.write.parquet(csv_path, mode="overwrite")
spark.stop()
