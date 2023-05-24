dyf = glueContext.create_dynamic_frame.from_catalog(database='event', table_name = 'raw_parquet')
dyf.printSchema()

df = dyf.toDF()
df.show()

def flatten(df):
    complex_fields = dict([(field.name, field.dataType) for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    while len(complex_fields)!=0:
        col_name = list(complex_fields.keys())[0]
        print("processing")

        if(type(complex_fields[col_name]) == StructType):
            expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in complex_fields[col_name]]]
            df = df.select("*",*expanded).drop(col_name)
        
        elif(type(complex_fields[col_name]) == ArrayType):
            df = df.withColumn(col_name,explode_outer(col_name))

        complex_fields = dict([(field.name, field.dataType) for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
        
    return df

df_flat = flatten(df)
df_flat.printSchema()
df_flat.repartition(1).write.parquet("s3://path")
