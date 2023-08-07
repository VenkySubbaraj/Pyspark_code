import sys
import os
import csv
import json
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions

import aws_glue_etl_init
import aws_glue_etl_configfile

import aws_athena_api
import aws_crawler_api
import aws_glue_api
import aws_s3_api

    
  

def config_read(s3_client, glue, athena, bucket, key):
    
    csv_file = s3_client.get_object(Bucket=bucket, Key=key)
    csv_data = csv_file['Body'].read().decode('utf-8').splitlines(True)
    reader = csv.DictReader(csv_data)      
    csv_id = 0
    for row in reader:
        csv_id = csv_id + 1
        arg = {} 
        config ={}        
        usecase = row['usecase']
        routine_operation = row['routine_operation']
        execution = row['execution']
        parameters = json.loads(row['parameters'])
        keylist = parameters.keys()

        if execution.lower() == 'y' :
           print("csv " + str(csv_id) + "--------------------------------")    
           print(parameters)            
           print("------------------------------------")    

        if routine_operation == 'crawler_create' and execution.lower() == 'y' :  
           response = aws_crawler_api.crawler_create(glue,parameters)
           #response = crawler_create(glue,parameters)
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" -----------------------------------------------------------")  

        if routine_operation == 'crawler_create_chatbot_universal' and execution.lower() == 'y' :  
           response = aws_crawler_api.crawler_create_chatbot_universal(glue,parameters)
           #response = crawler_create(glue,parameters)
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" -----------------------------------------------------------")             
        if routine_operation == 'crawler_create_taxonomy_ts_data' and execution.lower() == 'y' :  
           response = aws_crawler_api.crawler_create_taxonomy_ts_data(glue,parameters)
           #response = crawler_create(glue,parameters)
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" -----------------------------------------------------------")             

        if routine_operation == 'crawler_update_schedule' and execution.lower() == 'y' :  
           response = aws_crawler_api.crawler_update_schedule(glue,parameters)
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" -----------------------------------------------------------") 	

        if routine_operation == 'crawler_update' and execution.lower() == 'y' :  
           response = aws_crawler_api.crawler_update(glue,parameters)
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" ============================================================")  

        if routine_operation == 'crawler_delete' and execution.lower() == 'y' :  
           response = aws_crawler_api.crawler_delete(glue,parameters['name'])
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" ============================================================")  

        if routine_operation == 'crawler_start' and execution.lower() == 'y' :  
           response = aws_crawler_api.crawler_start(glue,parameters['name'])
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" ============================================================")   		   

        if routine_operation == 'copy_object' and execution.lower() == 'y' :  
           response = aws_s3_api.s3_copy_object(s3_client, parameters)   
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" -----------------------------------------------------------")
           
        if routine_operation == 'delete_object' and execution.lower() == 'y' :  
           response = aws_s3_api.s3_delete_object(s3_client, parameters)   
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" -----------------------------------------------------------")     
           
        if routine_operation == 'glue_create_job' and execution.lower() == 'y' :  
           response = aws_glue_api.glue_create_job(glue, parameters)   
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" -----------------------------------------------------------") 

        if routine_operation == 'glue_create_job_pythonshell' and execution.lower() == 'y' :  
           response = aws_glue_api.glue_create_job_pythonshell(glue, parameters)   
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" -----------------------------------------------------------") 		   

        if routine_operation == 'glue_delete_job' and execution.lower() == 'y' :  
           response = aws_glue_api.glue_delete_job(glue, parameters['name'])   
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" -----------------------------------------------------------")              
           
           
        if routine_operation == 'glue_job_scheduled' and execution.lower() == 'y' :  
           response = aws_glue_api.glue_create_trigger_scheduled(glue, parameters)   
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" -----------------------------------------------------------")      
           
        if routine_operation == 'glue_delete_trigger' and execution.lower() == 'y' :  
           response = aws_glue_api.glue_delete_trigger(glue, parameters['name'])   
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" -----------------------------------------------------------")             
           
        if routine_operation == 'delete_object_recursive' and execution.lower() == 'y' :  
           response = aws_s3_api.s3_bucket_delete_recursive(s3_client, parameters['source_bucket'], parameters['source_key'])   
           print("  < response : " + routine_operation  + " >-----------------------------")
           print(response)
           print(" -----------------------------------------------------------") 

        if routine_operation == 'rds_stop' and execution.lower() == 'y' :  
           sync_command = f"aws rds stop-db-instance --db-instance-identifier {parameters['rds_instance']} --region us-east-1"
           os.system(sync_command)
           print(" -----------------------------------------------------------") 

        if routine_operation == 'database_ddl' and execution.lower() == 'y' :

           if 'sql_query' in keylist:
              query_string = parameters['sql_query']
           else:
              data = s3_client.get_object(Bucket=parameters['bucket'], Key=parameters['key'])
              query_string = data['Body'].read()
              query_string = query_string.decode("utf-8").replace("\\r"," ").replace("\\n", " ")
              
           athena_db = aws_athena_api.athena_execution(athena, parameters['database'], query_string, parameters['sql_output'], 'Y')
           print(" ============================================================")
        
        if routine_operation == 'database_ddl_custdlfs' and execution.lower() == 'y' :

           if 'sql_query' in keylist:
              query_string = parameters['sql_query']
           else:
              data = s3_client.get_object(Bucket=parameters['bucket'], Key=parameters['key'])
              query_string = data['Body'].read()
              query_string = query_string.decode("utf-8").replace("\\r"," ")
              
           athena_db = aws_athena_api.athena_execution(athena, parameters['database'], query_string, parameters['sql_output'], 'Y')
           print(" ============================================================")        

    return config     
    
def main():
    """ Main
    """
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'region', 'config_bucket', 'config_key'])
    
    print("dasdi_aws_deployment process started -> ", datetime.today().strftime('%Y-%m-%d-%H:%M:%S')   )
    print("Job parameters : ", str(args))
    
    spark, glue_context, job, s3_client, s3_resource, glue, athena = aws_glue_etl_init.create_context(args['region'])

    config_parameter = config_read(s3_client, glue, athena, args['config_bucket'], args['config_key'])

    #config_parameter = aws_glue_etl_configfile.read_config(s3_client, args['config_bucket'], args['config_key'])    

    
    print("-------")    
    print(config_parameter)

if __name__ == "__main__":
    main()
