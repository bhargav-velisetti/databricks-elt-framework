from env_setup import * 
import json
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
import pandas as pd
import requests  
from pyspark.sql.functions import explode_outer


add_modules_path()

from common_pyspark_libs.session_builder import session_builder
from common_pyspark_libs.schema_utility import build_schema_from_file, standarize_df_columns
from common_python_libs.util import validate_ing_format
from  common_pyspark_libs.df_io_util import *


###########UPDATE THE SAMPLE SCHEMA WITH APPROPRIATE SCHEMAS WHILE CALLING API################
#history
'{ "runtime":"local" , "api_url":"http://api.weatherapi.com/v1/history.json", "headers":"", "params":"{\"key\":\"9a0689402a904493937164632251101\",\"q\":\"76205\",\"dt\":\"2024-12-21\"}", "dbx_table_format" : "delta" , "dbx_table_location" : "D:\\Selectiva\\databricks-elt-framework\\ingestion" , "dbx_table_schema": "D:\\Selectiva\\databricks-elt-framework\\ingestion\\sample_schema.json", "outputmode" : "append", "trigger_type" : "availableNow","column_mapping_path": "D:\\Selectiva\\databricks-elt-framework\\ingestion\\column_mapping.json"}'
#current
'{ "runtime":"local" , "api_url":"http://api.weatherapi.com/v1/current.json", "headers":"", "params":"{\"key\":\"9a0689402a904493937164632251101\",\"q\":\"76205\"}", "dbx_table_format" : "delta" , "dbx_table_location" : "D:\\Selectiva\\databricks-elt-framework\\ingestion" , "dbx_table_schema": "D:\\Selectiva\\databricks-elt-framework\\ingestion\\sample_schema.json", "outputmode" : "append", "trigger_type" : "availableNow","column_mapping_path": "D:\\Selectiva\\databricks-elt-framework\\ingestion\\column_mapping.json"}'
#Forecast
'{ "runtime":"local" , "api_url":"http://api.weatherapi.com/v1/forecast.json", "headers":"", "params":"{\"key\":\"9a0689402a904493937164632251101\",\"q\":\"76205\",\"days\":\"1\"}", "dbx_table_format" : "delta" , "dbx_table_location" : "D:\\Selectiva\\databricks-elt-framework\\ingestion" , "dbx_table_schema": "D:\\Selectiva\\databricks-elt-framework\\ingestion\\sample_schema.json", "outputmode" : "append", "trigger_type" : "availableNow","column_mapping_path": "D:\\Selectiva\\databricks-elt-framework\\ingestion\\column_mapping.json"}'



def main():
    # Read arguments from command line
    cmdln_arg = json.loads(sys.argv[1])

    api_url = cmdln_arg.get('api_url')
    headers = cmdln_arg.get('headers', {})
    params = cmdln_arg.get('params', {})
    dbx_table_schema = cmdln_arg.get('dbx_table_schema')
    dbx_table_format = cmdln_arg.get('dbx_table_format')
    dbx_table_location = cmdln_arg.get('dbx_table_location')
    outputmode = cmdln_arg.get('outputmode')
    trigger_type = cmdln_arg.get('trigger_type')
    runtime = cmdln_arg.get('runtime')
    column_mapping_path=cmdln_arg.get('column_mapping_path')

    import os
    os.environ["PYSPARK_PYTHON"] = "python"

     # Initialize Spark session
    conf = SparkConf()
    spark = session_builder(conf)
    
    if runtime == 'local':
        conf.set("spark.pyspark.python", "python")  # or "python3" depending on your system
        conf.set("spark.executor.memory", "4g")


    # Validate ingestion format
    if not validate_ing_format("api"):
        raise ValueError("Unsupported ingestion format: API")

    # Fetch raw data from API
    print(f"Fetching data from API: {api_url}")
    raw_data = fetch_api_data(api_url, headers, params)

    # Load schema
    if dbx_table_schema != "":
        # if user provide schmea file path then we will use it else we will generate it.\
        schema = build_schema_from_file(dbx_table_schema)
        print("read schema from file")
    else:
        # we will generate the schema
        pass

    #this will convert nested json data to a initial df where it might not flatten all the columns.    
    raw_data=spark.read.option("multiline", "true").schema(schema).json(spark.sparkContext.parallelize([raw_data]))

    
    flattened_data = flatten(raw_data)

    #Column mapping 
    with open(column_mapping_path, 'r') as file:
        column_mapping = json.load(file)
    for new_name, old_name in column_mapping.items():
        flattened_data = flattened_data.withColumnRenamed(old_name.replace(".", "_"), new_name)

    # saving to local system by converting it into pandas df as i dont have hadoop in my laptop
    pandas_df = flattened_data.toPandas()
    pandas_df.to_csv("flattened.csv", index=False)
    print("saved to")
    print()
    

    flattened_data = standarize_df_columns(flattened_data)
    flattened_data.printSchema()




    # Write data to raw table
 #   flattened_data.write.format(dbx_table_format).mode(outputmode).option("path", dbx_table_location).save()

if __name__ == "__main__":
    main()
    print("API Ingestion Job Completed")




