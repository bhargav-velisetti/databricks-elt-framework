from pyspark.sql import SparkSession, DataFrameReader, DataFrame
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter 
from pyspark.sql.functions import col, explode, explode_outer

import json
import requests  



def fetch_api_data(url: str, headers: dict = None, params: dict = None) -> list:
    
    if isinstance(params, str):  # If params is a string, parse it into a dictionary
        params = json.loads(params)
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise ValueError(f"Failed to fetch data from {url}: {response.status_code} - {response.text}")


#Flatten array of structs and structs
def flatten(df):
    #compute Complex Fields (Lists and Structs) in Schema   
    complex_fields = dict([(field.name, field.dataType) for field in df.schema.fields if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
    while len(complex_fields)!=0:
        col_name=list(complex_fields.keys())[0]
        print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
            
        #if StructType then convert all sub element to columns.
        #i.e. flatten structs
        if (type(complex_fields[col_name]) == StructType):
            expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
            df=df.select("*", *expanded).drop(col_name)
           
        #if ArrayType then add the Array Elements as Rows using the explode function
        #i.e. explode Arrays
        elif (type(complex_fields[col_name]) == ArrayType):    
            df=df.withColumn(col_name,explode_outer(col_name))
            
        #recompute remaining Complex Fields in Schema       
        complex_fields = dict([(field.name, field.dataType)
                                for field in df.schema.fields
                                if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
    return df


def df_rdr_kafka(spark : SparkSession, options_dict : dict ):

    df = spark.readStream \
    .options(**options_dict) \
    .format("kafka") \
    .load()
    # Returning the RAW Kafka Data as a Dataframe
    return df 

def get_trigger_type(df : DataFrame,trigger_type : str) -> DataStreamWriter:
    if trigger_type == 'availableNow':
       # return df.writeStream.trigger(once=True) hint: latest spark version recommends availableNow=True
         return df.writeStream.trigger(processingTime='10 seconds')

    else:
        print(f'implement the trigger type ==> {trigger_type}')
        raise ValueError(f'Not supported trigger type {trigger_type}')

def df_stream_wrt_local(df : DataFrame, dbx_table_format : str , dbx_table_location : str ,outputmode : str , trigger_type : str):
        get_trigger_type(df,trigger_type)\
                    .format(dbx_table_format)\
                    .outputMode(outputmode)\
                    .option("path", f'{dbx_table_location}\\data\\')\
                    .option("checkpointLocation", f'{dbx_table_location}\\checkpoint\\')\
                    .start()\

def df_stream_wrt_aws():
    pass 
def df_stream_wrt_gcp():
    pass 
def df_stream_wrt_azure():
    pass 

def df_wrt_kafka(runtime : str, df : DataFrame, dbx_table_format : str , dbx_table_location : str,outputmode : str , trigger_type : str):
    if runtime == 'local':
        df_stream_wrt_local(  df ,dbx_table_format, dbx_table_location, outputmode, trigger_type)
    if runtime == 'aws':
        df_stream_wrt_aws()
    if runtime == 'gcp':
        df_stream_wrt_gcp()
    if runtime == 'azure':
        df_stream_wrt_azure()


def tranform_kafka_json_records(df : DataFrame, schema : StructType) -> DataFrame:
    df = df.select( F.from_json(df.value.cast("string"), schema=schema).alias('data'), 'timestamp' , 'topic' ).withColumn('row_insert_ts', df.timestamp).withColumn('kafka_topic', df.topic)\
    .select('data.*', 'kafka_topic', 'row_insert_ts')
    return df 

def tranform_kafka_records(df : DataFrame, schema : StructType, src_sys_format : str) -> DataFrame: 
    if src_sys_format == 'json':
        return tranform_kafka_json_records(df , schema)
    else:
        raise ValueError(f'Kafka record format {src_sys_format} is nor defined yet.')
    
def df_rdr_jdbc(spark : SparkSession, options_dict : dict):
    df = spark.read.format("jdbc").options(**options_dict).load()
    return df


def  df_wrt_jdbc(runtime , df :  DataFrame, dbx_table_location, outputmode , dbx_catalog, dbx_db, dbx_table, dbx_table_format):
    if runtime == 'local':
        df.write.format(dbx_table_format).mode(outputmode).option("path", f'{dbx_table_location}\\data\\').saveAsTable(f'{dbx_table}')
        print(f'Table {dbx_catalog}.{dbx_db}.{dbx_table} created successfully')

    else:
        raise ValueError(f'Runtime {runtime} not supported yet.')

