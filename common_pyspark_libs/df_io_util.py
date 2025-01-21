from pyspark.sql import SparkSession, DataFrameReader, DataFrame
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter 
from pyspark.sql.functions import col, explode, explode_outer

import json
import requests  

def read_json(spark, file_path, schema, column_mapping_path):
    raw_data = spark.read.option("multiline", "true")
    if schema:
        raw_data = raw_data.schema(schema)
    raw_data = raw_data.json(file_path)

    if schema is None:
        schema = raw_data.schema

    if is_nested_schema(schema):
        print("Flattening nested JSON schema...")
        raw_data = flatten(raw_data)

    if column_mapping_path:
        print("Mapping columns")
        with open(column_mapping_path, 'r') as file:
            column_mapping = json.load(file)
        for new_name, old_name in column_mapping.items():
            raw_data = raw_data.withColumnRenamed(old_name.replace(".", "_"), new_name)

    return raw_data

def read_csv(spark, file_path, schema, header, column_mapping_path,delimiter):
    reader = spark.read.format("csv").option("header", header).option("inferSchema", not schema).option("delimiter", delimiter)
    if schema:
        reader = reader.schema(schema)
    raw_data = reader.load(file_path)

    return raw_data

def read_parquet(spark, file_path, schema):
    raw_data = spark.read.format("parquet")
    if schema:
        raw_data = raw_data.schema(schema)
    raw_data = raw_data.load(file_path)
    return raw_data

def read_avro(spark, file_path, schema):
    raw_data = spark.read.format("avro")
    if schema:
        raw_data = raw_data.schema(schema)
    raw_data = raw_data.load(file_path)
    return raw_data

def read_text(spark, file_path):
    return spark.read.text(file_path)


def process_file(spark, file_path, file_format, schema, header, column_mapping_path,delimiter):
    if file_format == "json":
        return read_json(spark, file_path, schema, column_mapping_path)
    elif file_format == "csv":
        return read_csv(spark, file_path, schema, header, column_mapping_path,delimiter)
    elif file_format == "parquet":
        return read_parquet(spark, file_path, schema)
    elif file_format == "avro":
        return read_avro(spark, file_path, schema)
    elif file_format == "text":
        return read_text(spark, file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


    
def is_nested_schema(schema):
    return any(isinstance(field.dataType, StructType) for field in schema.fields)

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
from common_pyspark_libs.dbx_tbl_util import chk_tbl_exists_local


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

def df_stream_wrt(runtime : str, df : DataFrame, dbx_table_format : str , dbx_table_location : str,outputmode : str , trigger_type : str):
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


def df_wrt_local(spark : SparkSession, runtime , df :  DataFrame, dbx_table_location, outputmode , dbx_catalog, dbx_db, dbx_table, dbx_table_format):
    if chk_tbl_exists_local(spark, f'{dbx_table_location}\\data\\'):
        df.write.format(dbx_table_format).mode(outputmode).save(f'{dbx_table_location}\\data\\')
    else:
        df.write.format(dbx_table_format).mode(outputmode).option("path", f'{dbx_table_location}\\data\\').saveAsTable(f'{dbx_table}')

def df_wrt_aws():
    pass

def df_wrt_gcp():
    pass

def df_wrt_azure():
    pass 

def  df_wrt(spark : SparkSession, runtime , df :  DataFrame, dbx_table_location, outputmode , dbx_catalog, dbx_db, dbx_table, dbx_table_format):
    if runtime == 'local':
        df_wrt_local(spark, runtime , df, dbx_table_location, outputmode , dbx_catalog, dbx_db, dbx_table, dbx_table_format)
        print(f' Data loaded into Table {dbx_catalog}.{dbx_db}.{dbx_table} successfully')

    else:
        raise ValueError(f'Runtime {runtime} not supported yet.')

def df_strm_files_rdr(spark : SparkSession, options_dict : dict):
    '''
    df = spark.readStream.format("json").options(**options_dict).load()
    return df
    '''
    pass 


def df_files_rdr(spark : SparkSession, options_dict : dict , path ,format , schema):
    df = spark.read.options(**options_dict).load(path=path, format=format, schema=schema)
    return df