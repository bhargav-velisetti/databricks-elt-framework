from pyspark.sql import SparkSession, DataFrameReader, DataFrame
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter 
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