from env_setup import * 
import json
from pyspark import SparkConf
from pyspark.sql import SparkSession 
from pyspark.sql import DataFrameReader
from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.functions import from_unixtime, column , date_trunc , trunc , to_date, to_json, col , struct, monotonically_increasing_id,from_json, explode
from pyspark.sql.types import StringType, StructField, StringType, IntegerType, DateType, StructType

add_modules_path()

from  common_pyspark_libs.session_builder import *
from  common_pyspark_libs.df_io_util import *
from  common_pyspark_libs.schema_utility import * 
from  common_python_libs.util import  *

'''
'{ "runtime":"local" ,"source_system" : "kafka", "src_sys_format" : "json" ,"bootstrap_servers" : "NA", "subscribe_topic" : "test_producer01", "consumer_group_id" : "test_producer01_gp01" ,"truststore" : "NA", "truststore_password" : "NA", "keystore" : "NA", "keystore_password" : "NA" , "dbx_catalog" : "default", "dbx_db" : "default", "dbx_table" : "NA" , "dbx_table_format" : "delta" , "dbx_table_location" : "C:\\Users\\bharg\\dbx_delta_db_path\\", "dbx_table_schema": "C:\\Users\\bharg\\Documents\\dataeng\\databricks-elt-framework\\ingestion\\sample_schema.json", "outputmode" : "append", "trigger_type" : "availableNow" }'
'''

def main():

        
    cmdln_arg : dict  = json.loads(sys.argv[1])

    runtime = cmdln_arg.get('runtime')
    source_system  = cmdln_arg.get('source_system')
    src_sys_format = cmdln_arg.get('src_sys_format')
    subscribe_topic = cmdln_arg.get('subscribe_topic')
    dbx_catalog = cmdln_arg.get('dbx_catalog')
    dbx_db = cmdln_arg.get('dbx_db')
    dbx_table = cmdln_arg.get('dbx_table')
    dbx_table_format = cmdln_arg.get('dbx_table_format')
    dbx_table_location = cmdln_arg.get('dbx_table_location')
    consumer_group_id = cmdln_arg.get('consumer_group_id')
    dbx_table_schema = cmdln_arg.get('dbx_table_schema')
    outputmode = cmdln_arg.get('outputmode')
    trigger_type = cmdln_arg.get('trigger_type')

    if runtime == 'local':
        bootstrap_servers = 'localhost:9092'
        options_dict  =  {"kafka.bootstrap.servers" : bootstrap_servers, "subscribe" : subscribe_topic , "kafka.group.id" : consumer_group_id, "startingOffsets" : "earliest" }

        conf = SparkConf()
        #Spark need kafka jars to read from kafka source
        packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:3.1.0,io.delta:delta-core_2.12:1.0.1,io.delta:delta-contribs_2.12:1.0.1"
        conf.set("spark.jars.packages",packages)
        conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    
    else:
        bootstrap_servers = cmdln_arg.get('bootstrap_servers')
        truststore = cmdln_arg.get('truststore')
        truststore_password = cmdln_arg.get('truststore_password')
        keystore = cmdln_arg.get('keystore')
        keystore_password = cmdln_arg.get('keystore_password')
        options_dict  = {"kafka.bootstrap.servers" : bootstrap_servers, "subscribe" : subscribe_topic , "kafka.group.id" : consumer_group_id, "startingOffsets": "earliest",
                        "kafka.security.protocol": "SSL", "kafka.ssl.enabled.protocols": "TLSv1.2", "kafka.ssl.truststore.location": truststore, "kafka.ssl.truststore.type": "JKS",
                        "kafka.ssl.truststore.password": truststore_password, "kafka.ssl.keystore.location": keystore, "kafka.ssl.keystore.type" : "JKS", "kafka.ssl.keystore.password": keystore_password,
                        }
        conf = SparkConf()

    if validate_ing_format(source_system):
        pass
    else:
        raise ValueError(f'Not supported format {source_system}')

    #Initiate Spark Session
    spark = session_builder(conf)
    #Read RAW Data from kafka
    df  = df_rdr_kafka(spark, options_dict)
    df.printSchema()

    #Building the schema from json schema file 
    schema = build_schema_from_file(dbx_table_schema)

    #Structuring the kafka messages with delta table schema
    df = tranform_kafka_records(df, schema , src_sys_format )
    #Removing the special characters from the column names
    df = standarize_df_columns(df)
    df.printSchema()

    df_stream_wrt(runtime , df,dbx_table_format, dbx_table_location,outputmode , trigger_type )

if __name__ == '__main__':
    main()
    print("Kafka Consumer Job Completed")    
