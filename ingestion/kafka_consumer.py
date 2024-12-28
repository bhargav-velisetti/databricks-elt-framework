from env_setup import * 
import json
from pyspark import SparkConf
from pyspark.sql import SparkSession 
from pyspark.sql import DataFrameReader
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.functions import from_unixtime, column , date_trunc , trunc , to_date, to_json, col , struct, monotonically_increasing_id,from_json, explode
from pyspark.sql.types import StringType, StructField, StringType, IntegerType, DateType, StructType

add_modules_path()

from  common_pyspark_libs.session_builder import *
from  common_pyspark_libs.df_io_util import *
from  common_python_libs.util import  validate_ing_format

cmdln_arg : dict  = json.loads(sys.argv[1])

runtime = cmdln_arg.get('runtime')
format  = cmdln_arg.get('format')
subscribe_topic = cmdln_arg.get('subscribe_topic')
dbx_catalog = cmdln_arg.get('dbx_catalog')
dbx_db = cmdln_arg.get('dbx_db')
dbx_table = cmdln_arg.get('dbx_table')
dbx_table_format = cmdln_arg.get('dbx_table_format')
dbx_table_location = cmdln_arg.get('dbx_table_location')
consumer_group_id = cmdln_arg.get('consumer_group_id')
dbx_table_schema = cmdln_arg.get('dbx_table_schema')

if runtime == 'local':
    bootstrap_servers = 'localhost:9092'
    options_dict  =  {"kafka.bootstrap.servers" : bootstrap_servers, "subscribe" : subscribe_topic , "kafka.group.id" : consumer_group_id, "startingOffsets" : "earliest" }
   
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
'''
'{"runtime":"local" ,"source_system" : "kafka", "src_sys_format" : "json" ,"bootstrap_servers" : "NA", "subscribe_topic" : "test_producer01", "consumer_group_id" : "test_producer01_gp01" ,"truststore" : "NA", "truststore_password" : "NA", "keystore" : "NA", "keystore_password" : "NA" ,  "dbx_catalog" : "default", "dbx_db" : "default", "dbx_table" : "NA" , "dbx_table_format" : "delta" , "dbx_table_location" : "NA", "dbx_table_schema": "C:\\Users\\bharg\\Documents\\dataeng\\databricks-elt-framework\\ingestion\\sample_schema.json" }'

'''
validate_ing_format('kafka')


def main():

    conf = SparkConf()
    #Spark need kafka jars to read from kafka source
    packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:3.1.0"
    conf.set("spark.jars.packages",packages)
    #Initiate Spark Session
    spark = session_builder(conf)
    #Read RAW Data from kafka
    df = df_rdr_kafka(spark, options_dict)
    df.printSchema()

    #Building schema for delta table
    with open(dbx_table_schema) as f:
        import json 
        schema = StructType().fromJson(json.load(f))

    #Structuring the kafka json message with delta table schema
    df = df.select( from_json(df.value.cast("string"), schema=schema).alias('data'), 'timestamp' , 'topic' ).withColumn('row_insert_ts', df.timestamp).withColumn('kafka_topic', df.topic)\
    .select('data.*', 'kafka_topic', 'row_insert_ts')

    df.printSchema()

    df.writeStream\
        .format("console")\
        .outputMode("append")\
        .start()\
        .awaitTermination()
    

if __name__ == '__main__':
    main()