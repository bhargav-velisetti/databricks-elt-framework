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

from common_pyspark_libs.session_builder import *
from common_pyspark_libs.df_io_util import *
from common_pyspark_libs.schema_utility import * 
from common_python_libs.util import  *
from common_pyspark_libs.dbx_tbl_util import * 

'''
'{ "runtime":"local" ,"source_system" : "file", "src_sys_format" : "csv" ,"archive_dir" : "C:\\Users\\bharg\\dbx_delta_db_path\\file_based\\archive", "file_path" : "C:\\Users\\bharg\\Documents\\dataeng\\data\\data.csv","stream_api": "false", "archive_processed_files" : "true", "dbx_table_schema": "C:\\Users\\bharg\\Documents\\dataeng\\databricks-elt-framework\\ingestion\\sample_schema2.json", "dbx_catalog" : "default", "dbx_db" : "default", "dbx_table" : "ship_csv_file_delta_table" , "dbx_table_format" : "delta" , "dbx_table_location" : "C:\\Users\\bharg\\dbx_delta_db_path\\file_based\\table_delta",  "outputmode" : "overwrite"}'
'''

def main():

        
    cmdln_arg : dict  = json.loads(sys.argv[1])

    runtime = cmdln_arg.get('runtime')
    source_system  = cmdln_arg.get('source_system')
    src_sys_format = cmdln_arg.get('src_sys_format')
    archive_dir = cmdln_arg.get('archive_dir')
    file_path = cmdln_arg.get('file_path')
    stream_api = cmdln_arg.get('stream_api')
    archive_processed_files = cmdln_arg.get('archive_processed_files')

    dbx_catalog = cmdln_arg.get('dbx_catalog')
    dbx_db = cmdln_arg.get('dbx_db')
    dbx_table = cmdln_arg.get('dbx_table')
    dbx_table_format = cmdln_arg.get('dbx_table_format')
    dbx_table_location = cmdln_arg.get('dbx_table_location')
    dbx_table_schema = cmdln_arg.get('dbx_table_schema')

    outputmode = cmdln_arg.get('outputmode')

    options_dict =  {}

    if runtime == 'local':

        conf = SparkConf()
        #Spark need jdbc related jar files to read from jdbc source
        packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:3.1.0,io.delta:delta-core_2.12:1.0.1,io.delta:delta-contribs_2.12:1.0.1,com.mysql:mysql-connector-j:9.1.0"
        conf.set("spark.jars.packages",packages)
        conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        #Initiate Spark Session
        spark = session_builder(conf)


    else:

        conf = SparkConf()
        #Initiate Spark Session
        spark = session_builder(conf)

    if validate_ing_format(source_system):
        pass
    else:
        raise ValueError(f'Not supported format {source_system}')

    
    #Building the schema from json schema file 
    if src_sys_format == 'parquet' or src_sys_format == 'avro':
        print(f'Building the schema for parquet and avro is not needed. The schema will be inferred from the file')
        schema = None
    else:
        schema = build_schema_from_file(dbx_table_schema)
    
    if stream_api == 'true':
    
        #Streaming API read files into DF 
        df  = df_strm_files_rdr(spark, options_dict)
        #Removing the special characters from the column names
        df = standarize_df_columns(df)
        df.printSchema()
        df.show()
        #Writing the dataframe to the delta table
        df_stream_wrt(spark, runtime , df, dbx_table_location, outputmode , dbx_catalog, dbx_db, dbx_table, dbx_table_format)

    else:
        #Read RAW Data from files
        df  = df_files_rdr(spark = spark, options_dict = options_dict, path = file_path ,format = src_sys_format , schema = schema)
        #Removing the special characters from the column names
        df = standarize_df_columns(df)
        df.printSchema()
        df.show()
        #Writing the dataframe to the delta table
        df_wrt(spark, runtime , df, dbx_table_location, outputmode , dbx_catalog, dbx_db, dbx_table, dbx_table_format)
        #Archive the processed files
        if archive_processed_files == 'true':
            archive_files(runtime,file_path, archive_dir)

if __name__ == '__main__':
    main()
    print("Filebased Data Ingestion Process Completed Successfully")    