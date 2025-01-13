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
'{ "runtime":"local" ,"source_system" : "jdbc", "src_sys_format" : "mysql" ,"jdbc_url" : "jdbc:mysql://localhost:3306/sakila",  "user":"xxx", "password":"xxx", "query" : "select * from film where 1=1","incremental_column" : "last_update","inc_col_type": "TIMESTAMP" ,"filter":"film_id > 10", "driver":"com.mysql.cj.jdbc.Driver", "driver_class_path": "", "dbx_catalog" : "default", "dbx_db" : "default", "dbx_table" : "NA" , "dbx_table_format" : "delta" , "dbx_table_location" : "C:\\Users\\bharg\\dbx_delta_db_path\\mysql\\table_delta",  "outputmode" : "append"}'
'''

def main():

        
    cmdln_arg : dict  = json.loads(sys.argv[1])

    runtime = cmdln_arg.get('runtime')
    source_system  = cmdln_arg.get('source_system')
    src_sys_format = cmdln_arg.get('src_sys_format')
    jdbc_url = cmdln_arg.get('jdbc_url')
    user = cmdln_arg.get('user')
    password = cmdln_arg.get('password')
    query = cmdln_arg.get('query')
    incremental_column = cmdln_arg.get('incremental_column')
    inc_col_type = cmdln_arg.get('inc_col_type')
    filter = cmdln_arg.get('filter')
    driver = cmdln_arg.get('driver')
    driver_class_path = cmdln_arg.get('driver_class_path')

    dbx_catalog = cmdln_arg.get('dbx_catalog')
    dbx_db = cmdln_arg.get('dbx_db')
    dbx_table = cmdln_arg.get('dbx_table')
    dbx_table_format = cmdln_arg.get('dbx_table_format')
    dbx_table_location = cmdln_arg.get('dbx_table_location')
    dbx_table_schema = cmdln_arg.get('dbx_table_schema')

    outputmode = cmdln_arg.get('outputmode')

    options_dict =  {"driver" : driver, "url" : jdbc_url ,  "user" : user , "password" : password}


    if filter is not None and filter != '':
        query = f'{query} and {filter}'


    if runtime == 'local':


        conf = SparkConf()
        #Spark need jdbc related jar files to read from jdbc source
        packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:3.1.0,io.delta:delta-core_2.12:1.0.1,io.delta:delta-contribs_2.12:1.0.1,com.mysql:mysql-connector-j:9.1.0"
        conf.set("spark.jars.packages",packages)
        conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        #conf.set()

        #Initiate Spark Session
        spark = session_builder(conf)

        if outputmode == 'append' and incremental_column is not None and inc_col_type is not None:

            max_value = str(get_dbx_tbl_max_value(runtime, spark, dbx_table_location , incremental_column))

            print(f'Max value of the incremental column {incremental_column} is {max_value}')

            if str(src_sys_format).upper() == 'MYSQL':
                if inc_col_type.upper() == 'TIMESTAMP':
                   max_value = f'TIMESTAMP("{max_value}")'
                elif inc_col_type.upper() == 'DATETIME':
                    max_value = f'STR_TO_DATE("{max_value}", "%Y-%m-%d %H:%i:%s" )'
                elif inc_col_type.upper() == 'DATE':
                    max_value = f'DATE("{max_value}")'
            else:
                pass 


            query = f'{query} and {incremental_column} > {max_value}'
            options  =  {"query" : query }
            options_dict.update(**options)

    
    else:
        options  =  {}
        options_dict.update(**options)

        conf = SparkConf()
        #Initiate Spark Session
        spark = session_builder(conf)

    if validate_ing_format(source_system):
        pass
    else:
        raise ValueError(f'Not supported format {source_system}')


    #Read RAW Data from jdbc source
    print('DF RDR Options are ......')
    df  = df_rdr_jdbc(spark, options_dict)
    #Removing the special characters from the column names
    df = standarize_df_columns(df)
    df.printSchema()
    #Writing the dataframe to the delta table
    df_wrt_jdbc(runtime , df, dbx_table_location, outputmode , dbx_catalog, dbx_db, dbx_table, dbx_table_format)

if __name__ == '__main__':
    main()
    print("JDBC Data Ingestion Process Completed Successfully")    
