from env_setup import *
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode_outer
import json
import sys
import os

add_modules_path()

from common_pyspark_libs.session_builder import session_builder
from common_pyspark_libs.schema_utility import build_schema_from_file, standarize_df_columns
from common_python_libs.util import validate_ing_format
from common_pyspark_libs.df_io_util import *



def main():
    # Read arguments from command line
    cmdln_arg = json.loads(sys.argv[1])

    file_path = cmdln_arg.get('file_path')
    file_format = cmdln_arg.get('file_format')
    header = cmdln_arg.get('header', 'false')  # Default is 'false' if not provided
    dbx_table_schema = cmdln_arg.get('dbx_table_schema')
    dbx_table_format = cmdln_arg.get('dbx_table_format')
    dbx_table_location = cmdln_arg.get('dbx_table_location')
    outputmode = cmdln_arg.get('outputmode')
    runtime = cmdln_arg.get('runtime')
    column_mapping_path = cmdln_arg.get('column_mapping_path')
    delimiter = cmdln_arg.get('delimiter', ',')  # Default delimiter is comma if not provided

    # Ensure the header is a boolean
    header = 'true' if header.lower() == 'true' else 'false'


    # Initialize Spark session
    conf = SparkConf()
    spark = session_builder(conf)

    if runtime == 'local':
        conf.set("spark.executor.memory", "4g")

    # Validate ingestion format
    if not validate_ing_format(file_format):
        raise ValueError("Unsupported ingestion format:", file_format)

    # Load schema
    schema = None
    if dbx_table_schema:
        schema = build_schema_from_file(dbx_table_schema)
        print("Schema loaded from file.")

    # Process file based on format
    raw_data = process_file(spark, file_path, file_format, schema, header, column_mapping_path,delimiter)

    # Standardize column names
    raw_data = standarize_df_columns(raw_data)

    # Write data to destination
    if runtime == 'local':
        pandas_df = raw_data.toPandas()
        pandas_df.to_csv("filebasedIngestionResult.csv", index=False)
        print("Data saved to CSV format.")
    elif runtime == 'aws':
        print("AWS ingestion not yet implemented.")
    elif runtime == 'azure':
        print("Azure ingestion not yet implemented.")
    elif runtime == 'gcp':
        print("GCP ingestion not yet implemented.")
    else:
        raise ValueError(f"Unsupported runtime: {runtime}")



#json
'{ "runtime": "local", "file_path": "D:\\Selectiva\\databricks-elt-framework\\ingestion\\raw_data.json", "file_format": "json", "header": "true", "dbx_table_format": "csv", "dbx_table_location": "D:\\Selectiva\\databricks-elt-framework\\ingestion", "dbx_table_schema": "D:\\Selectiva\\databricks-elt-framework\\ingestion\\sample_schema.json", "outputmode": "append", "column_mapping_path": "D:\\Selectiva\\databricks-elt-framework\\ingestion\\column_mapping.json" }'

#csv
'{ "runtime": "local", "file_path": "D:\\Selectiva\\databricks-elt-framework\\ingestion\\filebasedIngestionResult.csv", "file_format": "csv", "header": "true", "dbx_table_format": "csv", "dbx_table_location": "D:\\Selectiva\\databricks-elt-framework\\ingestion", "dbx_table_schema": "D:\\Selectiva\\databricks-elt-framework\\ingestion\\sample_schema.json" }'

if __name__ == "__main__":
    main()
    print("File-based Ingestion Job Completed")
