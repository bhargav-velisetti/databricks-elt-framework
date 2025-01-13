from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


def chk_tbl_exists_local(spark : SparkSession, dbx_table_location):
    try:
        df = spark.read.format("delta").load(f'{dbx_table_location}')
        return True
    except:
        return False


def chk_tbl_exists(spark : SparkSession,tbl_name, db_name='default', ctlg_name='default'):
    """
    Check if a table exists in the metastore
    :param spark: SparkSession
    :param ctlg_name: catalog name
    :param tbl_name: table name
    :param db_name: database name
    :return: True if table exists, False otherwise
    """
    tbl_exists = False
    try:
        spark.sql(f"DESCRIBE {ctlg_name}.{db_name}.{tbl_name}")
        tbl_exists = True
    except AnalysisException as e:
        if "Table not found" in str(e):
            tbl_exists = False
    return tbl_exists


def get_dbx_tbl_max_value_cloud(runtime : str , spark : SparkSession ,dbx_catalog : str, dbx_db : str, dbx_table : str , incremental_column : str):
    """
    Get the maximum value of the incremental column from the table
    :param runtime: runtime environment
    :param spark: SparkSession
    :param dbx_catalog: catalog name
    :param dbx_db: database name
    :param dbx_table: table name
    :param incremental_column: incremental column
    :return: maximum value of the incremental column
    """
    if runtime == 'local':
        spark.sql(f"USE {dbx_catalog}")
        max_value = spark.sql(f"SELECT MAX({incremental_column}) FROM {dbx_db}.{dbx_table}").collect()[0][0]
        return max_value
    else:
        raise ValueError(f'Runtime {runtime} not supported yet.')
    
def get_dbx_tbl_max_value(runtime : str , spark : SparkSession ,dbx_table_location : str , incremental_column : str):
    """
    Get the maximum value of the incremental column from the table
    :param runtime: runtime environment
    :param spark: SparkSession
    :param dbx_table_location: table location
    :param incremental_column: incremental column
    :return: maximum value of the incremental column
    """
    if runtime == 'local':
        if chk_tbl_exists_local(spark, f'{dbx_table_location}\\data'):
            df = spark.read.format("delta").load(f'{dbx_table_location}\\data')
            max_value = df.agg({incremental_column: "max"}).collect()[0][0]
            return max_value
        else:
            max_value = '1900-01-01 00:00:00'

    elif runtime == 'aws':
        print('implement the code for aws')
    elif runtime == 'gcp':
        print('implement the code for gcp')
    elif runtime == 'azure':
        print('implement the code for azure')
        
    return max_value
