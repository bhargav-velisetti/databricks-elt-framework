from pyspark.sql.types import IntegerType,StringType,StructType,StructField
import json

def build_schema_from_file(path_to_schema_file : str) -> StructType:

    with open(path_to_schema_file) as f:
        scheme = StructType.fromJson(json.load(f))
        return scheme

def standarize_columns(df_schema : StructType):

    print("""implement this function. 
          1. all the columns should be in lower case 
          2. no spaces are special chars.
          3. replace special chars with _
          """)

    return StructType

