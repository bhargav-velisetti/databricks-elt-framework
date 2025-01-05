from pyspark.sql.types import IntegerType,StringType,StructType,StructField
from pyspark.sql import DataFrame
import json
import re 

def build_schema_from_file(path_to_schema_file : str) -> StructType:

    with open(path_to_schema_file) as f:
        schema = StructType.fromJson(json.load(f))
        return schema

def standarize_df_columns(df : DataFrame) -> DataFrame:
        for field in df.schema.names:
            df = df.withColumnRenamed(field, re.sub(r'[^_a-zA-Z0-9\s]', '', field.lower().replace(" ","_")) )
            return df