from pyspark.sql import SparkSession, DataFrameReader
from pyspark.sql.streaming import DataStreamReader


def df_rdr_kafka(spark : SparkSession, options_dict : dict ):

    df_stream_reader = spark.readStream

    for k, v in options_dict.items():
        df_stream_reader.option(k,v)

    df = df_stream_reader \
    .format("kafka") \
    .load()
    # Returning the RAW Kafka Data as a Dataframe
    return df 
