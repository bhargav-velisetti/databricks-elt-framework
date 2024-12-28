from pyspark.sql import SparkSession 
from pyspark import SparkConf

def session_builder(conf : SparkConf):
    print("Get or Creating the spark session")
    return (
        SparkSession.builder
            .config(conf=conf)
            .getOrCreate()
    )