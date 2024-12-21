from pyspark.sql import SparkSession 


def session_builder():
    print("Get or Creating the spark session")
    return (
        SparkSession.builder
            .getOrCreate()
    )