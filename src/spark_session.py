from pyspark.sql import SparkSession

_spark = None

def get_spark_session(app_name="LocalSparkApp"):
    global _spark
    if _spark is not None:
        return _spark

    spark = (
        SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    _spark = spark
    return _spark
