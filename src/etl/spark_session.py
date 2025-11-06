from pyspark.sql import SparkSession
from pyspark import SparkConf


def get_spark_session(app_name="UberAnalytics"):
    """
    Crea o recupera una sesión Spark existente.
    """
    conf = (
        SparkConf()
        .setAppName(app_name)
        .setMaster("local[*]")
        .set("spark.sql.shuffle.partitions", "8")
        .set("spark.driver.memory", "2g")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark