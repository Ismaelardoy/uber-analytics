from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Ejemplo PySpark SQL") \
    .getOrCreate()

df = spark.read.parquet("data/raw/fhvhv_tripdata_2025-01.parquet")

df.createOrReplaceTempView("personas")

spark.sql("""
SELECT DISTINCT(dispatching_base_num) FROM personas WHERE dispatching_base_num = "B00009"
""").show(500)

spark.stop()