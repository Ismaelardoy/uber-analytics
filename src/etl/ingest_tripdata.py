from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

def create_spark_session():
    conf = SparkConf() \
        .setAppName("UberDataIngestion") \
        .setMaster("local[*]")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def ingest_data():
    spark = create_spark_session()

    # Carpeta donde guardaremos los datos crudos
    raw_dir = "data/raw"
    os.makedirs(raw_dir, exist_ok=True)

    # Dataset de enero 2025 (HVFHV = Uber/Lyft)
    dataset_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2025-01.parquet"
    dataset_path = os.path.join(raw_dir, "fhvhv_tripdata_2025-01.parquet")

    # Descargamos el archivo si no existe
    if not os.path.exists(dataset_path):
        import requests
        print(f"⬇️ Descargando {dataset_url}...")
        r = requests.get(dataset_url)
        with open(dataset_path, "wb") as f:
            f.write(r.content)
        print(f"✅ Archivo guardado en {dataset_path}")

    # Leemos con Spark
    print("📂 Leyendo datos con Spark...")
    df = spark.read.parquet(dataset_path)
    print(f"✅ Total de filas: {df.count():,}")

    df.printSchema()
    df.show(5)

    spark.stop()

if __name__ == "__main__":
    ingest_data()


