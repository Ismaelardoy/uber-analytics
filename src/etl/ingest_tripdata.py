from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
from src.etl.spark_session import get_spark_session
import requests

def ingest_tripdata():
    spark = get_spark_session("UberDataIngestion")

    # Carpeta donde guardaremos los datos crudos
    raw_dir = "data/raw"
    os.makedirs(raw_dir, exist_ok=True)

    # Dataset de enero 2025 (HVFHV = Uber/Lyft)
    dataset_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2025-01.parquet"
    dataset_path = os.path.join(raw_dir, "fhvhv_tripdata_2025-01.parquet")

    # Descargamos el archivo si no existe
    if not os.path.exists(dataset_path):
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

    return df

if __name__ == "__main__":
    ingest_data()


