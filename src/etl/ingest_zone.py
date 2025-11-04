from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

def create_spark_session():
    conf = SparkConf() \
        .setAppName("ZoneDataIngestion") \
        .setMaster("local[*]")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def ingest_data():
    spark = create_spark_session()

    raw_dir = "data/raw"
    os.makedirs(raw_dir, exist_ok=True)

    dataset_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    dataset_path = os.path.join(raw_dir, "taxi_zone_lookup.csv")

    if not os.path.exists(dataset_path):
        import requests
        print(f"⬇️ Descargando {dataset_url}...")
        r = requests.get(dataset_url)
        with open(dataset_path, 'wb') as f:
            f.write(r.content)
        print(f"✅ Archivo guardado en {dataset_path}")

    print("📂 Leyendo datos con Spark...")
    df = spark.read.csv(dataset_path, header= True, inferSchema=True)
    print(f"✅ Total de filas: {df.count():,}")

    df.printSchema()
    df.show(5)
    spark.stop()

if __name__ == "__main__":
    ingest_data()
