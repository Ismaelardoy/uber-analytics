from src.etl.transform import transform_data
from src.etl.spark_session import get_spark_session
import os

def load_data():
    spark = get_spark_session("UberDataLoad")
    df = transform_data()

    output_path = "data/processed/fhvhv_tripdata_clean"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)
    print(f"💾 Datos guardados en: {output_path}")

    spark.stop()

if __name__ == "__main__":
    load_data()