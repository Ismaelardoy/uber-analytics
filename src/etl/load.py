from transform import transform_data

df, spark = transform_data()

df.write.mode("overwrite").parquet("data/processed/fhvhv_tripdata_clean")