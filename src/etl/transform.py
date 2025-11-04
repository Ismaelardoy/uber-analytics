from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, unix_timestamp, trim, upper, round

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("transform") \
    .config("spark.sql.parquet.native.enable", "false") \
    .config("spark.hadoop.hadoop.native.lib", "false") \
    .getOrCreate()

# ======================
# 1️⃣ Cargar datasets
# ======================
df = spark.read.parquet("data/raw/fhvhv_tripdata_2025-01.parquet")
zones = spark.read.csv("data/raw/taxi_zone_lookup.csv", header=True, inferSchema=True)

# ======================
# 2️⃣ Limpiar columnas
# ======================
df = df.drop("SR_Flag", "DOlocationID", "Affiliated_base_number")
zones = zones.drop("service_zone")

# Quitar filas con datos esenciales nulos
df = df.dropna(subset=["dispatching_base_num", "pickup_datetime", "dropOff_datetime"])
zones = zones.dropna(subset=["LocationID", "Borough", "Zone"]).dropDuplicates()

# ======================
# 3️⃣ Calcular nuevas columnas
# ======================

df = df.withColumn(
    "trip_duration_min",
    round((unix_timestamp(col("dropOff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60
, 2))
df = df.withColumn("pickup_hour", hour(col("pickup_datetime"))) \
       .withColumn("pickup_day_of_week", dayofweek(col("pickup_datetime")))

# Rellenar nulos en PUlocationID
df = df.fillna({"PUlocationID": 0})

# ======================
# 4️⃣ Filtrar solo viajes de Uber
# ======================
try:
    with open("data/raw/dispatching_base_num.txt", "r") as f:
        data_string = f.read()
        uber_bases = [b.strip().upper() for b in data_string.split(",")]
except FileNotFoundError as e:
    print(f"ERROR: {e}")
    uber_bases = []

df = df.filter(upper(trim(col("dispatching_base_num"))).isin(uber_bases))

# ======================
# 5️⃣ Hacer el join con las zonas
# ======================
df = df.join(
    zones,
    df.PUlocationID == zones.LocationID,
    "left"
).withColumnRenamed("Borough", "pickup_borough") \
 .withColumnRenamed("Zone", "pickup_zone") \
 .drop("LocationID").drop("PUlocationID")

# ======================
# 6️⃣ Ver resultados y guardar
# ======================
print("✅ Registros totales después del join:", df.count())
df.filter(col("pickup_hour") == 1).show()


# Guardar resultado procesado
# df.write.mode("overwrite").parquet("data/processed/fhvhv_tripdata_clean.parquet")

spark.stop()
