from pyspark.sql.functions import col, hour, dayofweek, unix_timestamp, trim, upper, round
from src.etl.spark_session import get_spark_session
from src.etl.ingest_tripdata import ingest_tripdata
from src.etl.ingest_zone import ingest_zonedata


def transform_data(df_tripdata=None, df_zones=None):
    """
    Cleans, transforms, and joins Uber trip data with zone information.

    Args:
        df_tripdata (DataFrame, optional): Spark DataFrame containing trip data.
        df_zones (DataFrame, optional): Spark DataFrame containing zone data.

    If no DataFrames are provided (e.g., when running locally), 
    the function will automatically ingest them.
    """

    # Initialize a Spark session
    spark = get_spark_session("UberDataTransform")

    # 1️⃣ Load datasets (if not passed from Mage)
    if df_tripdata is None:
        df_tripdata = ingest_tripdata()
    if df_zones is None:
        df_zones = ingest_zonedata()

    df = df_tripdata
    zones = df_zones

    # 2️⃣ Clean columns
    df = df.drop("SR_Flag", "DOlocationID", "Affiliated_base_number")
    zones = zones.drop("service_zone")

    # Remove rows with missing critical values
    df = df.dropna(subset=["dispatching_base_num", "pickup_datetime", "dropOff_datetime"])
    zones = zones.dropna(subset=["LocationID", "Borough", "Zone"]).dropDuplicates()

    # 3️⃣ Compute new columns
    df = df.withColumn(
        "trip_duration_min",
        round(
            (unix_timestamp(col("dropOff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60,
            2
        )
    )

    df = df.withColumn("pickup_hour", hour(col("pickup_datetime"))) \
           .withColumn("pickup_day_of_week", dayofweek(col("pickup_datetime")))

    # Fill missing pickup location IDs
    df = df.fillna({"PUlocationID": 0})

    # 4️⃣ Filter only Uber trips
    try:
        with open("data/raw/dispatching_base_num.txt", "r") as f:
            data_string = f.read()
            uber_bases = [b.strip().upper() for b in data_string.split(",")]
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        uber_bases = []

    df = df.filter(upper(trim(col("dispatching_base_num"))).isin(uber_bases))

    # 5️⃣ Join with zone lookup data
    df = df.join(
        zones,
        df.PUlocationID == zones.LocationID,
        "left"
    ).withColumnRenamed("Borough", "pickup_borough") \
     .withColumnRenamed("Zone", "pickup_zone") \
     .drop("LocationID").drop("PUlocationID")

    # 6️⃣ Display and return results
    print("✅ Total records after join:", df.count())
    df.show(5)

    return df


if __name__ == "__main__":
    """
    Allows this file to be executed independently (outside Mage)
    for testing or development.
    """
    transform_data()
