from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, avg, window, first, from_unixtime, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, BooleanType
import redis
import time

# --- CONFIG ---
KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "building_energy_stream"
REDIS_HOST = "redis"
MONGO_URI = "mongodb://mongodb:27017/ecotwin.telemetry"

def push_to_redis(batch_df, batch_id):
    """Push aggregated metrics to Redis for Live Dashboard"""
    if batch_df.count() == 0:
        return

    r = redis.Redis(host=REDIS_HOST, port=6379)
    rows = batch_df.collect()
    
    # Portfolio-level aggregates
    latest_row = rows[0]
    r.set("portfolio_total_load", str(round(latest_row['total_load'], 2)))
    r.set("portfolio_total_cost", str(round(latest_row['total_cost'], 4)))
    r.set("portfolio_total_carbon", str(round(latest_row['total_carbon'], 3)))
    
    for row in rows:
        try:
            bid = row['building_id']
            if row['latest_raw']:
                r.set(f"live:{bid}", row['latest_raw'])
        except Exception as e:
            print(f"Error Redis: {e}")
    
    print(f"✅ Batch {batch_id} pushed to Redis")

def run_job():
    # Include MongoDB Connector Package
    spark = SparkSession.builder \
        .appName("EcoTwin_Engine") \
        .config("spark.mongodb.write.connection.uri", MONGO_URI) \
        .config("spark.mongodb.read.connection.uri", MONGO_URI) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Schema (Truncated for brevity, assumes standard EcoTwin schema)
    schema = StructType([
        StructField("timestamp_gen", FloatType(), True),
        StructField("building_id", StringType(), True),
        StructField("b_load_kw", FloatType(), True),
        StructField("b_indoor_temp", FloatType(), True),
        StructField("b_occupancy", IntegerType(), True),
        StructField("b_cost_rate", FloatType(), True),
        StructField("b_carbon_rate", FloatType(), True),
        StructField("b_energy_efficiency", FloatType(), True),
        StructField("b_comfort_index", FloatType(), True),
        StructField("b_equip_health", FloatType(), True),
        StructField("ieq_co2_ppm", IntegerType(), True),
        StructField("ieq_voc_ppb", IntegerType(), True),
        StructField("elec_power_factor", FloatType(), True),
        # ... add other fields as needed ...
    ])

    # Parse JSON
    json_df = df.select(
        col("value").cast("string").alias("raw_json"),
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("raw_json", "data.*")

    # Add Event Time
    structured = json_df.withColumn("event_time", current_timestamp())

    # --- STREAM 1: WRITE RAW DATA TO MONGODB ---
    mongo_query = structured.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "/tmp/checkpoints/mongo_stream") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .trigger(processingTime="10 seconds") \
        .start()

    # --- STREAM 2: AGGREGATE FOR REDIS ---
    agg_for_redis = structured \
        .withWatermark("event_time", "10 seconds") \
        .groupBy(window(col("event_time"), "5 seconds"), col("building_id")) \
        .agg(
            _sum("b_load_kw").alias("total_load"),
            _sum("b_cost_rate").alias("total_cost"),
            _sum("b_carbon_rate").alias("total_carbon"),
            first("raw_json").alias("latest_raw")
        )

    redis_query = agg_for_redis.writeStream \
        .outputMode("update") \
        .foreachBatch(push_to_redis) \
        .option("checkpointLocation", "/tmp/checkpoints/redis_stream") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_job()