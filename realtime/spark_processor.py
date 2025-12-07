from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import redis

# --- Configuration (Internal Docker DNS) ---
KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "building_energy_stream"
REDIS_HOST = "redis"
REDIS_PORT = 6379

def write_to_redis(batch_df, batch_id):
    """
    Writes the aggregated results of the micro-batch to Redis.
    This runs on the worker nodes.
    """
    # Create Redis connection inside the function (important for serialization)
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        
        # Collect data to the driver to write simply (for small aggregations this is fine)
        data = batch_df.collect()
        
        for row in data:
            # We overwrite the keys to show "Latest" status on dashboard
            r.set("total_kwh", str(row['total_kwh']))
            r.set("total_cost", str(row['total_cost']))
            r.set("last_updated", str(row['window']['end']))
            print(f"🔥 Updated Redis: {row['total_kwh']} kWh")
            
    except Exception as e:
        print(f"Redis Error: {e}")

def run_job():
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("EnergyOptimizationEngine") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Define Schema matching the Producer JSON
    schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("building_id", StringType()),
        StructField("kwh", FloatType()),
        StructField("temp", FloatType()),
        StructField("price", FloatType()),
        StructField("cost", FloatType())
    ])

    # 1. Read Stream from Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse JSON Payload
    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # 3. Aggregation Logic (Windowed)
    # Group by 10-second windows to calculate live total load
    df_agg = df_parsed \
        .groupBy(window(col("timestamp"), "10 seconds")) \
        .agg(
            _sum("kwh").alias("total_kwh"),
            _sum("cost").alias("total_cost")
        )

    # 4. Write Output to Redis
    query = df_agg.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_to_redis) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_job()