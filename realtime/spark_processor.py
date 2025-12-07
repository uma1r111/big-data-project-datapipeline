from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, avg, window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import redis

KAFKA_BOOTSTRAP = "kafka:29092" 
TOPIC = "building_energy_stream"
REDIS_HOST = "redis"

def write_to_redis(batch_df, batch_id):
    try:
        r = redis.Redis(host=REDIS_HOST, port=6379)
        data = batch_df.collect()
        for row in data:
            r.set("total_kwh", str(row['total_kwh']))
            r.set("total_cost", str(row['total_cost']))
            r.set("total_co2", str(row['total_co2']))
            r.set("avg_carbon_intensity", str(row['avg_carbon_intensity']))
            print(f"🔥 Batch: {row['total_kwh']:.2f} kWh | £{row['total_cost']:.2f}")
    except Exception as e:
        print(f"Redis Error: {e}")

def run_job():
    spark = SparkSession.builder.appName("EnergyEngine").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("timestamp", StringType()), # Read as string first
        StructField("building_id", StringType()),
        StructField("kwh", FloatType()),
        StructField("cost", FloatType()),
        StructField("co2_kg", FloatType()),
        StructField("grid_carbon_intensity", FloatType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC) \
        .load()

    # Parse JSON and Cast Timestamp
    parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    parsed = parsed.withColumn("timestamp", col("timestamp").cast(TimestampType()))

    agg = parsed.groupBy(window(col("timestamp"), "5 seconds")) \
        .agg(
            _sum("kwh").alias("total_kwh"),
            _sum("cost").alias("total_cost"),
            _sum("co2_kg").alias("total_co2"),
            avg("grid_carbon_intensity").alias("avg_carbon_intensity")
        )

    agg.writeStream.outputMode("complete").foreachBatch(write_to_redis).start().awaitTermination()

if __name__ == "__main__":
    run_job()