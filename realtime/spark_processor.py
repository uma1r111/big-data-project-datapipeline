from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, avg, window, first, max as _max, min as _min, count, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
import redis
import json

# CONNECTION
KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "building_energy_stream"
REDIS_HOST = "redis"

def push_to_redis(batch_df, batch_id):
    """Push aggregated metrics to Redis every 5 seconds"""
    r = redis.Redis(host=REDIS_HOST, port=6379)
    
    rows = batch_df.collect()
    
    if not rows:
        return
    
    for row in rows:
        try:
            bid = row['building_id']
            
            # === 1. PORTFOLIO-LEVEL AGGREGATES ===
            r.set("portfolio_total_load", str(round(row['total_load'], 2)))
            r.set("portfolio_total_cost", str(round(row['total_cost'], 4)))
            r.set("portfolio_total_carbon", str(round(row['total_carbon'], 3)))
            r.set("portfolio_avg_efficiency", str(round(row['avg_efficiency'], 2)))
            r.set("portfolio_total_occupancy", str(int(row['total_occupancy'])))
            r.set("portfolio_avg_comfort", str(round(row['avg_comfort'], 1)))
            r.set("portfolio_avg_health", str(round(row['avg_health'], 1)))
            
            # === 2. BUILDING-SPECIFIC DETAILED DATA ===
            # Store the latest complete payload for each building
            if row['latest_raw']:
                r.set(f"live:{bid}", row['latest_raw'])
            
            # === 3. PERFORMANCE METRICS ===
            metrics = {
                "peak_load": round(row['peak_load'], 2),
                "min_load": round(row['min_load'], 2),
                "avg_load": round(row['avg_load'], 2),
                "total_samples": int(row['sample_count']),
                "avg_pf": round(row['avg_power_factor'], 3),
                "avg_indoor_temp": round(row['avg_indoor_temp'], 1),
                "total_solar_gen": round(row['total_solar_gen'], 2)
            }
            r.set(f"metrics:{bid}", json.dumps(metrics))
            
            # === 4. ALERT TRACKING ===
            # Store counts of various alert conditions detected
            alert_data = {
                "high_temp_count": int(row.get('high_temp_count', 0)),
                "low_pf_count": int(row.get('low_pf_count', 0)),
                "high_co2_count": int(row.get('high_co2_count', 0)),
                "equipment_health_low": int(row.get('health_low_count', 0))
            }
            r.set(f"alerts:{bid}", json.dumps(alert_data))
            
        except Exception as e:
            print(f"Error processing row for {bid}: {e}")
    
    print(f"✅ Batch {batch_id} processed - {len(rows)} buildings updated")

def run_job():
    spark = SparkSession.builder \
        .appName("EcoTwin_Analytics") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Define schema for key metrics we need to aggregate
    schema = StructType([
        StructField("building_id", StringType(), True),
        StructField("timestamp_gen", FloatType(), True),
        StructField("b_load_kw", FloatType(), True),
        StructField("b_solar_gen_kw", FloatType(), True),
        StructField("b_cost_rate", FloatType(), True),
        StructField("b_carbon_rate", FloatType(), True),
        StructField("b_occupancy", IntegerType(), True),
        StructField("b_energy_efficiency", FloatType(), True),
        StructField("b_comfort_index", FloatType(), True),
        StructField("b_equip_health", FloatType(), True),
        StructField("b_indoor_temp", FloatType(), True),
        StructField("elec_power_factor", FloatType(), True),
        StructField("ieq_co2_ppm", IntegerType(), True)
    ])

    # Parse JSON and extract fields
    json_df = df.select(
        col("value").cast("string").alias("raw_json"),
        from_json(col("value").cast("string"), schema).alias("data")
    )
    
    # Convert float timestamp to proper timestamp type
    structured = json_df.select(
        "raw_json", 
        "data.*",
        from_unixtime(col("data.timestamp_gen")).cast("timestamp").alias("event_time")
    )

    # Aggregate over 5-second windows using the converted timestamp
    agg = structured \
        .withWatermark("event_time", "10 seconds") \
        .groupBy(
            window(col("event_time"), "5 seconds"),
            col("building_id")
        ) \
        .agg(
            # Load metrics
            _sum("b_load_kw").alias("total_load"),
            _max("b_load_kw").alias("peak_load"),
            _min("b_load_kw").alias("min_load"),
            avg("b_load_kw").alias("avg_load"),
            
            # Financial
            _sum("b_cost_rate").alias("total_cost"),
            _sum("b_carbon_rate").alias("total_carbon"),
            
            # Solar
            _sum("b_solar_gen_kw").alias("total_solar_gen"),
            
            # Occupancy & Efficiency
            _sum("b_occupancy").alias("total_occupancy"),
            avg("b_energy_efficiency").alias("avg_efficiency"),
            
            # Comfort & Health
            avg("b_comfort_index").alias("avg_comfort"),
            avg("b_equip_health").alias("avg_health"),
            avg("b_indoor_temp").alias("avg_indoor_temp"),
            
            # Power Quality
            avg("elec_power_factor").alias("avg_power_factor"),
            
            # Sample count
            count("*").alias("sample_count"),
            
            # Alert conditions (count how many samples exceeded thresholds)
            _sum((col("b_indoor_temp") > 24).cast("int")).alias("high_temp_count"),
            _sum((col("elec_power_factor") < 0.90).cast("int")).alias("low_pf_count"),
            _sum((col("ieq_co2_ppm") > 1000).cast("int")).alias("high_co2_count"),
            _sum((col("b_equip_health") < 80).cast("int")).alias("health_low_count"),
            
            # Keep latest raw packet for detailed view
            first("raw_json").alias("latest_raw")
        )

    # Write to Redis via foreachBatch
    query = agg.writeStream \
        .outputMode("update") \
        .foreachBatch(push_to_redis) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_job()