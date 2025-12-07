import json
from kafka import KafkaConsumer
from influxdb import InfluxDBClient
from datetime import datetime

# CONFIG
KAFKA_TOPIC = "building_energy_stream"
INFLUX_DB = "energy_db"

def run_bridge():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    client = InfluxDBClient(host='localhost', port=8086)
    
    # Ensure DB exists
    dbs = client.get_list_database()
    if not any(db['name'] == INFLUX_DB for db in dbs):
        print(f"Creating database: {INFLUX_DB}")
        client.create_database(INFLUX_DB)
    
    client.switch_database(INFLUX_DB)
    print("🌉 Kafka -> InfluxDB Bridge Started...")

    for message in consumer:
        data = message.value
        
        json_body = [
            {
                "measurement": "building_metrics",
                "tags": {
                    "building_id": data['building_id'],
                    "location": "London"
                },
                "time": data['timestamp'],
                "fields": {
                    "kwh": float(data['kwh']),
                    "cost": float(data['cost']),
                    "co2_kg": float(data['co2_kg']),
                    "temp_c": float(data['temp_c']),
                    "grid_carbon_intensity": float(data['grid_carbon_intensity']),
                    "grid_price": float(data['grid_price'])
                }
            }
        ]
        
        try:
            client.write_points(json_body)
            # print(f"saved: {data['building_id']}") # Uncomment for debug
        except Exception as e:
            print(f"Influx Error: {e}")

if __name__ == "__main__":
    run_bridge()