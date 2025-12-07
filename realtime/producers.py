import time
import json
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
import threading
import itertools

# CONFIG
KAFKA_BROKER = 'localhost:9092' 
TOPIC = 'building_energy_stream'
DATA_FILE = 'data/engineered_data.csv'

# Load Context Data
print(f"📂 Loading context from {DATA_FILE}...")
try:
    df = pd.read_csv(DATA_FILE)
    print("✅ Data Loaded Successfully.")
except Exception as e:
    print(f"❌ Error loading CSV: {e}")
    exit()

class BuildingProducer(threading.Thread):
    def __init__(self, building_id, base_load, type_factor):
        threading.Thread.__init__(self)
        self.building_id = building_id
        self.base_load = base_load
        self.type_factor = type_factor
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def run(self):
        print(f"🚀 Producer started for {self.building_id}")
        local_stream = itertools.cycle(df.to_dict('records'))
        
        for row in local_stream:
            # 1. EXTRACT CONTEXT
            temp = float(row.get('temperature_C', 20))
            humidity = float(row.get('humidity_%', 50))
            carbon_intensity = float(row.get('carbon_intensity_actual', 150))
            grid_price = float(row.get('retail_price_£_per_kWh', 0.20))
            solar_rad = float(row.get('solar_radiation_Wm2', 0))

            # 2. SIMULATE LOGIC
            # HVAC: Increases if temp is far from 21C
            hvac_load = abs(temp - 21) * 1.5 
            
            # Solar: Warehouse generates power if sun is out
            solar_gen = (solar_rad * 0.08) if "Warehouse" in self.building_id else 0
            
            # Base Load Variation (Random noise handled by math)
            total_kwh = (self.base_load * self.type_factor) + hvac_load - solar_gen
            total_kwh = max(total_kwh, 0.5) # Minimum idle load
            
            # 3. CALCULATE METRICS
            cost = total_kwh * grid_price
            co2_emissions = (total_kwh * carbon_intensity) / 1000 

            payload = {
                "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), # UTC for InfluxDB
                "building_id": self.building_id,
                "kwh": round(total_kwh, 3),
                "cost": round(cost, 4),
                "co2_kg": round(co2_emissions, 3),
                "temp_c": round(temp, 1),
                "humidity": round(humidity, 1),
                "grid_carbon_intensity": round(carbon_intensity, 1),
                "grid_price": round(grid_price, 4)
            }

            try:
                self.producer.send(TOPIC, value=payload)
            except Exception as e:
                print(f"Kafka Error: {e}")
            
            time.sleep(1) # Fast simulation (1 hour of data every 1 second)

if __name__ == "__main__":
    buildings = [
        BuildingProducer("Building-HQ-London", 120, 1.2),      # Office
        BuildingProducer("Building-ServerFarm", 350, 1.0),     # Constant High Load
        BuildingProducer("Building-Warehouse-A", 50, 0.8)      # Solar Panel Equipped
    ]
    for b in buildings:
        b.start()