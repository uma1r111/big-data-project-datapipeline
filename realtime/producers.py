import time
import json
import random
import pandas as pd
import numpy as np
from datetime import datetime
from kafka import KafkaProducer
import threading
import os

# --- Configuration ---
# Since this runs on your HOST machine, we target localhost
KAFKA_BROKER = 'localhost:9092' 
TOPIC = 'building_energy_stream'
DATA_FILE = 'data/engineered_data.csv'

# --- Load Context Data ---
if os.path.exists(DATA_FILE):
    print(f"✅ Loading context from {DATA_FILE}")
    context_df = pd.read_csv(DATA_FILE)
    context_df['datetime'] = pd.to_datetime(context_df['datetime'])
else:
    print("⚠️ Data file not found. Using random fallbacks.")
    context_df = pd.DataFrame()

def get_current_context():
    """Get weather/price for the current hour from CSV."""
    now_hour = datetime.now().hour
    if not context_df.empty:
        # Get data for the current hour of the day (roughly)
        try:
            row = context_df[context_df['datetime'].dt.hour == now_hour].iloc[0]
            return float(row.get('temperature_C', 15)), float(row.get('retail_price_£_per_kWh', 0.25))
        except:
            pass
    return 15.0, 0.25 # Default Temperature and Price

class BuildingProducer(threading.Thread):
    def __init__(self, building_id, base_load):
        threading.Thread.__init__(self)
        self.building_id = building_id
        self.base_load = base_load
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def run(self):
        print(f"🚀 Producer started for {self.building_id}")
        while True:
            try:
                temp, price = get_current_context()
                
                # Logic: Higher temp = higher load (AC usage)
                load_factor = 1 + (max(0, temp - 20) * 0.1) 
                # Add randomness
                current_usage = (self.base_load * load_factor) + np.random.normal(0, 5)
                current_usage = max(0, round(current_usage, 2))

                payload = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "building_id": self.building_id,
                    "kwh": current_usage,
                    "temp": temp,
                    "price": price,
                    "cost": round(current_usage * price, 4)
                }

                self.producer.send(TOPIC, value=payload)
                # print(f"Sent {self.building_id}: {payload}") # Uncomment to see logs
                time.sleep(2) # Send data every 2 seconds
            except Exception as e:
                print(f"Error in {self.building_id}: {e}")
                time.sleep(5)

if __name__ == "__main__":
    # Define 3 Buildings
    buildings = [
        BuildingProducer("Building-A-HQ", 150),
        BuildingProducer("Building-B-Server", 300),
        BuildingProducer("Building-C-Warehouse", 50)
    ]

    for b in buildings:
        b.start()