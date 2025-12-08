import time
import json
import pandas as pd
import numpy as np
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import threading
import itertools

# --- CONFIG ---
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'building_energy_stream'
DATA_FILE = 'data/engineered_data.csv'

print(f"📂 Loading Context from {DATA_FILE}...")
try:
    df = pd.read_csv(DATA_FILE).fillna(0)
except Exception as e:
    print(f"❌ Error loading CSV: {e}")
    exit()

class DigitalTwin(threading.Thread):
    def __init__(self, building_id, profile):
        threading.Thread.__init__(self)
        self.building_id = building_id
        self.profile = profile
        self.producer = None  # Initialize producer as None

        # --- START: ROBUST KAFKA CONNECTION LOGIC ---
        retries = 10
        for i in range(retries):
            try:
                print(f"[{self.building_id}] Attempting to connect to Kafka at {KAFKA_BROKER} (Attempt {i+1}/{retries})...")
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKER,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    # Add timeouts to fail faster if Kafka is not available
                    api_version_auto_timeout_ms=5000,
                    request_timeout_ms=10000,
                    # Security protocol might be needed in some environments, though PLAINTEXT is default
                    # security_protocol='PLAINTEXT'
                )
                print(f"✅ [{self.building_id}] Successfully connected to Kafka.")
                break  # Exit the loop if connection is successful
            except NoBrokersAvailable:
                print(f"⚠️ [{self.building_id}] Kafka connection failed: NoBrokersAvailable. Broker may not be ready.")
                if i < retries - 1:
                    print("Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    print(f"❌ [{self.building_id}] Could not connect to Kafka after {retries} attempts. Exiting thread.")
            except Exception as e:
                print(f"⚠️ [{self.building_id}] An unexpected error occurred: {e}")
                if i < retries - 1:
                    print("Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    print(f"❌ [{self.building_id}] Could not connect to Kafka after {retries} attempts. Exiting thread.")
        # --- END: ROBUST KAFKA CONNECTION LOGIC ---

        # Physics State
        self.indoor_temp = 21.0
        self.occupancy = 0
        self.equipment_health = 100.0
        self.water_leak_detected = False
        self.fire_risk_level = 0

    def generate_deep_telemetry(self, base_load, outdoor_temp, humidity, wind_speed, aqi_outdoor):
        """Generates 70+ Granular IoT Metrics with Environmental Data Integration"""
        
        # ⚡ 1. ELECTRICAL (3-Phase + Advanced Power Quality)
        volts = np.random.normal(230, 2, 3)
        amps = [(base_load * 1000 / 690) * np.random.normal(1, 0.05) for _ in range(3)]
        
        elec = {
            "elec_volt_L1": round(volts[0], 1),
            "elec_volt_L2": round(volts[1], 1),
            "elec_volt_L3": round(volts[2], 1),
            "elec_amp_L1": round(amps[0], 1),
            "elec_amp_L2": round(amps[1], 1),
            "elec_amp_L3": round(amps[2], 1),
            "elec_power_factor": round(random.uniform(0.85, 0.99), 3),
            "elec_frequency": round(np.random.normal(50, 0.05), 2),
            "elec_thd_current": round(random.uniform(1.0, 5.0), 2),
            "elec_thd_voltage": round(random.uniform(0.5, 3.0), 2),
            "elec_reactive_kvar": round(base_load * 0.2 * random.uniform(0.8, 1.2), 2),
            "elec_apparent_kva": round(base_load * 1.05, 2),
            "elec_ups_battery_pct": round(max(95, 100 - (base_load/1000)), 1),
            "elec_ups_load_pct": round((base_load / self.profile['base_equipment']) * 100, 1),
            "elec_surge_events_count": int(np.random.poisson(0.1)),
            "elec_peak_demand_kw": round(base_load * random.uniform(1.0, 1.15), 2)
        }

        # 🌫️ 2. INDOOR ENVIRONMENT (IEQ) - Enhanced with Outdoor Correlation
        outdoor_pollutant_influence = aqi_outdoor * 0.05
        
        ieq = {
            "ieq_co2_ppm": int(400 + (self.occupancy * 2) + np.random.normal(0, 10) + outdoor_pollutant_influence),
            "ieq_voc_ppb": int(self.occupancy * 1.5 + np.random.normal(10, 5) + outdoor_pollutant_influence),
            "ieq_pm25_indoor": round(aqi_outdoor * 0.3 + np.random.normal(0, 2), 2),
            "ieq_pm10_indoor": round(aqi_outdoor * 0.4 + np.random.normal(0, 3), 2),
            "ieq_noise_db": round(35 + (self.occupancy * 0.1) + np.random.normal(0, 2), 1),
            "ieq_light_lux": int(500 * random.uniform(0.9, 1.1)),
            "ieq_humidity_in": round(humidity * random.uniform(0.9, 1.1), 1),
            "ieq_pressure_pa": round(1013 + np.random.normal(0, 2), 1),
            "ieq_radon_bqm3": round(random.uniform(10, 50), 1),
            "ieq_formaldehyde_ppb": round(random.uniform(5, 15), 1),
            "ieq_co_ppm": round(random.uniform(0.5, 2.0), 2)
        }

        # ⚙️ 3. MECHANICAL / HVAC - Enhanced with Weather Integration
        hvac_load_factor = abs(outdoor_temp - self.indoor_temp) / 15.0
        
        mech = {
            "hvac_fan_speed_rpm": int(1200 * (base_load/self.profile['base_equipment']) * hvac_load_factor),
            "hvac_duct_pressure_pa": round(250 + np.random.normal(0, 10), 1),
            "hvac_filter_health": round(max(0, 100 - (time.time() % 10000)/100), 1),
            "hvac_chiller_vibration": round(random.uniform(0.1, 0.8), 2),
            "hvac_chiller_efficiency_cop": round(random.uniform(2.5, 4.5), 2),
            "hvac_return_temp": round(self.indoor_temp + 2, 1),
            "hvac_supply_temp": round(self.indoor_temp - 5, 1),
            "hvac_outdoor_temp": outdoor_temp,
            "hvac_differential_pressure": round(abs(1013 - ieq['ieq_pressure_pa']), 1),
            "water_flow_rate_lpm": round(base_load * 0.5, 1),
            "water_supply_temp": round(random.uniform(50, 70), 1),
            "water_return_temp": round(random.uniform(40, 60), 1),
            "water_pressure_bar": round(random.uniform(2.5, 4.0), 2),
            "water_consumption_m3": round(self.occupancy * 0.05 + np.random.normal(0, 0.5), 2),
            "boiler_temp_c": round(60 + np.random.normal(0, 2), 1),
            "boiler_efficiency_pct": round(random.uniform(85, 95), 1),
            "steam_pressure_bar": round(random.uniform(5, 8), 2)
        }

        # 🛜 4. IT & SECURITY - Enhanced
        net = {
            "net_wifi_devices": int(self.occupancy * 1.2),
            "net_iot_latency_ms": int(random.expovariate(1/20)),
            "net_bandwidth_mbps": round(random.uniform(50, 200), 1),
            "net_packet_loss_pct": round(random.uniform(0, 0.5), 2),
            "net_server_temp_c": round(22 + (base_load/100), 1),
            "net_server_cpu_pct": round(random.uniform(20, 80), 1),
            "net_server_ram_pct": round(random.uniform(40, 85), 1),
            "sec_door_access_count": int(np.random.poisson(self.occupancy/50)),
            "sec_camera_active": int(random.uniform(80, 100)),
            "sec_intrusion_alerts": int(np.random.poisson(0.05)),
            "sec_badge_swipes": int(self.occupancy * 0.3)
        }

        # 🔋 5. ENERGY STORAGE & RENEWABLES
        pv_gen_kw = round(max(0, base_load * 0.2 * random.uniform(0.8, 1.2)), 2)
        energy = {
            "battery_soc_pct": round(random.uniform(40, 95), 1),
            "battery_charge_kw": round(random.uniform(-50, 50), 2),
            "pv_generation_kw": pv_gen_kw,
            "pv_efficiency_pct": round(random.uniform(15, 22), 1),
            "wind_turbine_rpm": int(wind_speed * 50),
            "grid_import_kw": round(max(0, base_load - pv_gen_kw), 2),
            "grid_export_kw": round(max(0, random.uniform(-5, 5)), 2)
        }

        # 🏗️ 6. STRUCTURAL & SAFETY
        structure = {
            "struct_vibration_mmps": round(random.uniform(0.1, 0.5), 2),
            "struct_tilt_degrees": round(random.uniform(-0.02, 0.02), 3),
            "elevator_trips": int(np.random.poisson(self.occupancy/100)),
            "fire_alarm_status": random.choice([0, 0, 0, 0, 1]) if random.random() < 0.001 else 0,
            "fire_extinguisher_pressure_bar": round(random.uniform(12, 15), 1),
            "emergency_exit_clear": random.choice([1, 1, 1, 0]) if random.random() < 0.99 else 0,
            "water_leak_sensors": 1 if self.water_leak_detected else 0,
            "gas_leak_ppm": round(random.uniform(0, 5), 2)
        }

        # 🌡️ 7. WEATHER CORRELATION METRICS
        weather = {
            "weather_outdoor_humidity": humidity,
            "weather_wind_speed": wind_speed,
            "weather_heat_index": round(outdoor_temp + (humidity * 0.1), 1),
            "weather_wind_chill": round(outdoor_temp - (wind_speed * 0.5), 1)
        }

        return {**elec, **ieq, **mech, **net, **energy, **structure, **weather}

    def run(self):
        # Gracefully exit the thread if producer was never created
        if not self.producer:
            return

        print(f"🚀 Full-Stack Twin Online: {self.building_id}")
        local_stream = itertools.cycle(df.to_dict('records'))
        
        for world_state in local_stream:
            try:
                # 1. READ WORLD DRIVERS (Enhanced with more features)
                outdoor_temp = float(world_state.get('temperature_C', 20))
                humidity = float(world_state.get('humidity_%', 50))
                wind_speed = float(world_state.get('wind_speed_mps', 5))
                solar_rad = float(world_state.get('solar_radiation_Wm2', 0))
                cloud_cover = float(world_state.get('cloud_cover_%', 50))
                grid_carbon = float(world_state.get('carbon_intensity_actual', 150))
                price = float(world_state.get('retail_price_£_per_kWh', 0.20))
                pm25_out = float(world_state.get('pm2_5', 10))
                aqi_outdoor = float(world_state.get('aqi_us', 50))
                
                # Renewable generation data
                uk_gen_wind = float(world_state.get('uk_gen_wind_%', 20))
                uk_gen_solar = float(world_state.get('uk_gen_solar_%', 5))
                uk_gen_nuclear = float(world_state.get('uk_gen_nuclear_%', 15))
                renewable_pct = float(world_state.get('renewable_pct', 30))

                # 2. CALCULATE PHYSICS (Base Metrics)
                hour = int(world_state.get('hour', 12))
                is_day = 8 <= hour <= 18
                is_peak = hour in [7, 8, 9, 17, 18, 19]
                
                target_occ = self.profile['base_occupancy'] if is_day else self.profile['base_occupancy'] * 0.1
                self.occupancy += (target_occ - self.occupancy) * 0.2 + np.random.normal(0, 2)
                self.occupancy = max(0, self.occupancy)

                delta_temp = outdoor_temp - self.indoor_temp
                thermal_loss = delta_temp * self.profile['insulation_factor']
                
                # Enhanced HVAC with weather consideration
                hvac_power_kw = abs(thermal_loss) * 5.0 + (self.occupancy * 0.1)
                hvac_power_kw *= (1 + (humidity - 50) / 100)  # Humidity impact
                hvac_power_kw *= (1 + cloud_cover / 200)  # Cloud cover impact
                
                self.indoor_temp += (thermal_loss * 0.1) - (np.sign(thermal_loss) * 0.08)

                lighting_kw = self.profile['base_lighting'] * (1 - (solar_rad / 1200.0))
                equipment_kw = self.profile['base_equipment'] + (self.occupancy * 0.05)
                
                # Solar generation with cloud impact
                local_solar_kw = (solar_rad * self.profile['solar_capacity'] * (1 - cloud_cover/150)) / 1000.0
                
                total_load_kw = max(0.5, hvac_power_kw + lighting_kw + equipment_kw - local_solar_kw)

                # Equipment health degradation
                decay = 0.05 if is_day else 0.01
                self.equipment_health -= decay
                if self.equipment_health < 75: 
                    self.equipment_health = 100.0

                # Dynamic pricing impact
                peak_multiplier = 1.3 if is_peak else 1.0
                actual_cost_rate = total_load_kw * price * peak_multiplier

                # 3. GENERATE 70+ IOT METRICS
                deep_metrics = self.generate_deep_telemetry(
                    total_load_kw, outdoor_temp, humidity, wind_speed, aqi_outdoor
                )

                # 4. CALCULATE ADDITIONAL KPIs
                comfort_index = 100 - abs(21 - self.indoor_temp) * 5 - abs(50 - humidity) * 0.3
                comfort_index = max(0, min(100, comfort_index))
                
                energy_efficiency = (self.occupancy / max(1, total_load_kw)) * 100
                carbon_intensity_building = (total_load_kw * grid_carbon) / 1000
                
                indoor_aqi = int(
                    (pm25_out * 0.1) + 
                    (self.occupancy * 0.2) + 
                    deep_metrics['ieq_voc_ppb'] * 0.1 +
                    deep_metrics['ieq_co2_ppm'] * 0.01
                )

                # 5. PREPARE COMPREHENSIVE PAYLOAD
                payload = world_state.copy()
                payload.update({
                    "timestamp_gen": time.time(),
                    "building_id": self.building_id,
                    
                    # Core Building Metrics
                    "b_load_kw": round(total_load_kw, 2),
                    "b_solar_gen_kw": round(local_solar_kw, 2),
                    "b_hvac_power_kw": round(hvac_power_kw, 2),
                    "b_lighting_kw": round(lighting_kw, 2),
                    "b_equipment_kw": round(equipment_kw, 2),
                    "b_indoor_temp": round(self.indoor_temp, 1),
                    "b_occupancy": int(self.occupancy),
                    "b_indoor_aqi": indoor_aqi,
                    "b_equip_health": round(self.equipment_health, 1),
                    "b_comfort_index": round(comfort_index, 1),
                    "b_energy_efficiency": round(energy_efficiency, 2),
                    
                    # Financial Metrics
                    "b_cost_rate": round(actual_cost_rate, 4),
                    "b_cost_per_occupant": round(actual_cost_rate / max(1, self.occupancy), 4),
                    "b_carbon_rate": round(carbon_intensity_building, 3),
                    "b_carbon_per_occupant": round(carbon_intensity_building / max(1, self.occupancy), 4),
                    
                    # Grid & Renewable Metrics
                    "grid_renewable_pct": renewable_pct,
                    "grid_carbon_intensity": grid_carbon,
                    "grid_uk_wind_pct": uk_gen_wind,
                    "grid_uk_solar_pct": uk_gen_solar,
                    "grid_uk_nuclear_pct": uk_gen_nuclear,
                    
                    # Operational Flags
                    "is_peak_hour": is_peak,
                    "is_day_time": is_day,
                    "weather_cloud_cover": cloud_cover,
                    
                    # Add all deep IoT metrics
                    **deep_metrics
                })

                self.producer.send(TOPIC, value=payload)
                time.sleep(1)  # Produce every 1 second as requested
                
            except Exception as e:
                print(f"Error in {self.building_id}: {e}")
                time.sleep(1)

if __name__ == "__main__":
    profiles = [
        {
            "id": "Building-A-HQ", 
            "insulation_factor": 0.2, 
            "solar_capacity": 50, 
            "base_occupancy": 600, 
            "base_lighting": 30, 
            "base_equipment": 150
        },
        {
            "id": "Building-B-Lab", 
            "insulation_factor": 0.1, 
            "solar_capacity": 20, 
            "base_occupancy": 150, 
            "base_lighting": 60, 
            "base_equipment": 400
        },
        {
            "id": "Building-C-Warehouse", 
            "insulation_factor": 0.5, 
            "solar_capacity": 250, 
            "base_occupancy": 30, 
            "base_lighting": 120, 
            "base_equipment": 80
        },
    ]

    threads = []
    for p in profiles:
        t = DigitalTwin(p['id'], p)
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()