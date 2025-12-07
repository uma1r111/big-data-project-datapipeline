import time
import redis
import json
from datetime import datetime

# CONFIG
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

# CONNECTION
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def check_alerts():
    print("🛡️  Alert Engine Started... Monitoring Rules.")
    
    while True:
        try:
            # 1. FETCH LIVE DATA
            current_load = float(r.get("total_kwh") or 0)
            current_cost = float(r.get("total_cost") or 0)
            carbon_intensity = float(r.get("avg_carbon_intensity") or 0)
            
            # 2. FETCH USER RULES (Stored in Redis by the Dashboard)
            # Default rules if none exist
            rules = json.loads(r.get("alert_config") or '{"max_load": 400, "max_cost": 50.0, "max_carbon": 250}')
            
            alerts = []

            # 3. EVALUATE LOGIC
            # Rule A: Peak Load
            if current_load > rules['max_load']:
                alerts.append({
                    "severity": "CRITICAL",
                    "msg": f"Grid Overload: {current_load} kW exceeds limit ({rules['max_load']} kW)",
                    "category": "Operations"
                })

            # Rule B: High Cost
            if current_cost > rules['max_cost']:
                alerts.append({
                    "severity": "WARNING",
                    "msg": f"Cost Surge: £{current_cost}/hr is expensive.",
                    "category": "Finance"
                })

            # Rule C: Dirty Energy
            if carbon_intensity > rules['max_carbon']:
                alerts.append({
                    "severity": "INFO",
                    "msg": f"High Pollution: Grid carbon is {carbon_intensity}g.",
                    "category": "Sustainability"
                })

            # 4. PUBLISH ALERTS
            if alerts:
                timestamp = datetime.now().strftime("%H:%M:%S")
                for alert in alerts:
                    alert_obj = {**alert, "time": timestamp}
                    # Push to a Redis List (History)
                    r.lpush("alert_history", json.dumps(alert_obj))
                    # Keep only last 50 alerts
                    r.ltrim("alert_history", 0, 49)
                    print(f"⚠️  ALERT TRIGGERED: {alert['msg']}")

        except Exception as e:
            print(f"Error in Alert Engine: {e}")
        
        time.sleep(2) # Check every 2 seconds

if __name__ == "__main__":
    check_alerts()