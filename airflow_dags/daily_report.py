from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
import json
import os
from pymongo import MongoClient

default_args = {
    'owner': 'ecotwin',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def generate_daily_bill():
    """
    Reads from MongoDB (Persistent Storage)
    Generates a billing report
    Saves to the Shared Data Lake Volume
    """
    print("✅ Connecting to MongoDB...")
    
    try:
        # Connect to MongoDB Service
        client = MongoClient("mongodb://mongodb:27017/")
        db = client["ecotwin"]
        collection = db["telemetry"]
        
        # Count total records (Simulating complex billing logic)
        count = collection.count_documents({})
        print(f"📊 Found {count} records in MongoDB.")
        
        report = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "status": "Billing Generated",
            "data_source": "MongoDB",
            "total_records_processed": count,
            "total_estimated_cost": count * 0.05, # Mock cost: 5 cents per record
            "currency": "USD"
        }
        
        # Save Report to Shared Volume (Mapped to /opt/airflow/datalake)
        output_dir = "/opt/airflow/datalake/reports"
        os.makedirs(output_dir, exist_ok=True)
        
        filename = f"{output_dir}/billing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=4)
            
        print(f"💰 Daily Billing Report Saved to: {filename}")
        
    except Exception as e:
        print(f"⚠️ Error generating bill: {e}")

with DAG('ecotwin_daily_billing',
         default_args=default_args,
         schedule_interval='*/1 * * * *',  # Runs every minute,
         catchup=False) as dag:

    generate_bill_task = PythonOperator(
        task_id='calculate_daily_bill',
        python_callable=generate_daily_bill
    )