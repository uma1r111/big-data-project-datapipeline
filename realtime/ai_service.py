from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import json
import os
import google.generativeai as genai
from pymongo import MongoClient

app = FastAPI()

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemma-3-27b-it') 

# Connect to Redis & Mongo
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
mongo_client = MongoClient(f"mongodb://{MONGO_HOST}:27017/")
db = mongo_client["ecotwin"]
collection = db["telemetry"]

class ChatRequest(BaseModel):
    query: str
    context_type: str  # "live" or "history"
    building_id: str
    time_range_hours: int = 1

@app.post("/ask")
async def ask_gemini(req: ChatRequest):
    try:
        context_data = ""
        
        if req.context_type == "live":
            # Fetch latest data from Redis
            raw = r.get(f"live:{req.building_id}")
            context_data = f"Current Live Snapshot: {raw}" if raw else "No live data available."
                
        elif req.context_type == "history":
            # Fetch last N records from MongoDB
            limit = 20  # Limit tokens for AI
            cursor = collection.find({"building_id": req.building_id})\
                               .sort("timestamp_gen", -1)\
                               .limit(limit)
            
            docs = list(cursor)
            # Remove ObjectId for JSON serialization
            for doc in docs:
                doc.pop('_id', None)
                doc.pop('raw_json', None) # Reduce noise
            
            context_data = f"Last {limit} data points from Database: {json.dumps(docs)}"

        # Prompt Engineering
        system_prompt = """
        You are EcoBot, an AI Facility Manager.
        Analyze the provided telemetry data.
        1. Identify anomalies (Temp > 24C, CO2 > 1000ppm).
        2. Suggest energy saving actions.
        3. Be concise.
        """
        
        full_prompt = f"{system_prompt}\n\nDATA CONTEXT:\n{context_data}\n\nUSER QUESTION:\n{req.query}"

        response = model.generate_content(full_prompt)
        return {"answer": response.text}

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))