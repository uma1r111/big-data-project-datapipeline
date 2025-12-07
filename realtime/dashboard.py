import streamlit as st
import redis
import time
import pandas as pd
import plotly.express as px

# Connect to Redis (Localhost because Streamlit runs on host)
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

st.set_page_config(page_title="Energy AI Monitor", layout="wide")
st.title("⚡ Smart Building Energy Optimizer")

# UI Layout
col1, col2, col3 = st.columns(3)
kwh_placeholder = col1.empty()
cost_placeholder = col2.empty()
status_placeholder = col3.empty()

chart_placeholder = st.empty()

# Initialization
if "history" not in st.session_state:
    st.session_state.history = []

def get_data():
    try:
        kwh = float(r.get("total_kwh") or 0)
        cost = float(r.get("total_cost") or 0)
        return kwh, cost
    except:
        return 0.0, 0.0

# Main Loop
while True:
    kwh, cost = get_data()
    
    # Update KPIs
    kwh_placeholder.metric("Live Portfolio Load", f"{kwh:.2f} kW")
    cost_placeholder.metric("Current Cost Rate", f"£{cost:.2f} /hr")
    
    # Smart Alert Logic
    if kwh > 600:
        status_placeholder.error("⚠️ PEAK LOAD EXCEEDED")
    elif kwh > 400:
        status_placeholder.warning("High Demand")
    else:
        status_placeholder.success("✅ Optimized")

    # Update Chart History
    timestamp = pd.Timestamp.now().strftime('%H:%M:%S')
    st.session_state.history.append({"Time": timestamp, "Load": kwh})
    
    # Keep last 30 data points
    if len(st.session_state.history) > 30:
        st.session_state.history.pop(0)

    # Draw Chart
    df = pd.DataFrame(st.session_state.history)
    if not df.empty:
        fig = px.area(df, x='Time', y='Load', title="Real-Time Power Consumption")
        chart_placeholder.plotly_chart(fig, use_container_width=True)

    time.sleep(1) # Refresh every second