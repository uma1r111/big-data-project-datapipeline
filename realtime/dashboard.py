import streamlit as st
import redis
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

st.set_page_config(
    page_title="EcoSmart Building Monitor",
    layout="wide",
    page_icon="⚡"
)

# Header
st.title("⚡ EcoSmart: Portfolio Energy & Carbon Monitor")
st.markdown("Real-time telemetry from Building IoT Sensors | **Pipeline: Kafka -> Spark -> Redis**")

# Layout: Top KPIs
kpi1, kpi2, kpi3, kpi4 = st.columns(4)
metric_load = kpi1.empty()
metric_cost = kpi2.empty()
metric_co2 = kpi3.empty()
metric_temp = kpi4.empty()

# Layout: Charts
col_main, col_side = st.columns([2, 1])

with col_main:
    st.subheader("Live Energy Consumption (kW)")
    chart_load = st.empty()

with col_side:
    st.subheader("Grid Carbon Intensity")
    gauge_carbon = st.empty()
    st.info("High Carbon Intensity (>200g) indicates the grid is using fossil fuels. Low (<100g) indicates renewables.")

# History Storage for Charts
if "history" not in st.session_state:
    st.session_state.history = []

def get_redis_val(key, default=0.0):
    val = r.get(key)
    return float(val) if val else default

while True:
    # 1. Fetch Data from Redis (Instant)
    total_kwh = get_redis_val("total_kwh")
    total_cost = get_redis_val("total_cost")
    total_co2 = get_redis_val("total_co2")
    avg_temp = get_redis_val("avg_temp")
    avg_carbon_int = get_redis_val("avg_carbon_intensity")
    grid_price = get_redis_val("avg_grid_price")

    # 2. Update KPI Cards
    metric_load.metric("Total Load", f"{total_kwh:.2f} kW", delta_color="inverse")
    metric_cost.metric("Current Cost", f"£{total_cost:.2f} /hr", f"Price: £{grid_price:.2f}/kWh")
    metric_co2.metric("Carbon Emissions", f"{total_co2:.2f} kgCO2", delta_color="inverse")
    metric_temp.metric("Avg Outdoor Temp", f"{avg_temp:.1f} °C")

    # 3. Update History for Charts
    current_time = pd.Timestamp.now()
    st.session_state.history.append({
        "Time": current_time, 
        "Load (kW)": total_kwh,
        "Cost (£)": total_cost,
        "CO2 (kg)": total_co2
    })
    
    # Keep last 60 seconds of data
    if len(st.session_state.history) > 60:
        st.session_state.history.pop(0)
    
    df_hist = pd.DataFrame(st.session_state.history)

    # 4. Render Line Chart (Load)
    if not df_hist.empty:
        fig_line = px.area(
            df_hist, 
            x='Time', 
            y='Load (kW)', 
            title="Portfolio Energy Demand",
            color_discrete_sequence=["#00CC96"]
        )
        fig_line.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=20))
        chart_load.plotly_chart(fig_line, use_container_width=True)

    # 5. Render Gauge (Carbon Intensity)
    fig_gauge = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = avg_carbon_int,
        title = {'text': "gCO2 / kWh"},
        gauge = {
            'axis': {'range': [None, 400]},
            'bar': {'color': "black"},
            'steps': [
                {'range': [0, 100], 'color': "lightgreen"}, # Clean
                {'range': [100, 250], 'color': "yellow"},   # Moderate
                {'range': [250, 400], 'color': "red"}       # Dirty
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 300
            }
        }
    ))
    fig_gauge.update_layout(height=300, margin=dict(l=20, r=20, t=10, b=20))
    gauge_carbon.plotly_chart(fig_gauge, use_container_width=True)

    time.sleep(1)