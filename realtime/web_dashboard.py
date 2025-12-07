import streamlit as st
import redis
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from influxdb import InfluxDBClient
from streamlit_autorefresh import st_autorefresh
import json

# --- CONFIGURATION ---
st.set_page_config(page_title="EcoGrid Enterprise", layout="wide", page_icon="⚡")
st_autorefresh(interval=2000, key="datarefresh") # Auto-refresh every 2s

# --- CONNECTIONS ---
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
influx = InfluxDBClient(host='localhost', port=8086, database='energy_db')

# --- CUSTOM CSS (THE "COOL" LOOK) ---
st.markdown("""
<style>
    /* Dark Theme Background */
    .stApp { background-color: #0E1117; }
    
    /* Metrics Cards */
    div[data-testid="stMetric"] {
        background-color: #1E1E1E;
        border: 1px solid #333;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.5);
    }
    div[data-testid="stMetricValue"] { font-size: 24px; color: #00FFC2; }
    
    /* Alert Box Styling */
    .alert-box {
        padding: 10px;
        border-radius: 5px;
        margin-bottom: 5px;
        font-family: monospace;
    }
    .crt { background-color: #521818; color: #ff9999; border-left: 5px solid red; }
    .wrn { background-color: #524718; color: #ffff99; border-left: 5px solid orange; }
    .inf { background-color: #182a52; color: #99ccff; border-left: 5px solid blue; }
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR: SETTINGS & RULES ---
with st.sidebar:
    st.header("⚙️ Control Panel")
    st.subheader("Alert Thresholds")
    
    # Load existing rules
    current_rules = json.loads(r.get("alert_config") or '{"max_load": 400, "max_cost": 50.0, "max_carbon": 250}')
    
    new_max_load = st.slider("Max Load (kW)", 100, 1000, current_rules['max_load'])
    new_max_cost = st.slider("Max Cost (£/hr)", 10.0, 200.0, float(current_rules['max_cost']))
    new_max_carbon = st.slider("Max Carbon (gCO2)", 50, 500, current_rules['max_carbon'])
    
    if st.button("💾 Update Rules"):
        new_config = {"max_load": new_max_load, "max_cost": new_max_cost, "max_carbon": new_max_carbon}
        r.set("alert_config", json.dumps(new_config))
        st.success("Rules Updated!")

# --- DATA FETCHING ---
def get_live_metrics():
    return {
        "load": float(r.get("total_kwh") or 0),
        "cost": float(r.get("total_cost") or 0),
        "co2": float(r.get("total_co2") or 0),
        "temp": float(r.get("avg_temp") or 0),
        "grid_co2": float(r.get("avg_carbon_intensity") or 0)
    }

def get_history():
    try:
        query = 'SELECT mean("kwh") as kwh, mean("cost") as cost FROM "building_metrics" WHERE time > now() - 30m GROUP BY time(1m)'
        result = influx.query(query)
        points = list(result.get_points())
        return pd.DataFrame(points)
    except:
        return pd.DataFrame()

data = get_live_metrics()

# --- DASHBOARD LAYOUT ---

# 1. HEADER
st.title("⚡ EcoGrid Enterprise Ops")
st.markdown("Real-time telemetry and anomaly detection system.")

# 2. KPI ROW
k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Load", f"{data['load']:.1f} kW", delta=f"{data['load'] - 300:.1f}")
k2.metric("Burn Rate", f"£{data['cost']:.2f} /hr", delta_color="inverse")
k3.metric("Grid Carbon", f"{data['grid_co2']:.0f} g/kWh", delta_color="inverse")
k4.metric("Outdoor Temp", f"{data['temp']:.1f} °C")

# 3. MAIN CHARTS & ALERTS
c1, c2 = st.columns([2, 1])

with c1:
    st.subheader("📈 Live Consumption Trends")
    df_hist = get_history()
    
    if not df_hist.empty:
        df_hist['time'] = pd.to_datetime(df_hist['time'])
        
        # Dual Axis Chart
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_hist['time'], y=df_hist['kwh'], name='Load (kW)',
                                 line=dict(color='#00FFC2', width=3), fill='tozeroy'))
        fig.add_trace(go.Scatter(x=df_hist['time'], y=df_hist['cost'], name='Cost (£)',
                                 line=dict(color='#FF0055', width=2), yaxis='y2'))
        
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            xaxis=dict(showgrid=False),
            yaxis=dict(title='Load (kW)', showgrid=True, gridcolor='#333'),
            yaxis2=dict(title='Cost (£)', overlaying='y', side='right'),
            margin=dict(l=0, r=0, t=10, b=0),
            height=350,
            legend=dict(orientation="h", y=1.1)
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Waiting for InfluxDB history...")

with c2:
    st.subheader("🔔 Live Alert Feed")
    
    # Fetch Alert Logs from Redis List
    raw_alerts = r.lrange("alert_history", 0, 6) # Get last 7 alerts
    
    if raw_alerts:
        for a in raw_alerts:
            alert = json.loads(a)
            # Determine CSS class based on severity
            css_class = "crt" if alert['severity'] == "CRITICAL" else "wrn" if alert['severity'] == "WARNING" else "inf"
            
            st.markdown(f"""
            <div class='alert-box {css_class}'>
                <b>[{alert['time']}] {alert['severity']}</b><br>
                {alert['msg']}
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("✅ Systems Nominal. No active alerts.")

# 4. BOTTOM ROW: BREAKDOWN
st.subheader("🏗️ Building Performance Heatmap")

# Fake breakdown for visualization (since we aggregate in Spark)
# In a real app, you'd query specific building tags from InfluxDB
heatmap_data = pd.DataFrame({
    'Building': ['HQ-London', 'ServerFarm', 'Warehouse-A'],
    'Load': [data['load']*0.3, data['load']*0.6, data['load']*0.1],
    'Efficiency': [95, 82, 98],
    'Status': ['Optimal', 'High Load', 'Eco Mode']
})

col_heat, col_gauge = st.columns([2,1])

with col_heat:
    fig_bar = px.bar(heatmap_data, x='Load', y='Building', orientation='h', 
                     color='Status', text_auto=True,
                     color_discrete_map={'Optimal': '#00CC96', 'High Load': '#EF553B', 'Eco Mode': '#636EFA'})
    fig_bar.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font=dict(color='white'))
    st.plotly_chart(fig_bar, use_container_width=True)

with col_gauge:
    # Carbon Gauge
    fig_g = go.Figure(go.Indicator(
        mode = "gauge+number", value = data['grid_co2'],
        title = {'text': "Sustainability Index"},
        gauge = {
            'axis': {'range': [None, 400]},
            'bar': {'color': "white"},
            'steps': [
                {'range': [0, 150], 'color': "#00CC96"},
                {'range': [150, 300], 'color': "#FFA15A"},
                {'range': [300, 400], 'color': "#EF553B"}
            ]
        }
    ))
    fig_g.update_layout(height=250, margin=dict(t=30, b=10, l=30, r=30), paper_bgcolor='rgba(0,0,0,0)', font=dict(color='white'))
    st.plotly_chart(fig_g, use_container_width=True)