import streamlit as st
import redis
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from streamlit_autorefresh import st_autorefresh
from datetime import datetime
import requests

# --- 1. PAGE CONFIG ---
st.set_page_config(
    page_title="EcoTwin Enterprise | Real-Time Monitoring", 
    layout="wide", 
    page_icon="🏢",
    initial_sidebar_state="expanded"
)

# Auto-refresh every 5 seconds (5000 ms)
st_autorefresh(interval=5000, key="data_refresh")

# --- 2. ENHANCED THEME ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
    
    * { font-family: 'Inter', sans-serif; }
    
    .stApp {
        background: linear-gradient(135deg, #0f2027 0%, #203a43 50%, #2c5364 100%);
        color: #e8eaf6;
    }
    
    /* Glass Morphism Cards */
    .glass-card {
        background: linear-gradient(135deg, rgba(255,255,255,0.08) 0%, rgba(255,255,255,0.03) 100%);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255,255,255,0.12);
        border-radius: 16px;
        padding: 20px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.4);
        transition: all 0.3s ease;
    }
    
    .glass-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 12px 40px rgba(0,0,0,0.5);
    }
    
    /* Metric Styles */
    .metric-container {
        background: rgba(0,0,0,0.2);
        border-radius: 12px;
        padding: 16px;
        border-left: 4px solid;
        margin: 8px 0;
    }
    
    .metric-title {
        font-size: 11px;
        color: #b0bec5;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 6px;
        font-weight: 600;
    }
    
    .metric-value {
        font-size: 28px;
        font-weight: 700;
        color: #ffffff;
        line-height: 1.2;
    }
    
    .metric-unit {
        font-size: 14px;
        color: #90a4ae;
        margin-left: 4px;
    }
    
    .metric-change {
        font-size: 12px;
        margin-top: 4px;
    }
    
    /* Alert Styles */
    .alert-critical {
        background: linear-gradient(135deg, rgba(244,67,54,0.2) 0%, rgba(244,67,54,0.1) 100%);
        border-left-color: #f44336 !important;
        animation: pulse 2s infinite;
    }
    
    .alert-warning {
        background: linear-gradient(135deg, rgba(255,152,0,0.2) 0%, rgba(255,152,0,0.1) 100%);
        border-left-color: #ff9800 !important;
    }
    
    .alert-info {
        background: linear-gradient(135deg, rgba(33,150,243,0.2) 0%, rgba(33,150,243,0.1) 100%);
        border-left-color: #2196f3 !important;
    }
    
    .alert-success {
        background: linear-gradient(135deg, rgba(76,175,80,0.2) 0%, rgba(76,175,80,0.1) 100%);
        border-left-color: #4caf50 !important;
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }
    
    /* Status Badges */
    .status-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .status-online {
        background: rgba(76,175,80,0.2);
        color: #4caf50;
        border: 1px solid #4caf50;
    }
    
    .status-offline {
        background: rgba(244,67,54,0.2);
        color: #f44336;
        border: 1px solid #f44336;
    }
    
    /* Sensor Grid */
    .sensor-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
        gap: 12px;
        margin-top: 12px;
    }
    
    .sensor-box {
        background: rgba(0,0,0,0.3);
        padding: 12px;
        border-radius: 10px;
        text-align: center;
        border: 1px solid rgba(255,255,255,0.1);
        transition: all 0.3s ease;
    }
    
    .sensor-box:hover {
        background: rgba(0,0,0,0.4);
        border-color: #00e676;
    }
    
    .sensor-label {
        font-size: 10px;
        color: #90a4ae;
        text-transform: uppercase;
        margin-bottom: 6px;
    }
    
    .sensor-value {
        font-size: 18px;
        font-weight: 700;
        color: #ffffff;
    }
    
    /* Header Styling */
    h1, h2, h3 {
        color: #ffffff !important;
        font-weight: 700 !important;
    }
    
    /* Sidebar */
    .css-1d391kg {
        background: rgba(0,0,0,0.3);
    }
    
    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: rgba(0,0,0,0.2);
    }
    
    ::-webkit-scrollbar-thumb {
        background: rgba(255,255,255,0.2);
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: rgba(255,255,255,0.3);
    }
</style>
""", unsafe_allow_html=True)

# --- 3. INITIALIZE SESSION STATE ---
if "history" not in st.session_state:
    st.session_state.history = {}

if "alert_thresholds" not in st.session_state:
    st.session_state.alert_thresholds = {
        "temp_max": 24.0,
        "temp_min": 18.0,
        "co2_max": 1000,
        "power_factor_min": 0.90,
        "equipment_health_min": 80.0,
        "comfort_min": 70.0,
        "load_max": 500.0,
        "aqi_max": 100
    }

if "active_alerts" not in st.session_state:
    st.session_state.active_alerts = []

# --- 4. REDIS CONNECTION ---
r = redis.Redis(host='redis', port=6379, decode_responses=True)
buildings = ["Building-A-HQ", "Building-B-Lab", "Building-C-Warehouse"]

# --- 5. DATA FETCH & ALERT GENERATION ---
def check_alerts(curr, building_id):
    """Generate alerts based on current metrics and thresholds"""
    alerts = []
    t = st.session_state.alert_thresholds
    
    # Temperature alerts
    temp = curr.get('b_indoor_temp', 21)
    if temp > t['temp_max']:
        alerts.append({
            'level': 'critical',
            'building': building_id,
            'metric': 'Temperature',
            'message': f"High temperature: {temp}°C",
            'value': temp,
            'timestamp': datetime.now()
        })
    elif temp < t['temp_min']:
        alerts.append({
            'level': 'warning',
            'building': building_id,
            'metric': 'Temperature',
            'message': f"Low temperature: {temp}°C",
            'value': temp,
            'timestamp': datetime.now()
        })
    
    # CO2 alerts
    co2 = curr.get('ieq_co2_ppm', 400)
    if co2 > t['co2_max']:
        alerts.append({
            'level': 'warning',
            'building': building_id,
            'metric': 'Air Quality',
            'message': f"High CO2: {co2} ppm",
            'value': co2,
            'timestamp': datetime.now()
        })
    
    # Power factor alerts
    pf = curr.get('elec_power_factor', 0.95)
    if pf < t['power_factor_min']:
        alerts.append({
            'level': 'info',
            'building': building_id,
            'metric': 'Power Quality',
            'message': f"Low power factor: {pf:.2f}",
            'value': pf,
            'timestamp': datetime.now()
        })
    
    # Equipment health alerts
    health = curr.get('b_equip_health', 100)
    if health < t['equipment_health_min']:
        alerts.append({
            'level': 'warning',
            'building': building_id,
            'metric': 'Equipment',
            'message': f"Equipment health low: {health:.1f}%",
            'value': health,
            'timestamp': datetime.now()
        })
    
    # Comfort index alerts
    comfort = curr.get('b_comfort_index', 100)
    if comfort < t['comfort_min']:
        alerts.append({
            'level': 'info',
            'building': building_id,
            'metric': 'Comfort',
            'message': f"Comfort index low: {comfort:.1f}",
            'value': comfort,
            'timestamp': datetime.now()
        })
    
    # Load alerts
    load = curr.get('b_load_kw', 0)
    if load > t['load_max']:
        alerts.append({
            'level': 'critical',
            'building': building_id,
            'metric': 'Power',
            'message': f"High load: {load:.1f} kW",
            'value': load,
            'timestamp': datetime.now()
        })
    
    # AQI alerts
    aqi = curr.get('b_indoor_aqi', 50)
    if aqi > t['aqi_max']:
        alerts.append({
            'level': 'warning',
            'building': building_id,
            'metric': 'Air Quality',
            'message': f"Poor indoor AQI: {aqi}",
            'value': aqi,
            'timestamp': datetime.now()
        })
    
    return alerts

def update_state():
    """Fetch data from Redis and update session state"""
    st.session_state.active_alerts = []
    
    for bid in buildings:
        raw = r.get(f"live:{bid}")
        if raw:
            try:
                d = json.loads(raw)
                d['ui_time'] = datetime.now().strftime("%H:%M:%S")
                
                if bid not in st.session_state.history:
                    st.session_state.history[bid] = []
                
                st.session_state.history[bid].append(d)
                
                # Keep last 60 data points (5 minutes at 5-second updates)
                if len(st.session_state.history[bid]) > 60:
                    st.session_state.history[bid].pop(0)
                
                # Generate alerts
                alerts = check_alerts(d, bid)
                st.session_state.active_alerts.extend(alerts)
                
            except Exception as e:
                st.error(f"Error parsing data for {bid}: {e}")

update_state()

# --- 6. HELPER FUNCTIONS ---
def render_metric_card(label, value, unit="", delta=None, color="blue"):
    """Render a modern metric card"""
    color_map = {
        'blue': '#2196f3',
        'green': '#4caf50',
        'orange': '#ff9800',
        'red': '#f44336',
        'purple': '#9c27b0'
    }
    
    border_color = color_map.get(color, '#2196f3')
    
    delta_html = ""
    if delta is not None:
        delta_color = "#4caf50" if delta >= 0 else "#f44336"
        delta_symbol = "▲" if delta >= 0 else "▼"
        delta_html = f'<div class="metric-change" style="color: {delta_color};">{delta_symbol} {abs(delta):.1f}%</div>'
    
    st.markdown(f"""
    <div class="metric-container" style="border-left-color: {border_color};">
        <div class="metric-title">{label}</div>
        <div class="metric-value">{value}<span class="metric-unit">{unit}</span></div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)

def render_sensor_mini(label, value, unit=""):
    """Render mini sensor display"""
    st.markdown(f"""
    <div class="sensor-box">
        <div class="sensor-label">{label}</div>
        <div class="sensor-value">{value}<span style="font-size:12px; color:#90a4ae;">{unit}</span></div>
    </div>
    """, unsafe_allow_html=True)

def render_gauge(val, title, min_v, max_v, color="#00e676"):
    """Render a gauge chart"""
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=val,
        title={'text': title, 'font': {'size': 14, 'color': 'white'}},
        delta={'reference': (max_v + min_v) / 2},
        gauge={
            'axis': {'range': [min_v, max_v], 'tickcolor': 'white'},
            'bar': {'color': color},
            'bgcolor': "rgba(0,0,0,0.2)",
            'borderwidth': 2,
            'bordercolor': "rgba(255,255,255,0.2)",
            'steps': [
                {'range': [min_v, (max_v-min_v)*0.33 + min_v], 'color': 'rgba(76,175,80,0.2)'},
                {'range': [(max_v-min_v)*0.33 + min_v, (max_v-min_v)*0.66 + min_v], 'color': 'rgba(255,152,0,0.2)'},
                {'range': [(max_v-min_v)*0.66 + min_v, max_v], 'color': 'rgba(244,67,54,0.2)'}
            ]
        }
    ))
    fig.update_layout(
        height=200,
        margin=dict(t=40, b=10, l=20, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        font_color="white"
    )
    return fig

# --- 7. SIDEBAR - ALERT CONFIGURATION ---
with st.sidebar:
    st.markdown("## ⚙️ Alert Configuration")
    st.markdown("Set thresholds for automated alerts")
    
    with st.expander("🌡️ Temperature", expanded=False):
        st.session_state.alert_thresholds['temp_max'] = st.number_input(
            "Max Temperature (°C)", 
            value=st.session_state.alert_thresholds['temp_max'],
            min_value=15.0,
            max_value=30.0,
            step=0.5
        )
        st.session_state.alert_thresholds['temp_min'] = st.number_input(
            "Min Temperature (°C)",
            value=st.session_state.alert_thresholds['temp_min'],
            min_value=15.0,
            max_value=25.0,
            step=0.5
        )
    
    with st.expander("🌫️ Air Quality", expanded=False):
        st.session_state.alert_thresholds['co2_max'] = st.number_input(
            "Max CO2 (ppm)",
            value=st.session_state.alert_thresholds['co2_max'],
            min_value=400,
            max_value=2000,
            step=100
        )
        st.session_state.alert_thresholds['aqi_max'] = st.number_input(
            "Max AQI",
            value=st.session_state.alert_thresholds['aqi_max'],
            min_value=50,
            max_value=200,
            step=10
        )
    
    with st.expander("⚡ Power & Equipment", expanded=False):
        st.session_state.alert_thresholds['load_max'] = st.number_input(
            "Max Load (kW)",
            value=st.session_state.alert_thresholds['load_max'],
            min_value=100.0,
            max_value=1000.0,
            step=50.0
        )
        st.session_state.alert_thresholds['power_factor_min'] = st.number_input(
            "Min Power Factor",
            value=st.session_state.alert_thresholds['power_factor_min'],
            min_value=0.8,
            max_value=1.0,
            step=0.01
        )
        st.session_state.alert_thresholds['equipment_health_min'] = st.number_input(
            "Min Equipment Health (%)",
            value=st.session_state.alert_thresholds['equipment_health_min'],
            min_value=50.0,
            max_value=100.0,
            step=5.0
        )
    
    with st.expander("😊 Comfort", expanded=False):
        st.session_state.alert_thresholds['comfort_min'] = st.number_input(
            "Min Comfort Index",
            value=st.session_state.alert_thresholds['comfort_min'],
            min_value=50.0,
            max_value=100.0,
            step=5.0
        )
    
    st.markdown("---")
    st.markdown("### 📊 System Status")
    st.markdown(f'<span class="status-badge status-online">● ONLINE</span>', unsafe_allow_html=True)
    st.caption(f"Last Update: {datetime.now().strftime('%H:%M:%S')}")


    st.markdown("---")
    st.markdown("### 🤖 AI Assistant")
    st.page_link("pages/EcoBot_AI_Assistant.py", label="Open EcoBot Chat", icon="🤖")

# --- 8. MAIN DASHBOARD ---
st.title("🏙️ EcoTwin Enterprise | Real-Time Monitoring Dashboard")

# Portfolio Overview
try:
    portfolio_load = float(r.get("portfolio_total_load") or 0)
    portfolio_cost = float(r.get("portfolio_total_cost") or 0)
    portfolio_carbon = float(r.get("portfolio_total_carbon") or 0)
    portfolio_occupancy = int(r.get("portfolio_total_occupancy") or 0)
    portfolio_comfort = float(r.get("portfolio_avg_comfort") or 0)
    portfolio_efficiency = float(r.get("portfolio_avg_efficiency") or 0)
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        render_metric_card("Total Load", f"{portfolio_load:.1f}", "kW", color="blue")
    with col2:
        render_metric_card("Cost Rate", f"£{portfolio_cost:.2f}", "/hr", color="orange")
    with col3:
        render_metric_card("Carbon", f"{portfolio_carbon:.2f}", "kgCO2", color="red")
    with col4:
        render_metric_card("Occupancy", f"{portfolio_occupancy}", "people", color="purple")
    with col5:
        render_metric_card("Comfort", f"{portfolio_comfort:.1f}", "%", color="green")
    with col6:
        render_metric_card("Efficiency", f"{portfolio_efficiency:.1f}", "", color="blue")
        
except Exception as e:
    st.error(f"Error fetching portfolio data: {e}")

st.markdown("---")

# Main Layout: 3 columns - Buildings (70%) + Alerts (30%)
main_col, alert_col = st.columns([7, 3])

with main_col:
    # Building Tabs
    tabs = st.tabs(buildings)
    
    for i, bid in enumerate(buildings):
        with tabs[i]:
            hist = st.session_state.history.get(bid, [])
            
            if not hist:
                st.warning(f"⏳ Waiting for data from {bid}...")
                continue
            
            curr = hist[-1]
            
            # === TOP ROW: KEY METRICS ===
            c1, c2, c3, c4 = st.columns(4)
            
            with c1:
                st.plotly_chart(
                    render_gauge(
                        curr.get('b_equip_health', 0), 
                        "Equipment Health", 
                        0, 100,
                        "#4caf50"
                    ), 
                    use_container_width=True,
                    config={'displayModeBar': False},
                    key=f"gauge_health_{bid}"
                )
            
            with c2:
                st.plotly_chart(
                    render_gauge(
                        curr.get('b_comfort_index', 0),
                        "Comfort Index",
                        0, 100,
                        "#2196f3"
                    ),
                    use_container_width=True,
                    config={'displayModeBar': False},
                    key=f"gauge_comfort_{bid}"
                )
            
            with c3:
                st.plotly_chart(
                    render_gauge(
                        curr.get('b_indoor_aqi', 50),
                        "Indoor AQI",
                        0, 150,
                        "#ff9800"
                    ),
                    use_container_width=True,
                    config={'displayModeBar': False},
                    key=f"gauge_aqi_{bid}"
                )
            
            with c4:
                st.plotly_chart(
                    render_gauge(
                        curr.get('elec_power_factor', 0.95) * 100,
                        "Power Factor",
                        85, 100,
                        "#9c27b0"
                    ),
                    use_container_width=True,
                    config={'displayModeBar': False},
                    key=f"gauge_pf_{bid}"
                )
            
            # === TIME SERIES CHARTS ===
            st.markdown("### 📈 Real-Time Analytics")
            
            df = pd.DataFrame(hist)
            
            # Power & Energy Chart
            fig_power = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Active Power', 'Temperature', 'Occupancy', 'Carbon Intensity'),
                vertical_spacing=0.12,
                horizontal_spacing=0.1
            )
            
            # Active Power
            fig_power.add_trace(
                go.Scatter(
                    x=df['ui_time'],
                    y=df['b_load_kw'],
                    name='Load (kW)',
                    fill='tozeroy',
                    line=dict(color='#00e676', width=2)
                ),
                row=1, col=1
            )
            
            # Temperature
            fig_power.add_trace(
                go.Scatter(
                    x=df['ui_time'],
                    y=df['b_indoor_temp'],
                    name='Indoor Temp',
                    line=dict(color='#ff9800', width=2)
                ),
                row=1, col=2
            )
            fig_power.add_trace(
                go.Scatter(
                    x=df['ui_time'],
                    y=df['hvac_outdoor_temp'],
                    name='Outdoor Temp',
                    line=dict(color='#2196f3', width=2, dash='dash')
                ),
                row=1, col=2
            )
            
            # Occupancy
            fig_power.add_trace(
                go.Scatter(
                    x=df['ui_time'],
                    y=df['b_occupancy'],
                    name='Occupancy',
                    fill='tozeroy',
                    line=dict(color='#9c27b0', width=2)
                ),
                row=2, col=1
            )
            
            # Carbon
            fig_power.add_trace(
                go.Scatter(
                    x=df['ui_time'],
                    y=df['grid_carbon_intensity'],
                    name='Grid Carbon',
                    line=dict(color='#f44336', width=2)
                ),
                row=2, col=2
            )
            
            fig_power.update_layout(
                height=500,
                showlegend=True,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(255,255,255,0.05)",
                font_color="white",
                margin=dict(t=40, b=20, l=20, r=20)
            )
            fig_power.update_xaxes(showgrid=False, color='white')
            fig_power.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)', color='white')
            
            st.plotly_chart(fig_power, use_container_width=True, config={'displayModeBar': False}, key=f"chart_power_{bid}")
            
            # === DETAILED METRICS EXPANDABLE SECTIONS ===
            st.markdown("### 🔍 Detailed Telemetry")
            
            # 1. Electrical Grid
            with st.expander("⚡ Electrical Grid & Power Quality", expanded=False):
                col1, col2 = st.columns([1, 2])
                
                with col1:
                    render_metric_card("Reactive Power", f"{curr.get('elec_reactive_kvar', 0):.1f}", "kVAR", color="orange")
                    render_metric_card("Apparent Power", f"{curr.get('elec_apparent_kva', 0):.1f}", "kVA", color="purple")
                    render_metric_card("Frequency", f"{curr.get('elec_frequency', 50):.2f}", "Hz", color="blue")
                
                with col2:
                    cols = st.columns(3)
                    with cols[0]:
                        render_sensor_mini("Volt L1", curr.get('elec_volt_L1', 0), "V")
                    with cols[1]:
                        render_sensor_mini("Volt L2", curr.get('elec_volt_L2', 0), "V")
                    with cols[2]:
                        render_sensor_mini("Volt L3", curr.get('elec_volt_L3', 0), "V")
                    
                    cols = st.columns(3)
                    with cols[0]:
                        render_sensor_mini("Amp L1", curr.get('elec_amp_L1', 0), "A")
                    with cols[1]:
                        render_sensor_mini("Amp L2", curr.get('elec_amp_L2', 0), "A")
                    with cols[2]:
                        render_sensor_mini("Amp L3", curr.get('elec_amp_L3', 0), "A")
                    
                    cols = st.columns(4)
                    with cols[0]:
                        render_sensor_mini("THD I", curr.get('elec_thd_current', 0), "%")
                    with cols[1]:
                        render_sensor_mini("THD V", curr.get('elec_thd_voltage', 0), "%")
                    with cols[2]:
                        render_sensor_mini("UPS Bat", curr.get('elec_ups_battery_pct', 0), "%")
                    with cols[3]:
                        render_sensor_mini("UPS Load", curr.get('elec_ups_load_pct', 0), "%")
            
            # 2. Indoor Environment
            with st.expander("🌫️ Indoor Environmental Quality (IEQ)", expanded=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    render_metric_card("CO2", f"{curr.get('ieq_co2_ppm', 0)}", "ppm", color="orange")
                    render_metric_card("VOCs", f"{curr.get('ieq_voc_ppb', 0)}", "ppb", color="red")
                    render_metric_card("PM2.5", f"{curr.get('ieq_pm25_indoor', 0):.1f}", "µg/m³", color="orange")
                    render_metric_card("PM10", f"{curr.get('ieq_pm10_indoor', 0):.1f}", "µg/m³", color="orange")
                
                with col2:
                    cols = st.columns(3)
                    with cols[0]:
                        render_sensor_mini("Noise", curr.get('ieq_noise_db', 0), "dB")
                    with cols[1]:
                        render_sensor_mini("Light", curr.get('ieq_light_lux', 0), "lux")
                    with cols[2]:
                        render_sensor_mini("Humidity", curr.get('ieq_humidity_in', 0), "%")
                    
                    cols = st.columns(3)
                    with cols[0]:
                        render_sensor_mini("Pressure", curr.get('ieq_pressure_pa', 0), "Pa")
                    with cols[1]:
                        render_sensor_mini("Radon", curr.get('ieq_radon_bqm3', 0), "Bq/m³")
                    with cols[2]:
                        render_sensor_mini("CO", curr.get('ieq_co_ppm', 0), "ppm")
            
            # 3. HVAC & Mechanical
            with st.expander("⚙️ HVAC & Mechanical Systems", expanded=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    render_metric_card("Fan Speed", f"{curr.get('hvac_fan_speed_rpm', 0)}", "RPM", color="blue")
                    render_metric_card("Chiller COP", f"{curr.get('hvac_chiller_efficiency_cop', 0):.2f}", "", color="green")
                    render_metric_card("Water Flow", f"{curr.get('water_flow_rate_lpm', 0):.1f}", "L/min", color="blue")
                
                with col2:
                    cols = st.columns(4)
                    with cols[0]:
                        render_sensor_mini("Supply T", curr.get('hvac_supply_temp', 0), "°C")
                    with cols[1]:
                        render_sensor_mini("Return T", curr.get('hvac_return_temp', 0), "°C")
                    with cols[2]:
                        render_sensor_mini("Boiler", curr.get('boiler_temp_c', 0), "°C")
                    with cols[3]:
                        render_sensor_mini("Filter", curr.get('hvac_filter_health', 0), "%")
                    
                    cols = st.columns(4)
                    with cols[0]:
                        render_sensor_mini("Duct P", curr.get('hvac_duct_pressure_pa', 0), "Pa")
                    with cols[1]:
                        render_sensor_mini("Vibration", curr.get('hvac_chiller_vibration', 0), "mm/s")
                    with cols[2]:
                        render_sensor_mini("Water P", curr.get('water_pressure_bar', 0), "bar")
                    with cols[3]:
                        render_sensor_mini("Steam P", curr.get('steam_pressure_bar', 0), "bar")
            
            # 4. Energy & Renewables
            with st.expander("🔋 Energy Storage & Renewables", expanded=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    render_metric_card("PV Generation", f"{curr.get('pv_generation_kw', 0):.1f}", "kW", color="green")
                    render_metric_card("Battery SOC", f"{curr.get('battery_soc_pct', 0):.1f}", "%", color="blue")
                    render_metric_card("Grid Import", f"{curr.get('grid_import_kw', 0):.1f}", "kW", color="orange")
                
                with col2:
                    cols = st.columns(3)
                    with cols[0]:
                        render_sensor_mini("PV Eff", curr.get('pv_efficiency_pct', 0), "%")
                    with cols[1]:
                        render_sensor_mini("Bat Charge", curr.get('battery_charge_kw', 0), "kW")
                    with cols[2]:
                        render_sensor_mini("Grid Export", curr.get('grid_export_kw', 0), "kW")
                    
                    # Renewable percentage chart
                    fig_renew = go.Figure(data=[
                        go.Bar(
                            x=['Wind', 'Solar', 'Nuclear'],
                            y=[
                                curr.get('grid_uk_wind_pct', 0),
                                curr.get('grid_uk_solar_pct', 0),
                                curr.get('grid_uk_nuclear_pct', 0)
                            ],
                            marker_color=['#00e676', '#ffc107', '#2196f3']
                        )
                    ])
                    fig_renew.update_layout(
                        title="Grid Mix (%)",
                        height=200,
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(255,255,255,0.05)",
                        font_color="white",
                        margin=dict(t=40, b=20, l=20, r=20)
                    )
                    st.plotly_chart(fig_renew, use_container_width=True, config={'displayModeBar': False}, key=f"chart_grid_{bid}")
            
            # 5. IT & Security
            with st.expander("🛜 Network & Security", expanded=False):
                cols = st.columns(4)
                with cols[0]:
                    render_sensor_mini("WiFi Dev", curr.get('net_wifi_devices', 0), "")
                with cols[1]:
                    render_sensor_mini("Latency", curr.get('net_iot_latency_ms', 0), "ms")
                with cols[2]:
                    render_sensor_mini("Bandwidth", curr.get('net_bandwidth_mbps', 0), "Mbps")
                with cols[3]:
                    render_sensor_mini("Loss", curr.get('net_packet_loss_pct', 0), "%")
                
                cols = st.columns(4)
                with cols[0]:
                    render_sensor_mini("Server T", curr.get('net_server_temp_c', 0), "°C")
                with cols[1]:
                    render_sensor_mini("CPU", curr.get('net_server_cpu_pct', 0), "%")
                with cols[2]:
                    render_sensor_mini("RAM", curr.get('net_server_ram_pct', 0), "%")
                with cols[3]:
                    render_sensor_mini("Cameras", curr.get('sec_camera_active', 0), "")
            
            # 6. Weather Correlation
            with st.expander("🌦️ Weather & External Factors", expanded=False):
                cols = st.columns(4)
                with cols[0]:
                    render_sensor_mini("Outdoor T", curr.get('hvac_outdoor_temp', 0), "°C")
                with cols[1]:
                    render_sensor_mini("Humidity", curr.get('weather_outdoor_humidity', 0), "%")
                with cols[2]:
                    render_sensor_mini("Wind", curr.get('weather_wind_speed', 0), "m/s")
                with cols[3]:
                    render_sensor_mini("Cloud", curr.get('weather_cloud_cover', 0), "%")
                
                cols = st.columns(3)
                with cols[0]:
                    render_sensor_mini("Heat Index", curr.get('weather_heat_index', 0), "°C")
                with cols[1]:
                    render_sensor_mini("Wind Chill", curr.get('weather_wind_chill', 0), "°C")
                with cols[2]:
                    render_sensor_mini("Solar Rad", curr.get('solar_radiation_Wm2', 0), "W/m²")

# Alert Panel (Right Side)
with alert_col:
    st.markdown("### 🚨 Active Alerts")
    
    # Alert summary
    critical_count = len([a for a in st.session_state.active_alerts if a['level'] == 'critical'])
    warning_count = len([a for a in st.session_state.active_alerts if a['level'] == 'warning'])
    info_count = len([a for a in st.session_state.active_alerts if a['level'] == 'info'])
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Critical", critical_count, delta=None, delta_color="off")
    with col2:
        st.metric("Warning", warning_count, delta=None, delta_color="off")
    with col3:
        st.metric("Info", info_count, delta=None, delta_color="off")
    
    st.markdown("---")
    
    # Display alerts
    if not st.session_state.active_alerts:
        st.success("✅ All systems nominal")
    else:
        # Sort by level priority
        level_priority = {'critical': 0, 'warning': 1, 'info': 2, 'success': 3}
        sorted_alerts = sorted(
            st.session_state.active_alerts,
            key=lambda x: level_priority.get(x['level'], 999)
        )
        
        for alert in sorted_alerts[:20]:  # Show top 20 alerts
            level_class = f"alert-{alert['level']}"
            icon_map = {
                'critical': '🔴',
                'warning': '⚠️',
                'info': 'ℹ️',
                'success': '✅'
            }
            icon = icon_map.get(alert['level'], '•')
            
            st.markdown(f"""
            <div class="metric-container {level_class}">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <div style="font-size: 12px; color: #90a4ae; margin-bottom: 4px;">
                            {icon} {alert['building']}
                        </div>
                        <div style="font-size: 14px; font-weight: 600; color: white;">
                            {alert['message']}
                        </div>
                        <div style="font-size: 10px; color: #b0bec5; margin-top: 4px;">
                            {alert['timestamp'].strftime('%H:%M:%S')}
                        </div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("<div style='margin: 8px 0;'></div>", unsafe_allow_html=True)
