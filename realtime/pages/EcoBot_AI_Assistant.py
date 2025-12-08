# File: realtime/pages/2_🤖_EcoBot_AI_Assistant.py

import streamlit as st
import requests
from datetime import datetime

# --- 1. PAGE CONFIG (Each page needs its own) ---
st.set_page_config(
    page_title="EcoBot AI Assistant",
    layout="wide",
    page_icon="🤖"
)

# --- 2. THEME / CSS (Copied for consistent styling) ---
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

# --- NO AUTO-REFRESH ON THIS PAGE ---

st.subheader("🤖 EcoBot AI Assistant")

# --- 3. Initialize Chat History ---
if "messages" not in st.session_state:
    st.session_state.messages = []

# This list is needed for the selectbox
buildings = ["Building-A-HQ", "Building-B-Lab", "Building-C-Warehouse"]

# --- 4. Define the Layout ---
chat_col1, chat_col2 = st.columns([1, 3])

# --- 5. Column 1: Settings ---
with chat_col1:
    st.markdown("### Context Settings")
    target_building = st.selectbox("Select Building", buildings, key="ai_building")
    data_mode = st.radio("Data Source", ["Live Data", "Historical Analysis"], key="ai_mode")
    
    hours_lookback = 0
    if data_mode == "Historical Analysis":
        hours_lookback = st.slider("Lookback (Hours)", 1, 24, 4)
    
    def clear_chat_history():
        st.session_state.messages = []
        
    if st.button("Clear Chat", on_click=clear_chat_history):
        pass

# --- 6. Column 2: Chat Interface ---
with chat_col2:
    # A. Display chat history from session state on every interaction
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # B. Handle new user input
    if prompt := st.chat_input("Ask about energy, anomalies, or efficiency..."):
        
        # Add user message to state and display it immediately
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Process and display the assistant's response
        with st.chat_message("assistant"):
            # Create a placeholder for the "Thinking..." message
            message_placeholder = st.empty()
            message_placeholder.markdown("Thinking...")
            
            ai_answer = "⚠️ AI Service Unavailable" # Default error message
            
            try:
                # Prepare the payload for the AI service
                payload = {
                    "query": prompt,
                    "context_type": "live" if data_mode == "Live Data" else "history",
                    "building_id": target_building,
                    "time_range_hours": hours_lookback
                }
                
                # Call the AI microservice
                response = requests.post("http://ai-service:8000/ask", json=payload, timeout=20)

                if response.status_code == 200:
                    ai_answer = response.json().get("answer", "Error: 'answer' key missing in response.")
                else:
                    ai_answer = f"⚠️ AI Service Error: HTTP {response.status_code} - Detail: {response.text[:50]}"
            
            except requests.exceptions.Timeout:
                ai_answer = "Error connecting to AI: Request timed out."
            except Exception as e:
                ai_answer = f"Error connecting to AI: {e}"

            # Update the placeholder with the final answer
            message_placeholder.markdown(ai_answer)
            
            # Add the final assistant answer to the session state history
            st.session_state.messages.append({"role": "assistant", "content": ai_answer})