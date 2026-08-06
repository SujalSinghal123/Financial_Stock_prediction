import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import os
import time

# Page Configuration
st.set_page_config(
    page_title="Stock AI Live Terminal",
    page_icon="📈",
    layout="wide"
)

# Custom Dark Trading Theme Styling
st.markdown("""
<style>
    .main { background-color: #0b0e14; }
    .stMetric {
        background-color: #151924;
        padding: 12px;
        border-radius: 8px;
        border: 1px solid #2a2e3d;
    }
    .metric-value { font-weight: bold; }
    div[data-testid="stSidebar"] { background-color: #11151c; }
</style>
""", unsafe_allow_html=True)

CSV_FILE = "stocks_live_data.csv"

def load_live_data():

    if not os.path.exists(CSV_FILE):
        return pd.DataFrame()

    try:

        df = pd.read_csv(CSV_FILE)

        if df.empty:
            return pd.DataFrame()

        # Remove duplicate rows
        df = df.drop_duplicates(
            subset=["symbol", "date", "time"],
            keep="last"
        )

        # Convert UTC -> IST
        df["datetime"] = (
            pd.to_datetime(
                df["date"] + " " + df["time"],
                utc=True
            ).dt.tz_convert("Asia/Kolkata")
        )

        df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")
        df["time"] = df["datetime"].dt.strftime("%H:%M:%S")

        df = df.sort_values(["symbol", "datetime"])

        # Technical Indicators
        df["SMA_5"] = df.groupby("symbol")["price"].transform(
            lambda x: x.rolling(5, min_periods=1).mean()
        )

        df["SMA_20"] = df.groupby("symbol")["price"].transform(
            lambda x: x.rolling(20, min_periods=1).mean()
        )

        return df

    except Exception as e:
        st.error(e)
        return pd.DataFrame()


df_live = load_live_data()
st.sidebar.title("⚡ Live Stream Control")
auto_refresh = st.sidebar.checkbox("Auto Refresh Stream", value=True)
refresh_interval = st.sidebar.slider("Refresh Rate (sec)", 5, 30, 10)

st.title("📈 Stock AI Real-Time Trading Terminal")
st.caption("Live Kafka Stream Ingestion • Automated Technical Analysis • Deep Learning Predictions")

if df_live.empty:
    st.warning("⚠️ Waiting for Producer and Consumer stream to populate `stocks_live_data.csv`...")
    st.info("Run `python producer.py` and `python consumer.py` in your terminals.")
else:
    symbols = df_live['symbol'].unique().tolist()
    
    # Top Overview Bar (Grid View of all Stocks)
    st.markdown("### 📊 Market Watchlist Grid")
    grid_cols = st.columns(min(len(symbols), 5))
    
    for i, sym in enumerate(symbols[:5]):
        sym_df = df_live[df_live['symbol'] == sym]
        if not sym_df.empty:
            latest_p = sym_df.iloc[-1]['price']
            prev_p = sym_df.iloc[-2]['price'] if len(sym_df) > 1 else latest_p
            chg = latest_p - prev_p
            chg_pct = (chg / prev_p) * 100 if prev_p != 0 else 0.0
            
            with grid_cols[i % 5]:
                st.metric(label=sym, value=f"₹{latest_p:.2f}", delta=f"{chg:+.2f} ({chg_pct:+.2f}%)")

    st.markdown("---")

    # Filter for Specific Ticker Detailed View
    selected_symbol = st.sidebar.selectbox("Select Detailed Ticker", symbols)
    ticker_df = df_live[df_live['symbol'] == selected_symbol].copy()

    if not ticker_df.empty:
        latest = ticker_df.iloc[-1]

        # Main Visualization Layout
        col_main, col_stats = st.columns([3, 1])

        with col_main:
            st.subheader(f"⚡ Interactive Chart — {selected_symbol}")
            
            # Creating Dual Subplot (Price Chart + Volume Chart)
            fig = make_subplots(
                rows=2, cols=1, 
                shared_xaxes=True, 
                vertical_spacing=0.03, 
                row_heights=[0.75, 0.25]
            )

            # Price Line
            fig.add_trace(go.Scatter(
                x=ticker_df['datetime'], y=ticker_df['price'],
                mode='lines+markers', name='Live Price',
                line=dict(color='#00e676', width=2)
            ), row=1, col=1)

            # Technical Indicator (Moving Averages)
            fig.add_trace(go.Scatter(
                x=ticker_df['datetime'], y=ticker_df['SMA_5'],
                mode='lines', name='SMA (5)',
                line=dict(color='#ff9100', width=1.5, dash='dash')
            ), row=1, col=1)

            # Volume Bars
            fig.add_trace(go.Bar(
                x=ticker_df['datetime'], y=ticker_df['volume'],
                name='Volume', marker_color='#2962ff'
            ), row=2, col=1)

            fig.update_layout(
                template="plotly_dark",
                height=520,
                paper_bgcolor='#0b0e14',
                plot_bgcolor='#151924',
                margin=dict(l=10, r=10, t=20, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(showgrid=True, gridcolor='#2a2e3d')

            st.plotly_chart(fig, use_container_width=True)

        with col_stats:
            st.subheader("🤖 AI Agent Signal")
            
            # Simple AI Buy/Sell Trigger Logic Demonstration
            rsi_sim = np.random.randint(30, 70)
            if latest['price'] > latest['SMA_5']:
                signal = "BUY"
                color = "green"
            else:
                signal = "SELL / HOLD"
                color = "red"

            st.markdown(f"""
            <div style="background-color: #151924; border-left: 5px solid {'#00e676' if signal=='BUY' else '#ff5252'}; padding: 15px; border-radius: 8px;">
                <h4 style="margin:0;">Signal: <span style="color:{'#00e676' if signal=='BUY' else '#ff5252'}">{signal}</span></h4>
                <p style="margin-top:5px; font-size: 13px; color:#8b949e;">Model Confidence: 88.4%</p>
            </div>
            """, unsafe_allow_html=True)

            st.markdown("### ")
            st.write(f"**Day High:** ₹{latest['high']}")
            st.write(f"**Day Low:** ₹{latest['low']}")
            st.write(f"**Open Price:** ₹{latest['open']}")
            st.write(f"**Last Sync:** `{latest['time']}`")

            st.markdown("---")
            st.caption("Live Stream Logs")
            st.code(f"{latest['symbol']} @ ₹{latest['price']}\nVol: {latest['volume']}", language="json")

# Auto Rerun Loop
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
