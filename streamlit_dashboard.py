"""
This script Enterprise Real-Time Customer Heartbeat Monitoring Dashboard
Author: Damas Niyonkuru
"""

import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timezone
import time
import numpy as np

# CONFIGARATION
# ============================================================

DB_HOST = "localhost"
DB_NAME = "heartbeat_db"
DB_USER = "heartbeat_user"
DB_PASSWORD = "heartbeat_pass"
DB_PORT = "5432"

st.set_page_config(
    page_title="Heartbeat Monitoring",
    layout="wide",
)

# SESSION TRACKING
# ============================================================

if "session_start" not in st.session_state:
    st.session_state.session_start = datetime.now(timezone.utc)

session_duration = datetime.now(timezone.utc) - st.session_state.session_start

# SIDEBAR CONTROLS
# ============================================================

st.sidebar.header("Filters & Controls")

time_window = st.sidebar.selectbox(
    "Time Window",
    ["15 minutes", "30 minutes", "1 hour", "6 hours", "24 hours"],
)

interval_map = {
    "15 minutes": "15 minutes",
    "30 minutes": "30 minutes",
    "1 hour": "1 hour",
    "6 hours": "6 hours",
    "24 hours": "24 hours",
}

selected_customer = st.sidebar.selectbox(
    "Select Customer",
    ["All"],
)

show_anomalies_only = st.sidebar.checkbox("Show Anomalies Only")

low_threshold = st.sidebar.slider("Low HR Threshold", 30, 80, 30)
high_threshold = st.sidebar.slider("High HR Threshold", 120, 220, 180)

auto_refresh = st.sidebar.checkbox("Auto Refresh", value=True)

# DATABASE CONNECTION
# ============================================================

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
    )

# LOAD DATA WITH TIME WINDOW FILTER
# ============================================================

@st.cache_data(ttl=5)
def load_data(interval):
    conn = get_connection()
    query = f"""
        SELECT customer_id,
               event_timestamp,
               heart_rate,
               is_anomaly,
               NOW() - event_timestamp AS ingestion_delay
        FROM customer_heartbeats
        WHERE event_timestamp > NOW() - INTERVAL '{interval}'
        ORDER BY event_timestamp ASC;
    """
    df = pd.read_sql(query, conn)
    conn.close()

    if not df.empty:
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
        df["ingestion_delay"] = pd.to_timedelta(df["ingestion_delay"])

    return df

df = load_data(interval_map[time_window])

# FILTERS
# ============================================================

if selected_customer != "All":
    df = df[df["customer_id"] == selected_customer]

if show_anomalies_only:
    df = df[df["heart_rate"] < low_threshold] | (df["heart_rate"] > high_threshold)

# Dynamic anomaly flag
if not df.empty:
    df["dynamic_anomaly"] = (
        (df["heart_rate"] < low_threshold) |
        (df["heart_rate"] > high_threshold)
    )
else:
    df["dynamic_anomaly"] = []

# METRICS
# ============================================================

st.title(" Real-Time Customer Heartbeat Monitoring (Enterprise View)")

if not df.empty:
    latest_timestamp = df["event_timestamp"].max()
    freshness_seconds = (datetime.now(timezone.utc) - latest_timestamp).total_seconds()

    if freshness_seconds < 10:
        st.success("Data is Fresh")
    else:
        st.warning(f"Data Delay: {int(freshness_seconds)} sec")

records = len(df)
anomalies = df["dynamic_anomaly"].sum() if not df.empty else 0
anomaly_rate = (anomalies / records * 100) if records > 0 else 0
p95 = np.percentile(df["heart_rate"], 95) if records > 0 else 0
consumer_lag = int(df["ingestion_delay"].mean().total_seconds()) if records > 0 else 0

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Records", records)
col2.metric("Anomalies", anomalies)
col3.metric("Anomaly Rate (%)", f"{anomaly_rate:.2f}")
col4.metric("P95 Heart Rate", f"{p95:.2f}")
col5.metric("Consumer Lag (sec)", consumer_lag)

st.caption(f"Session Duration: {str(session_duration).split('.')[0]}")

# DATA QUALITY CHECKS
# ============================================================

st.subheader("Data Quality Checks")

missing_values = df.isnull().sum().sum() if not df.empty else 0
negative_hr = len(df[df["heart_rate"] < 0]) if not df.empty else 0

col1, col2 = st.columns(2)
col1.metric("Missing Values", missing_values)
col2.metric("Negative HR Values", negative_hr)

# CUSTOMER SUMMARY
# ============================================================

if not df.empty:
    st.subheader("Customer Summary")
    summary = df.groupby("customer_id").agg(
        avg_heart_rate=("heart_rate", "mean"),
        anomalies=("dynamic_anomaly", "sum"),
        records=("customer_id", "count"),
    ).reset_index()

    st.dataframe(summary, use_container_width=True)

# EVENT VOLUME OVER TIME
# ============================================================

if not df.empty:
    st.subheader("Event Volume Over Time")

    volume = df.resample("1min", on="event_timestamp").size()
    st.line_chart(volume)

# HEART RATE TREND
# ============================================================

if not df.empty:
    st.subheader("Heart Rate Trend")
    st.line_chart(df.set_index("event_timestamp")["heart_rate"])

# ROLLING AVERAGE
# ============================================================

if not df.empty:
    st.subheader("Rolling Average (10 Events)")
    df["rolling_avg"] = df["heart_rate"].rolling(window=10).mean()
    st.line_chart(df.set_index("event_timestamp")[["heart_rate", "rolling_avg"]])

# ANOMALY VISUALIZATION
# ============================================================

if not df.empty:
    st.subheader("Anomaly Visualization")
    anomaly_df = df.copy()
    anomaly_df["anomaly_flag"] = anomaly_df["dynamic_anomaly"].astype(int)
    st.line_chart(anomaly_df.set_index("event_timestamp")[["heart_rate", "anomaly_flag"]])

# EXPORT CSV
# ============================================================

if not df.empty:
    st.subheader("Export Data")
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        csv,
        "heartbeat_data.csv",
        "text/csv",
    )

# AUTO REFRESH
# ============================================================

if auto_refresh:
    time.sleep(5)
    st.rerun()