# src/modules/m30/db.py
"""
MongoDB database layer for Module 30: Time-Series Patient Health Data System.
Adapted to the existing medicare_db schema.
Collections: time_series, patterns, trends, seasonality
"""

import os
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime, timedelta
from dotenv import load_dotenv
import statistics

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".env"))

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME", "medicare_db")

METRICS = ["HeartRate", "SystolicBP", "DiastolicBP", "Temperature"]


@st.cache_resource
def get_client():
    client = MongoClient(MONGO_URI)
    try:
        client.admin.command("ping")
    except ConnectionFailure:
        st.error("❌ Could not connect to MongoDB. Check your connection string.")
        return None
    return client


def get_db():
    client = get_client()
    if client is None:
        return None
    return client[DB_NAME]

def col_time_series():
    return get_db()["time_series"]

def col_patterns():
    return get_db()["patterns"]

def col_trends():
    return get_db()["trends"]

def col_seasonality():
    return get_db()["seasonality"]


def get_distinct_series_ids():
    return sorted(col_time_series().distinct("Series_ID"))


def get_distinct_patient_ids():
    return sorted(col_time_series().distinct("Patient_ID"))

def get_time_series_by_series_id(series_id: str, limit: int = 0):
    cursor = col_time_series().find(
        {"Series_ID": series_id}, {"_id": 0}
    ).sort("Timestamp", 1)
    if limit:
        cursor = cursor.limit(limit)
    return list(cursor)


def get_time_series_by_patient(patient_id: str, limit: int = 500):
    return list(
        col_time_series()
        .find({"Patient_ID": patient_id}, {"_id": 0})
        .sort("Timestamp", 1)
        .limit(limit)
    )


def get_all_time_series_sample(limit: int = 100):
    return list(col_time_series().find({}, {"_id": 0}).limit(limit))


def get_all_patterns():
    return list(col_patterns().find({}, {"_id": 0}))


def get_patterns_by_series(series_id: str):
    return list(col_patterns().find({"Series_ID": series_id}, {"_id": 0}))


def get_all_trends():
    return list(col_trends().find({}, {"_id": 0}))


def get_trends_by_series(series_id: str):
    return list(col_trends().find({"Series_ID": series_id}, {"_id": 0}))


def get_all_seasonality():
    return list(col_seasonality().find({}, {"_id": 0}))


def get_seasonality_by_series(series_id: str):
    return list(col_seasonality().find({"Series_ID": series_id}, {"_id": 0}))


def get_collection_stats():
    db = get_db()
    if db is None:
        return {"time_series": 0, "patterns": 0, "trends": 0, "seasonality": 0}
    return {
        "time_series": col_time_series().count_documents({}),
        "patterns": col_patterns().count_documents({}),
        "trends": col_trends().count_documents({}),
        "seasonality": col_seasonality().count_documents({}),
    }

def agg_moving_average(series_id: str, metric: str = "HeartRate", window: int = 5):
    docs = get_time_series_by_series_id(series_id)
    if not docs:
        return []
    values = [d.get(metric) for d in docs if d.get(metric) is not None]
    timestamps = [d.get("Timestamp") for d in docs if d.get(metric) is not None]
    ma = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        avg = statistics.mean(values[start : i + 1])
        ma.append({
            "Timestamp": timestamps[i],
            "Value": values[i],
            "Moving_Avg": round(avg, 2),
        })
    return ma


def agg_rate_of_change(series_id: str, metric: str = "HeartRate"):
    docs = get_time_series_by_series_id(series_id)
    if not docs:
        return []
    values = [(d.get("Timestamp"), d.get(metric)) for d in docs if d.get(metric) is not None]
    roc = []
    for i in range(1, len(values)):
        prev_val = values[i - 1][1]
        curr_val = values[i][1]
        change = round(curr_val - prev_val, 2)
        pct = round((change / prev_val) * 100, 2) if prev_val != 0 else 0
        roc.append({
            "Timestamp": values[i][0],
            "Value": curr_val,
            "Change": change,
            "Pct_Change": pct,
        })
    return roc


def agg_anomaly_detection(series_id: str, metric: str = "HeartRate", sigma: float = 2.0):
    docs = get_time_series_by_series_id(series_id)
    if not docs:
        return [], 0, 0
    values = [(d.get("Timestamp"), d.get(metric)) for d in docs if d.get(metric) is not None]
    if len(values) < 2:
        return [], 0, 0
    vals = [v[1] for v in values]
    mean = statistics.mean(vals)
    std = statistics.stdev(vals)
    lower = mean - sigma * std
    upper = mean + sigma * std
    anomalies = []
    for ts, val in values:
        anomalies.append({
            "Timestamp": ts,
            "Value": val,
            "Is_Anomaly": val < lower or val > upper,
            "Lower_Bound": round(lower, 2),
            "Upper_Bound": round(upper, 2),
        })
    return anomalies, round(mean, 2), round(std, 2)


def agg_pattern_frequency():
    pipeline = [
        {"$group": {"_id": "$Pattern_Type", "count": {"$sum": 1},
                     "avg_significance": {"$avg": "$Significance_Score"}}},
        {"$sort": {"count": -1}},
    ]
    return list(col_patterns().aggregate(pipeline))


def agg_trend_summary():
    pipeline = [
        {"$group": {"_id": "$Direction", "count": {"$sum": 1},
                     "avg_slope": {"$avg": "$Slope_Value"}}},
        {"$sort": {"count": -1}},
    ]
    return list(col_trends().aggregate(pipeline))