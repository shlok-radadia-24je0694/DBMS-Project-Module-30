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