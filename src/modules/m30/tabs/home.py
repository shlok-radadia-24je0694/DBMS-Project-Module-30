# src/modules/m30/tabs/home.py
"""Tab 1 — Home: Database overview, entity descriptions, relationships."""

import streamlit as st
from src.modules.m30.db import get_collection_stats


def render_home_tab():
    stats = get_collection_stats()

    st.markdown("### 📈 Database Overview")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("⏱️ Time Series", f"{stats['time_series']:,}")
    c2.metric("🔍 Patterns", stats["patterns"])
    c3.metric("📈 Trends", stats["trends"])
    c4.metric("🔄 Seasonality", stats["seasonality"])

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 📊 Entities")
        st.info(
            "**TimeSeries** — Patient vital sign recordings: HeartRate, SystolicBP, "
            "DiastolicBP, Temperature. Each record is a single timestamp reading."
        )
        st.info(
            "**Pattern** — Detected patterns (Spikes, Dips, etc.) "
            "with timestamps, metrics, and significance scores."
        )
    with col2:
        st.markdown("### 📊 Entities (cont.)")
        st.info(
            "**Trend** — Directional trends (Increasing/Decreasing/Stable) per metric "
            "with slope values showing rate of change."
        )
        st.info(
            "**Seasonality** — Cyclical patterns (Daily, Weekly, etc.) "
            "observed per metric with max amplitude."
        )

    st.divider()

    st.markdown("### 🔗 Relationships")
    st.markdown("""
| Relationship | From | To | Cardinality |
|---|---|---|---|
| **Exhibits** | TimeSeries | Trend | 1 : N |
| **Identifies** | TimeSeries | Pattern | 1 : N |
| **Follows** | TimeSeries | Seasonality | 1 : 1 |
    """)

    st.divider()
    st.markdown("### ⚙️ Technical Specifications")
    t1, t2 = st.columns(2)
    with t1:
        st.markdown("""
        - **Metrics:** HeartRate, SystolicBP, DiastolicBP, Temperature
        - **Analysis:** Moving averages, Rate of change, Cyclical patterns
        """)
    with t2:
        st.markdown("""
        - **Advanced:** Anomaly detection, Predictive modeling
        - **Database:** MongoDB Atlas (document-oriented)
        """)
