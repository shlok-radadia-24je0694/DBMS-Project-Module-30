# src/modules/m30/tabs/collections.py
"""Tab 3 — Collections: Browse all 4 MongoDB collections."""

import streamlit as st
import pandas as pd
from src.modules.m30.db import (
    METRICS,
    get_distinct_series_ids,
    get_time_series_by_series_id,
    get_all_patterns,
    get_all_trends,
    get_all_seasonality,
)


def render_collections_tab():
    st.markdown("### 📋 Browse MongoDB Collections")

    collection_choice = st.selectbox(
        "Select collection",
        ["time_series", "patterns", "trends", "seasonality"],
        key="m30_col_choice",
    )

    if collection_choice == "time_series":
        series_ids = get_distinct_series_ids()
        if not series_ids:
            st.warning("No time-series data found.")
            return

        selected_sid = st.selectbox("Filter by Series_ID:", series_ids, key="m30_ts_sid")
        data = get_time_series_by_series_id(selected_sid, limit=200)
        if data:
            df = pd.DataFrame(data)
            st.dataframe(df, use_container_width=True)
            st.caption(f"Showing {len(data)} records for **{selected_sid}**")

            # Chart
            if "Timestamp" in df.columns:
                st.markdown("#### 📈 Vitals Over Time")
                chart_cols = [c for c in METRICS if c in df.columns]
                if chart_cols:
                    chart_df = df[["Timestamp"] + chart_cols].copy()
                    chart_df = chart_df.set_index("Timestamp")
                    st.line_chart(chart_df)
        else:
            st.info("No data for this Series_ID.")

    elif collection_choice == "patterns":
        data = get_all_patterns()
        if data:
            st.dataframe(pd.DataFrame(data), use_container_width=True)
            st.caption(f"Showing {len(data)} pattern records")
        else:
            st.warning("No pattern data found.")

    elif collection_choice == "trends":
        data = get_all_trends()
        if data:
            st.dataframe(pd.DataFrame(data), use_container_width=True)
            st.caption(f"Showing {len(data)} trend records")
        else:
            st.warning("No trend data found.")

    elif collection_choice == "seasonality":
        data = get_all_seasonality()
        if data:
            st.dataframe(pd.DataFrame(data), use_container_width=True)
            st.caption(f"Showing {len(data)} seasonality records")
        else:
            st.warning("No seasonality data found.")
