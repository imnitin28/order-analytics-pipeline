"""
Streamlit dashboard showing live revenue-by-category and recent fraud alerts,
reading from Postgres (populated by consumer.py). Auto-refreshes.
"""
import os
import time

import pandas as pd
import psycopg2
import streamlit as st

PG_HOST = os.environ.get("POSTGRES_HOST", "localhost")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "orders_db")
PG_USER = os.environ.get("POSTGRES_USER", "orders_app")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "change-me-locally")

REFRESH_SECONDS = int(os.environ.get("DASHBOARD_REFRESH_SECONDS", "5"))

st.set_page_config(page_title="Order Analytics", layout="wide")


@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def load_revenue(conn) -> pd.DataFrame:
    return pd.read_sql(
        "SELECT category, total_revenue, order_count, updated_at "
        "FROM revenue_by_category ORDER BY total_revenue DESC;",
        conn,
    )


def load_fraud_alerts(conn) -> pd.DataFrame:
    return pd.read_sql(
        "SELECT order_id, user_id, category, amount, reason, order_timestamp, flagged_at "
        "FROM fraud_alerts ORDER BY flagged_at DESC LIMIT 25;",
        conn,
    )


def render():
    conn = get_connection()

    st.title("📦 Real-Time Order Analytics")
    st.caption(f"Auto-refreshing every {REFRESH_SECONDS}s")

    col1, col2 = st.columns([2, 1])

    revenue_df = load_revenue(conn)
    fraud_df = load_fraud_alerts(conn)

    with col1:
        st.subheader("Revenue by Category")
        if revenue_df.empty:
            st.info("No orders processed yet.")
        else:
            st.bar_chart(revenue_df.set_index("category")["total_revenue"])
            st.dataframe(revenue_df, use_container_width=True)

    with col2:
        st.subheader("Totals")
        st.metric("Total Orders", int(revenue_df["order_count"].sum()) if not revenue_df.empty else 0)
        st.metric("Total Revenue", f"${revenue_df['total_revenue'].sum():,.2f}" if not revenue_df.empty else "$0.00")
        st.metric("Fraud Alerts", len(fraud_df))

    st.subheader("🚨 Recent Fraud Alerts")
    if fraud_df.empty:
        st.info("No fraud alerts yet.")
    else:
        st.dataframe(fraud_df, use_container_width=True)


render()
time.sleep(REFRESH_SECONDS)
st.rerun()
