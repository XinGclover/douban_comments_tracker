import streamlit as st
import plotly.express as px
import pandas as pd
from streamlit_console.core.config import DB_TARGET
from streamlit_console.core.db import select_df
from streamlit_console.core.charts import CATEGORIES, CHARTS
from streamlit_console.core.tools import apply_date_filter
from streamlit_console.core.chart_engine import render_chart


st.set_page_config(page_title="Browse Views", layout="wide")
st.title("📊 Charts")

cat = st.radio("Category", CATEGORIES, horizontal=True)

charts_in_cat = [c for c in CHARTS if c.get("category") == cat]

if not charts_in_cat:
    st.warning("No charts under this category yet.")
    st.stop()

chart_item = st.selectbox(
    "Chart",
    charts_in_cat,
    format_func=lambda x: x.get("name", "unknown"),
)
if chart_item.get("desc"):
    st.caption(chart_item["desc"])

view_name = chart_item["view"]
date_col = chart_item.get("date_col")
x_col = chart_item.get("x")

sql = f"""
    SELECT *
    FROM {view_name}
"""

df = select_df(sql)

if df.empty:
    st.info("No data found.")
    st.stop()

df, period = apply_date_filter(df, date_col)

if period and len(period) == 2:
    st.caption(f"Period: {period[0]} → {period[1]}")

if df.empty:
    st.info("No data found for selected period.")
    st.stop()

render_chart(chart_item, df)

st.dataframe(df, use_container_width=True)