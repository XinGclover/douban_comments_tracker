from __future__ import annotations

import os
import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
from streamlit_console.core.db import test_connection, select_df
from streamlit_console.core.config import DB_TARGET

from core.query_registry import CATEGORIES, QUERIES, QueryDef, QueryParam


st.set_page_config(page_title="Query", layout="wide")
st.title("🔎 Query Console")
st.caption(f"DB: {DB_TARGET}")

# -------------------------
# UI helpers
# -------------------------
def _render_param_input(p: QueryParam):
    key = f"param__{p.key}"

    if p.type == "text":
        return st.text_input(
            p.label, value="" if p.default is None else str(p.default),
            placeholder=p.placeholder, help=p.help, key=key
        )

    if p.type == "int":
        return st.number_input(
            p.label, value=0 if p.default is None else int(p.default),
            step=1, help=p.help, key=key
        )

    if p.type == "float":
        return st.number_input(
            p.label, value=0.0 if p.default is None else float(p.default),
            help=p.help, key=key
        )

    if p.type == "date":
        return st.date_input(p.label, value=p.default, help=p.help, key=key)

    if p.type == "datetime":
        return st.text_input(
            p.label,
            value="" if p.default is None else str(p.default),
            placeholder="YYYY-MM-DD HH:MM:SS",
            help=p.help or "用字符串传入（建议带时区时在 SQL 里 ::timestamptz）",
            key=key
        )

    if p.type == "select":
        opts = p.options or []
        if not opts:
            st.warning(f"Param {p.key} is select but options is empty.")
        return st.selectbox(p.label, options=opts, index=0, help=p.help, key=key)

    # fallback
    return st.text_input(p.label, key=key)


def build_params(q: QueryDef) -> dict:
    out = {}
    for p in q.params:
        v = st.session_state.get(f"param__{p.key}")
        if p.required and (v is None or (isinstance(v, str) and not v.strip())):
            raise ValueError(f"参数缺失：{p.label}")
        out[p.key] = v
    return out


def queries_by_category(category: str) -> list[QueryDef]:
    return [q for q in QUERIES if q.category == category]


# -------------------------
# Page
# -------------------------

col_left, col_right  = st.columns([2, 1], gap="large")

result_container = st.container()

with col_left:
    st.subheader("1) 选择查询")

    category = st.radio("Category", CATEGORIES, horizontal=True)
    q_list = queries_by_category(category)

    if not q_list:
        st.info("该分类还没有添加 query。去 core/query_registry.py 里加 QueryDef。")
        st.stop()

    selected_name = st.selectbox(
        "Query",
        options=[q.name for q in q_list],
    )
    q = next(x for x in q_list if x.name == selected_name)

    st.caption(q.desc)

    with st.expander("查看 SQL（白名单模板）", expanded=False):
        st.code(q.sql.strip(), language="sql")

with col_right:
    st.subheader("2) 输入参数")
    for p in q.params:
        _render_param_input(p)

    # Optional：limit
    limit = st.number_input("Limit", min_value=1, max_value=50000, value=q.default_limit or 500, step=50)

    run = st.button("▶️ Run", type="primary", use_container_width=False)


with result_container:
    st.divider()
    st.subheader("结果")
    if run:
        try:
            params = build_params(q)
            # Add LIMIT to all queries (you don't need to add it if the SQL query already has a LIMIT clause).
            sql = f"SELECT * FROM ({q.sql.strip().rstrip(';')}) t LIMIT %(limit)s"
            params["limit"] = int(limit)

            df = select_df(sql, params)

            st.success(f"Returned {len(df)} rows")
            st.dataframe(df, use_container_width=True, height=520)

            if not df.empty:
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button("⬇️ Download CSV", data=csv, file_name="query_result.csv", mime="text/csv")

        except Exception as e:
            st.error(f"Error running query: {e}")
