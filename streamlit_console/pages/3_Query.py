from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_console.core.db import test_connection, select_df, execute_sql
from streamlit_console.core.config import DB_TARGET
from streamlit_console.core.query_registry import CATEGORIES, QUERIES, QueryDef, QueryParam

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
            p.label,
            value="" if p.default is None else str(p.default),
            placeholder=p.placeholder,
            help=p.help,
            key=key,
        )

    if p.type == "int":
        return st.number_input(
            p.label,
            value=0 if p.default is None else int(p.default),
            step=1,
            help=p.help,
            key=key,
        )

    if p.type == "float":
        return st.number_input(
            p.label,
            value=0.0 if p.default is None else float(p.default),
            help=p.help,
            key=key,
        )

    if p.type == "date":
        return st.date_input(p.label, value=p.default, help=p.help, key=key)

    if p.type == "datetime":
        return st.text_input(
            p.label,
            value="" if p.default is None else str(p.default),
            placeholder="YYYY-MM-DD HH:MM:SS",
            help=p.help or "用字符串传入（建议带时区时在 SQL 里 ::timestamptz）",
            key=key,
        )

    if p.type == "select":
        opts = p.options or []
        if not opts:
            st.warning(f"Param {p.key} is select but options is empty.")
        return st.selectbox(p.label, options=opts, index=0, help=p.help, key=key)

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


def extract_track_ids(q: QueryDef, params: dict) -> list[str]:
    keys = getattr(q, "track_id_keys", None) or []
    out = []
    for k in keys:
        v = params.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            out.append(s)
    # remove duplicates while preserving order
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def upsert_watchlist(person_id: str):
    upsert_sql = """
    INSERT INTO person_watchlist (person_id)
    VALUES (%(person_id)s)
    ON CONFLICT (person_id)
    DO UPDATE SET
        last_seen = NOW(),
        query_count = person_watchlist.query_count + 1;
    """
    execute_sql(upsert_sql, {"person_id": person_id})


def add_limit_wrapper(sql_body: str) -> str:
    base = sql_body.strip().rstrip(";")
    return f"SELECT * FROM ({base}) t LIMIT %(limit)s"


def run_one_query(q: QueryDef, params: dict, limit: int) -> pd.DataFrame:
    sql = add_limit_wrapper(q.sql)
    params2 = dict(params)
    params2["limit"] = int(limit)
    return select_df(sql, params2)


def queries_by_track_key(track_key: str) -> list[QueryDef]:
    out: list[QueryDef] = []
    for q in QUERIES:
        keys = getattr(q, "track_id_keys", None) or []
        if track_key in keys:
            out.append(q)
    return out


def build_params_for_bundle(q: QueryDef, track_key: str, track_value: str) -> dict:
    """
    Bundle Mode: Only one input (e.g., user_id) is used, automatically filling in the required parameters for the QueryDef.
    Rules:
    1) If a parameter with the same name as track_key exists in QueryDef.params -> fill it in directly.
    2) Compatibility with historical naming conventions: user_id -> member_id/uid/douban_user_id
    3) If there are other required parameters and no default is specified -> a missing parameter will be displayed here, resulting in an error (shown in expander).
    """
    params: dict = {}
    for p in q.params:
        # 1) Fill in the same name directly.
        if p.key == track_key:
            params[p.key] = track_value
            continue

        # 2) Aliases compatible with user_id (you can add them as needed)
        if track_key == "user_id" and p.key in ("member_id", "uid", "douban_user_id"):
            params[p.key] = track_value
            continue

        # 3) default
        if getattr(p, "default", None) is not None:
            params[p.key] = p.default

    # Validation required
    for p in q.params:
        if p.required:
            v = params.get(p.key)
            if v is None or (isinstance(v, str) and not str(v).strip()):
                raise ValueError(f"该查询还需要参数：{p.label}（key={p.key}）")
    return params


def bundle_run(track_key: str, track_value: str, limit_each: int):
    related = queries_by_track_key(track_key)

    st.subheader(f"🔁 Bundle results for {track_key} = {track_value}")
    st.caption(f"Matched queries: {len(related)}")

    # insert into watchlist
    upsert_watchlist(track_value.strip())

    for q in related:
        with st.expander(f"{q.name}", expanded=False):
            st.caption(q.desc)

            try:
                params = build_params_for_bundle(q, track_key, track_value)
                df = run_one_query(q, params, limit_each)

                st.success(f"Returned {len(df)} rows")
                st.dataframe(df, use_container_width=True, height=420)

                if not df.empty:
                    csv = df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        "⬇️ Download CSV",
                        data=csv,
                        file_name=f"bundle__{track_key}__{track_value}__{q.name}.csv".replace("/", "_"),
                        mime="text/csv",
                        key=f"dl__{track_key}__{track_value}__{q.name}",
                    )

            except Exception as e:
                st.error(f"Error: {e}")
                st.code(q.sql.strip(), language="sql")


# -------------------------
# Page
# -------------------------
col_left, col_right = st.columns([2, 1], gap="large")
result_container = st.container()

with col_left:
    st.subheader("1) 选择查询 / Bundle")

    mode = st.radio(
        "Mode",
        options=["Single Query", "Bundle"],
        horizontal=True,
    )

    if mode == "Single Query":
        category = st.radio("Category", CATEGORIES, horizontal=True)
        q_list = queries_by_category(category)

        if not q_list:
            st.info("该分类还没有添加 query。去 core/query_registry.py 里加 QueryDef。")
            st.stop()

        selected_name = st.selectbox("Query", options=[q.name for q in q_list])
        q = next(x for x in q_list if x.name == selected_name)

        st.caption(q.desc)
        with st.expander("查看 SQL（白名单模板）", expanded=False):
            st.code(q.sql.strip(), language="sql")

    else:
        BUNDLE_KEYS = ["user_id", "group_who"]

        track_key = st.selectbox("Track key", options=BUNDLE_KEYS, key="bundle__track_key")
        track_value = st.text_input(track_key, placeholder=f"输入 {track_key}", key="bundle__track_value")

with col_right:
    st.subheader("2) 输入参数")

    if mode == "Single Query":
        for p in q.params:
            _render_param_input(p)

        limit = st.number_input(
            "Limit",
            min_value=1,
            max_value=50000,
            value=q.default_limit or 500,
            step=50,
            key="single__limit",
        )
        run = st.button("▶️ Run", type="primary", use_container_width=False, key="single__run")

    else:
        limit_each = st.number_input(
            "Limit (each query)",
            min_value=1,
            max_value=50000,
            value=500,
            step=50,
            key="bundle__limit_each",
        )
        run_bundle = st.button(
            "▶️ Run bundle",
            type="primary",
            key="bundle__run",
            disabled=not st.session_state.get("bundle__track_value", "").strip(),
        )

with result_container:
    st.divider()
    st.subheader("结果")

    if mode == "Single Query":
        if run:
            try:
                params = build_params(q)
                df = run_one_query(q, params, limit)

                st.success(f"Returned {len(df)} rows")
                st.dataframe(df, use_container_width=True, height=520)

                # Record to watchlist (track_ids can contain user_id/member_id etc.)
                track_ids = extract_track_ids(q, params)
                for pid in track_ids:
                    upsert_watchlist(pid.strip())

                if not df.empty:
                    csv = df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        "⬇️ Download CSV",
                        data=csv,
                        file_name="query_result.csv",
                        mime="text/csv",
                        key="single__download",
                    )

            except Exception as e:
                st.error(f"Error running query: {e}")

    else:
        if run_bundle:
            try:
                bundle_run(
                    track_key=st.session_state["bundle__track_key"],
                    track_value=st.session_state["bundle__track_value"].strip(),
                    limit_each=int(st.session_state.get("bundle__limit_each", 500)),
                )
            except Exception as e:
                st.error(f"Error running bundle: {e}")
