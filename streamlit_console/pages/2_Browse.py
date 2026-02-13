import streamlit as st

from streamlit_console.core.config import DB_TARGET
from streamlit_console.core.db import test_connection, select_df
from streamlit_console.core.views import CATEGORIES, VIEWS

st.set_page_config(page_title="Browse Views", layout="wide")
st.title("🔎 Browse Views")
st.caption(f"DB: {DB_TARGET}")

# ---------- UI: category + view ----------
cat = st.radio("Category", CATEGORIES, horizontal=True)

views_in_cat = [v for v in VIEWS if v.get("category") == cat]
if not views_in_cat:
    st.warning("No views under this category yet.")
    st.stop()

view_item = st.selectbox(
    "View",
    views_in_cat,
    format_func=lambda x: x.get("name", x.get("view", "unknown")),
)

desc = view_item.get("desc", "")
if desc:
    st.caption(desc)

# ---------- UI: order ----------
sortable = view_item.get("sortable") or []
default_order = view_item.get("default_order")

# If sortable is not configured, it degenerates into "non-selectable sorting"
# But default_order is still allowed (if you specify it).
order_col = None
order_dir = "DESC"

if sortable:
    # If `default_order` is not in the list, use the first option as a fallback.
    if default_order in sortable:
        default_idx = sortable.index(default_order)
    else:
        default_idx = 0

    order_col = st.selectbox("Order by", sortable, index=default_idx)
    order_dir = st.radio("Direction", ["DESC", "ASC"], horizontal=True)
else:
    if default_order:
        st.info(f"Sorting uses default_order: `{default_order}` (not user-selectable)")
    else:
        st.info("This view has no sortable fields configured; no ORDER BY will be applied.")

# ---------- DB test ----------
col1, col2 = st.columns([1, 3])
with col1:
    if st.button("🔌 Test DB"):
        ok, msg = test_connection()
        if ok:
            st.toast("DB OK", icon="✅")
        else:
            st.error(msg)

# ---------- Build SQL (safe) ----------
LIMIT_SAFE = 500

sql = f"SELECT * FROM {view_item['view']}"

# Sorting: Only allowed if it's on the whitelist
if order_col:
    # Optional: NULLS sorting is more stable (especially for time columns).
    nulls = "NULLS LAST" if order_dir == "DESC" else "NULLS FIRST"
    sql += f" ORDER BY {order_col} {order_dir} {nulls}"
elif default_order:
    # default_order is configured in `views.py` and is part of the whitelist strategy.
    # More complex default_order values ​​are allowed, such as "reply_count DESC, user_id, pubtime".
    sql += f" ORDER BY {default_order}"

sql += f" LIMIT {LIMIT_SAFE};"

with col2:
    st.code(sql, language="sql")

# ---------- Load ----------
if st.button("📥 Load", type="primary"):
    df = select_df(sql, ())
    st.write(f"Rows: {len(df)} (limit={LIMIT_SAFE})")
    st.dataframe(df, use_container_width=True)

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download CSV",
        data=csv,
        file_name=f"{view_item['view']}.csv",
        mime="text/csv",
    )
