import streamlit as st
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

st.set_page_config(
    page_title="Douban Console",
    page_icon="🧰",
    layout="wide",
)

st.title("🧰 Douban Data Console")
st.caption("Run tasks • Browse views • Filter queries • Logs")

st.info("Select page from left：Run / Browse / Query / Logs")
