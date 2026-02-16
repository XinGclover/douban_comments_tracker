import streamlit as st
import sys
from pathlib import Path
import streamlit as st

st.markdown("""
<style>
* {
    font-family: "Apple Color Emoji",
                 "Segoe UI Emoji",
                 "Noto Color Emoji",
                 sans-serif !important;
}
</style>
""", unsafe_allow_html=True)


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
