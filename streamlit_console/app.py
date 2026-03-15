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

import streamlit as st
from pathlib import Path

st.set_page_config(layout="wide")

st.title("李兰迪 Gallery")

st.image(
    "images/lilandi/16th.jpg",
    use_container_width=True
)

st.divider()

image_dir = Path("images/lilandi/gallery")
images = sorted(list(image_dir.glob("*.jpg")) + list(image_dir.glob("*.png")))

st.markdown("""
<style>
img {
    border-radius: 16px;
}
[data-testid="stImage"] {
    margin-bottom: 1rem;
}
.block-container {
    padding-top: 2rem;
    padding-bottom: 2rem;
}
</style>
""", unsafe_allow_html=True)


cols = st.columns(4)

for i, img in enumerate(images):
    with cols[i % 4]:
        st.image(str(img), use_container_width=True)

