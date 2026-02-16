import streamlit as st
from streamlit_console.core.runner import run_cmd
from streamlit_console.core.tasks import TASKS, CATEGORIES

st.title("▶️ Run Bash Commands")

# 1) choose category
category = st.radio(
    "Choose task category",
    options=CATEGORIES,
    horizontal=True,
)

# 2) filter tasks by category
filtered = [t for t in TASKS if t["category"] == category]
if not filtered:
    st.warning("No tasks available for this category.")
    st.stop()

# 3) droplist shows only tasks in the selected category
task_names = [t["name"] for t in filtered]
selected_name = st.selectbox("Select the task to run", task_names)
task = next(t for t in filtered if t["name"] == selected_name)

st.caption(task.get("desc", ""))
st.code(" ".join(task["cmd"]), language="bash")

if st.button("🚀 Run", type="primary"):
    with st.spinner("Running..."):
        res = run_cmd(cmd=task["cmd"], cwd=None, env=None, timeout_s=None)

    if res.returncode == 0:
        st.success(f"✅ Success (exit={res.returncode})")
    else:
        st.error(f"❌ Failed (exit={res.returncode})")

    tab1, tab2 = st.tabs(["stdout", "stderr"])
    with tab1:
        st.text_area("stdout", value=res.stdout, height=350)
    with tab2:
        st.text_area("stderr", value=res.stderr, height=350)

