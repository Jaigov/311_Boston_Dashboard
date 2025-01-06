import streamlit as st

#st.set_page_config(page_title="311 Service Requests", layout="wide")

# Set page configuration with custom icon and layout
st.set_page_config(
    page_title="PREDICT 311",
    page_icon="https://play-lh.googleusercontent.com/-ei9jmjQa4C0dLIciwW3BF9Ym0M7_tjJlmlrDX3SxPQ7y5qflwWUsaGxuRzZyJBFZwg",
    layout="wide"
)

pg = st.navigation([
    st.Page("dashboard.py", title="Visualization", icon=":material/chat:"), 
    st.Page("prediction.py", title="Service Duration Prediction", icon=":material/text_snippet:"),
    st.Page("llm.py", title="Text to SQL", icon=":material/assignment:")
    ])
pg.run()