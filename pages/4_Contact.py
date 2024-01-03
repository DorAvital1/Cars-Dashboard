import streamlit as st

st.set_page_config(
    page_title="Car report",
    layout="centered",
    page_icon="✉️"
)
st.title("✉️ Contact")
st.subheader("Will be happy to chat 😊")
col1, col2, col3 = st.columns([1, 1, 5])
col1.link_button("Linkedin", "https://www.linkedin.com/in/doravital/")
col2.link_button("Github", "https://github.com/DorAvital1/")
st.markdown(
    f"""
    <div style="display: flex; justify-content: center; align-items: center; height: 50vh;">
        <p style="text-align: center;">Made with ❤️ by Dor Avital</p>
    </div>
    """,
    unsafe_allow_html=True
)