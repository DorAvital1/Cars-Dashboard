import streamlit as st

st.set_page_config(
    page_title="Car report",
    layout="centered",
    page_icon="❓"
)
st.title("❓ Car Dashboard Project")
st.subheader("🔎 Overview", divider="blue")

st.write("""
I created a data engineering project in order to research, plan, and program
a live report dealing with real, updated big data for the challenge 💪\n
This project analyzes the national transportation in Israel 🚗
Took the data from APIs in gov.data website which receives real and updated data
from the Ministry of Transportation.
""")
st.subheader("🎯 Goals", divider="blue")
st.write("""
1.Explore the data with nice visualizations about all the registered cars in Israel.
2.Practice & learn some technologies for software architecture.
3.answer to some questions:
 - who is the largest car manufacturer in Israel?
 - what are the most common models?
 - what is the percentage of vehicles that are not eligible to be active?
 - the distributed ownership of the vehicles.
4.In addition, build a neat-looking car report that inputs a car number and outputs relevant details about it.

""")
st.subheader("🛠️ Techs & tools", divider="blue")
st.write("""
- Coding language - Python
- Modules:
  -- Extraction - request and asyncio
  -- Data processing & transformation - pandas and re
  -- visualization - plotly.express
- for database - mysql.connector
- for building the app UI - Streamlit
""")

st.subheader("↪️ Methodology", divider="blue")
st.write("""
We are dealing with lots of data(more than 3.5 million instances), therefore we should Build our ETL strategy beforehand.\n
- **Extraction** - To start the data pipeline process, we would extract the data from APIs that return the data, and then we would process it asynchrony in Python in order to run it much faster than getting a request for each call.
This data will go to MySQL tables for better performance.
- **Transform** - In Python select data from MySQL tables, edit, alter, and change formats for Dataframe objects.
- **Load** - query in Python the data with logic for the visualizations.
In addition, process logic of filters in the dashboard and extraction of data by inputting the car's number in the database and outputting relevant info about it.
""")

st.markdown(
    f"""
    <div style="display: flex; justify-content: center; align-items: center; height: 50vh;">
        <p style="text-align: center;">Made with ❤️ by Dor Avital</p>
    </div>
    """,
    unsafe_allow_html=True
)