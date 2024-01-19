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
a live & updated report with real data, for the challenge 💪 dealing with big data.\n
This project analyzes the national transportation in Israel 🚗\n
Took the data from APIs in gov.data website which receives real and updated data
from the Ministry of Transportation and transfer it to MySql database for better performance.
""")
st.subheader("🎯 Goals", divider="blue")
st.write("""
1. Explore the data with nice visualizations about all the registered cars in Israel.\n
2. Practice & learn some technologies for software architecture.\n
3. answer to some questions:
 - who is the largest car manufacturer in Israel?
 - what are the most common models?
 - what is the percentage of vehicles that are not eligible to be active?
 - the distributed ownership of the vehicles.\n
4. In addition, build a neat-looking car report that inputs a car number and outputs relevant details about it.

""")
st.subheader("🛠️ Tech stack & tools", divider="blue")
st.write("""
- Coding language - Python
- Modules:\n
  - Extraction - request and asyncio\n
  - Data processing & transformation - pandas and re\n
  - visualization - plotly.express\n
  - Database connection - pymysql
- Remote Database - Mysql 
- Backend and Building the app - Streamlit
""")

st.subheader("↪️ Methodology", divider="blue")
st.image('diagram.png', caption='Architecture for the project')
st.write("""
We are dealing with lots of data(more than 3.8 million instances), therefore we should Build our ETL strategy beforehand.\n
- **Extraction** - To start the data pipeline process, we would extract the data from APIs that return the data, and then we would process it in Python so we could
organize the data, and than will go to MySQL tables for better performance. (querying & aggregating)
- **Transform** - select data from MySQL tables, edit, alter and update data & formats for Dataframe objects.
- **Load** - query the data with logic for the visualizations and for the car report.
In addition, process logic of filters in the dashboard and extraction of data by 
inputting the car's number in the database and outputting relevant info about it.
""")

st.markdown(
    f"""
    <div style="display: flex; justify-content: center; align-items: center; height: 50vh;">
        <p style="text-align: center;">Made with ❤️ by Dor Avital</p>
    </div>
    """,
    unsafe_allow_html=True
)