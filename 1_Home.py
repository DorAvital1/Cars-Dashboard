import requests
import pymysql
import time
import sql_functions
import re
import pandas as pd
import streamlit as st
import plotly.express as px
import os
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

@st.cache_data()
def multi_choice_maker():
    cursor, connection = sql_functions.create_Connection_Cursor()
    query = """
            SELECT DISTINCT tozar
            FROM cars_general
            WHERE tozar IS NOT NULL
            ORDER BY tozar;
            """
    maker_df = pd.read_sql_query(query, connection)
    maker_options = maker_df['tozar'].tolist()
    cursor.close()
    connection.close()
    return maker_options


@st.cache_data()
def home_statistics_data(maker_options):
    cursor, connection = sql_functions.create_Connection_Cursor()
    if maker_options:
        # If not empty, generate a parameterized query with the IN clause
        placeholders = ', '.join(['%s'] * len(maker_options))
        query = f"""
                SELECT COUNT(*) AS Count, COUNT(DISTINCT tozar) AS makers, AVG(shnat_yitzur) AS average_year,
                MIN(shnat_yitzur) AS min_year, MAX(shnat_yitzur) AS max_year
                FROM cars_general
                WHERE tozar IN ({placeholders});
                """
    else:
        query = "SELECT * FROM aggregate_table_homestats;"
    result_df = pd.read_sql_query(query, connection, params=maker_options)
    cursor.close()
    connection.close()
    return result_df.iloc[0]['Count'], result_df.iloc[0]['makers'], result_df.iloc[0]['average_year'], \
        result_df.iloc[0]['min_year'], result_df.iloc[0]['max_year']

#@st.cache_data()
def time_series(maker_options):
    cursor, connection = sql_functions.create_Connection_Cursor()
    if maker_options:
        placeholders = ', '.join(['%s'] * len(maker_options))
        year_query = f"""
                    SELECT CAST(LEFT(moed_aliya_lakvish, 4) AS UNSIGNED) AS Year,
                    COUNT(*) AS Count
                    FROM cars_general
                    WHERE moed_aliya_lakvish > '2007-01-01'
                    AND tozar IN ({placeholders})
                    GROUP BY Year
                    ORDER BY Year;
                    """
    else:
        year_query = "SELECT * FROM aggregated_table_time_series;"
    year_series_df = pd.read_sql_query(year_query, connection, params=maker_options)
    cursor.close()
    connection.close()
    return year_series_df


@st.cache_data()
def top5manufacturers(maker_options):
    cursor, connection = sql_functions.create_Connection_Cursor()
    # Check if the user_selected_makers list is empty
    if maker_options:
        # If not empty, generate a parameterized query with the IN clause
        placeholders = ', '.join(['%s'] * len(maker_options))
        query = f"""
                SELECT
                tozar AS Manufacturer, COUNT(*) AS Count
                FROM cars_general
                WHERE tozar IN ({placeholders})
                GROUP BY tozar
                ORDER BY Count DESC
                LIMIT 5;
                """
    else:
        # If user didn't pick any maker = empty,
        # retrieve all makers without a WHERE clause
        query = """
                select * from aggregated_table_top5manufacturers;
                """
    # Because this table build with CONCAT() function as PK and INSERT clause override the order by operation,
    # I use pandas sort_values to sort by column "Count" desc with ease.
    top_manufacturers_df = pd.read_sql_query(query, connection, params=maker_options)
    top_manufacturers_df_sorted = top_manufacturers_df.sort_values(by="Count", ascending=False)
    cursor.close()
    connection.close()
    return top_manufacturers_df_sorted


@st.cache_data()
def top5cars(maker_options):
    cursor, connection = sql_functions.create_Connection_Cursor()
    if maker_options:
        # If not empty, generate a parameterized query with the IN clause
        placeholders = ', '.join(['%s'] * len(maker_options))
        query = f"""
                SELECT CONCAT(kinuy_mishari, ' ', tozar) as Model,
                COUNT(kinuy_mishari) AS Count
                FROM cars_general
                WHERE tozar IN ({placeholders})
                GROUP BY CONCAT(kinuy_mishari, ' ', tozar)
                ORDER BY Count DESC
                LIMIT 5;
                """
    else:
        # If user didn't pick any maker = empty,
        # retrieve all makers without a WHERE clause
        query = """
                SELECT * FROM aggregated_table_top5cars;
                """
    top_models_df = pd.read_sql_query(query, connection, params=maker_options)
    top_models_df_sorted = top_models_df.sort_values(by="Count", ascending=False)
    cursor.close()
    connection.close()
    return top_models_df_sorted


@st.cache_data()
def pass_test(maker_options):
    cursor, connection = sql_functions.create_Connection_Cursor()
    if maker_options:
        placeholders = ', '.join(['%s'] * len(maker_options))
        query = f"""
                SELECT COUNT(CASE WHEN tokef_dt > CURDATE() THEN 1 END) as pass_test,
                      COUNT(CASE WHEN tokef_dt <= CURDATE() THEN 1 END) as do_not_pass
                FROM cars_general
                WHERE tozar IN ({placeholders});
                """
    else:
        query = "SELECT * FROM aggregated_table_passtest;"
    pass_test_df = pd.read_sql_query(query, connection, params=maker_options)
    cursor.close()
    connection.close()
    return pass_test_df


@st.cache_data()
def ownership(maker_options):
    cursor, connection = sql_functions.create_Connection_Cursor()
    placeholders = ', '.join(['%s'] * len(maker_options))
    if maker_options:
        query = f"""
                SELECT baalut, COUNT(baalut) as count
                FROM cars_general
                WHERE tozar IN ({placeholders})
                GROUP BY baalut
                ORDER BY count DESC;
                """
    else:
        query = "SELECT * FROM aggregated_table_ownership;"
    ownership_df = pd.read_sql_query(query, connection, params=maker_options)
    cursor.close()
    connection.close()
    return ownership_df

st.set_page_config(
    page_title="Cars Dashboard",
    page_icon="🚗",
    layout="wide"
)
st.title("🚗 Israel Cars Dashboard")


with st.sidebar:
    maker_options = st.multiselect('Please choose Makers:', multi_choice_maker())
total_cars, total_makers, average_year, min_year, max_year = home_statistics_data(maker_options)
# Display summary statistics in a single row
colmetric1, colmetric2, colmetric3, colmetric4, colmetric5 = st.columns(5)
colmetric1.metric(f"## Total cars:", f"{round(float(total_cars)):,}")
colmetric2.metric(f"## Total makers:", round(float(f"{total_makers}")))
colmetric3.metric(f"## Average year:", round(float(f"{average_year}")))
colmetric4.metric(f"## Min year:", round(float(f"{min_year}")))
colmetric5.metric(f"## Max year:", round(float(max_year)))

col1, col2, col3 = st.columns([0.5, 0.25, 0.25])
#################################################################################################################
year_series_data = time_series(maker_options)
# Create a line chart using Plotly Express with custom axis formatting
fig = px.line(year_series_data, x='Year', y='Count', title="Time series")
fig.update_xaxes(tickformat="", tickmode="linear")  # Disable comma formatting for thousands
# Display the chart using st.plotly_chart
# use_container_width=True will align the chart according to the container(column)
fig.update_traces(textfont_size=18)
col1.plotly_chart(fig, use_container_width=True)

#################################################################################################################

total_tests_df = pass_test(maker_options)
# Convert the fetched data to a Pandas DataFrame
# Create a pie chart with Plotly Express
fig = px.pie(total_tests_df, names=total_tests_df.columns, values=total_tests_df.iloc[0],
             color_discrete_sequence=["#3559E0", "#EF4040"],
             title='Test Distribution')
# Display the chart in Streamlit
fig.update_traces(textfont_size=18)
col2.plotly_chart(fig, use_container_width=True)

#################################################################################################################

ownership_df = ownership(maker_options)
# Create a pie chart with Plotly Express
fig = px.pie(ownership_df, names='baalut', values='count', hole=.5,
             color_discrete_sequence=["#711DB0", "#508D69", "#EF4040", "#FFA732", "#3559E0"],  # 9ADE7B
             title='Ownership Distribution')
# Rotate the pie chart by adjusting the direction
fig.update_traces(textfont_size=18, rotation=120, direction='clockwise')

# Display the chart in Streamlit
col3.plotly_chart(fig, use_container_width=True)

#################################################################################################################
col4, col5 = st.columns([1, 1])
top_manufacturers_df = top5manufacturers(maker_options)
fig = px.bar(top_manufacturers_df, y='Count', x='Manufacturer', text_auto='.2s', title="Top Manufacturers")
fig.update_traces(textfont_size=18, textangle=0, textposition="outside", cliponaxis=False)
fig.update_layout(showlegend=True)
col4.plotly_chart(fig, theme="streamlit", use_container_width=True)

#################################################################################################################

top_models_df = top5cars(maker_options)
fig = px.bar(top_models_df, y='Model', x='Count', text_auto='.2s', title="Top Car Models",
             color='Model', color_discrete_sequence=["#508D69", "#711DB0", "#EF4040", "#FFA732", "#3559E0"])
fig.update_traces(textfont_size=18, textposition='inside', cliponaxis=False)
fig.update_layout(showlegend=True)  # False = Hide the legend
fig.update_xaxes(tickformat=',s')
col5.plotly_chart(fig, theme="streamlit", use_container_width=True)

#################################################################################################################
st.markdown(
    f"""
    <div style="display: flex; justify-content: center; align-items: center; height: 50vh;">
        <p style="text-align: center;">Made with ❤️ by Dor Avital</p>
    </div>
    """,
    unsafe_allow_html=True
)
