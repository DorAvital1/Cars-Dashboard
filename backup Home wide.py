import requests
import mysql.connector
import time
import sql_functions
import re
import pandas as pd
import streamlit as st
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

@st.cache_data()
def multi_choice_maker():
    cursor, connection = sql_functions.createConnection_Cursor()
    query = """
            SELECT distinct(`tozar`) FROM cars_technical
            ORDER BY tozar
            """
    maker_df = pd.read_sql_query(query, connection)
    maker_list = maker_df['tozar'].tolist()
    return maker_list


@st.cache_data()
def home_statistics_data(maker_options):
    cursor, connection = sql_functions.createConnection_Cursor()
    if maker_options:
        # If not empty, generate a parameterized query with the IN clause
        placeholders = ', '.join(['%s'] * len(maker_options))
        query = f"""
                SELECT COUNT(*) AS Count, COUNT(DISTINCT ct.tozar) AS makers, AVG(cg.shnat_yitzur) AS average_year,
                MIN(cg.shnat_yitzur) AS min_year, MAX(cg.shnat_yitzur) AS max_year
                FROM cars_general cg
                JOIN cars_technical ct
                ON cg.tozeret_cd = ct.tozeret_cd AND cg.degem_cd = ct.degem_cd AND cg.shnat_yitzur = ct.shnat_yitzur
                WHERE ct.tozar IN ({placeholders});
                """
    else:
        query = "SELECT * FROM aggregate_table_homestats;"
    result_df = pd.read_sql_query(query, connection, params=maker_options)
    # Access values directly without checking for results
    connection.close()
    return result_df.iloc[0]['Count'], result_df.iloc[0]['makers'], result_df.iloc[0]['average_year'], \
        result_df.iloc[0]['min_year'], result_df.iloc[0]['max_year']


def time_series(maker_options):
    cursor, connection = sql_functions.createConnection_Cursor()
    if maker_options:
        placeholders = ', '.join(['%s'] * len(maker_options))
        year_query = f"""
                    SELECT YEAR(STR_TO_DATE(cg.moed_aliya_lakvish, '%Y-%m')) AS Year, 
                    COUNT(*) AS Count 
                    FROM cars_general cg
                    JOIN cars_technical ct 
                    ON cg.tozeret_cd = ct.tozeret_cd AND cg.degem_cd = ct.degem_cd AND cg.shnat_yitzur = ct.shnat_yitzur
                    WHERE cg.moed_aliya_lakvish > '2007-01-01'
                    AND ct.tozar IN ({placeholders})
                    GROUP BY Year 
                    ORDER BY Year;
                    """
    else:
        year_query = "SELECT * FROM aggregated_table_time_series;"
    year_series_df = pd.read_sql_query(year_query, connection, params=maker_options)
    connection.close()
    return year_series_df


@st.cache_data()
def top5manufacturers(maker_options):
    cursor, connection = sql_functions.createConnection_Cursor()
    # Check if the user_selected_makers list is empty
    if maker_options:
        # If not empty, generate a parameterized query with the IN clause
        placeholders = ', '.join(['%s'] * len(maker_options))
        query = f"""
                SELECT
                ct.tozar AS Manufacturer, COUNT(*) AS Count
                FROM cars_general cg
                JOIN cars_technical ct 
                ON cg.tozeret_cd = ct.tozeret_cd AND cg.degem_cd = ct.degem_cd AND cg.shnat_yitzur = ct.shnat_yitzur
                WHERE ct.tozar IN ({placeholders})
                GROUP BY ct.tozar
                ORDER BY Count DESC
                LIMIT 5;
                """
    else:
        # If user didn't pick any maker = empty,
        # retrieve all makers without a WHERE clause
        query = "SELECT * FROM aggregated_table_top5manufacturers;"
    top_manufacturers_df = pd.read_sql_query(query, connection, params=maker_options)
    connection.close()
    return top_manufacturers_df


@st.cache_data()
def top5cars(maker_options):
    cursor, connection = sql_functions.createConnection_Cursor()
    if maker_options:
        # If not empty, generate a parameterized query with the IN clause
        placeholders = ', '.join(['%s'] * len(maker_options))
        query = f"""
                SELECT CONCAT(cg.kinuy_mishari, ' ', ct.tozar) as Model, 
                COUNT(cg.kinuy_mishari) AS Count
                FROM cars_general cg
                JOIN cars_technical ct 
                ON cg.tozeret_cd = ct.tozeret_cd AND cg.degem_cd = ct.degem_cd AND cg.shnat_yitzur = ct.shnat_yitzur
                WHERE ct.tozar IN ({placeholders})
                GROUP BY Model
                ORDER BY Count DESC
                LIMIT 5;
                """
    else:
        # If user didn't pick any maker = empty,
        # retrieve all makers without a WHERE clause
        query = "SELECT * FROM aggregated_table_top5cars;"
    top_models_df = pd.read_sql_query(query, connection, params=maker_options)
    connection.close()
    return top_models_df


@st.cache_data()
def pass_test(maker_options):
    cursor, connection = sql_functions.createConnection_Cursor()
    if maker_options:
        placeholders = ', '.join(['%s'] * len(maker_options))
        query = f"""
                SELECT COUNT(CASE WHEN tokef_dt > CURDATE() THEN 1 END) as pass_test,
                       COUNT(CASE WHEN tokef_dt <= CURDATE() THEN 1 END) as do_not_pass
                FROM cars_general cg
                JOIN cars_technical ct 
                ON cg.tozeret_cd = ct.tozeret_cd AND cg.degem_cd = ct.degem_cd AND cg.shnat_yitzur = ct.shnat_yitzur
                WHERE ct.tozar IN ({placeholders});
                """
    else:
        query = "SELECT * FROM aggregated_table_passtest;"
    pass_test_df = pd.read_sql_query(query, connection, params=maker_options)
    connection.close()
    return pass_test_df


@st.cache_data()
def ownership(maker_options):
    cursor, connection = sql_functions.createConnection_Cursor()
    placeholders = ', '.join(['%s'] * len(maker_options))
    if maker_options:
        query = f"""
                SELECT baalut, COUNT(baalut) as count 
                FROM cars_general cg
                JOIN cars_technical ct 
                ON cg.tozeret_cd = ct.tozeret_cd AND cg.degem_cd = ct.degem_cd AND cg.shnat_yitzur = ct.shnat_yitzur
                WHERE ct.tozar IN ({placeholders})
                GROUP BY baalut  
                ORDER BY count DESC;
                """
    else:
        query = "SELECT * FROM aggregated_table_ownership;"
    ownership_df = pd.read_sql_query(query, connection, params=maker_options)
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
