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


@st.cache_data()
def multi_choice_maker():
    cursor, connection = sql_functions.createConnection_Cursor()
    query = """
            SELECT distinct(`tozeret_nm`) FROM cars_general
            """
    maker_df = pd.read_sql_query(query, connection)
    maker_list = maker_df['tozeret_nm'].tolist()
    return maker_list


@st.cache_data()
def home_statistics_data(maker_options):
    cursor, connection = sql_functions.createConnection_Cursor()

    if maker_options:
        # If not empty, generate a parameterized query with the IN clause
        placeholders = ', '.join(['%s'] * len(maker_options))
        query = f"SELECT COUNT(*) as Count, COUNT(DISTINCT `tozeret_nm`) as makers, " \
                f"AVG(shnat_yitzur) as average_year, " \
                f"MIN(shnat_yitzur) as min_year, MAX(shnat_yitzur) as max_year " \
                f"FROM cars_general " \
                f"WHERE tozeret_nm IN ({placeholders}) "

    else:
        # query = f"SELECT COUNT(*) as Count, MIN(shnat_yitzur) as min_year, MAX(shnat_yitzur) as max_year " \
        #         f"FROM cars_general "
        query = "SELECT * FROM aggregate_table_homestats;"
    result_df = pd.read_sql_query(query, connection, params=maker_options)
    # Access values directly without checking for results
    connection.close()
    return result_df.iloc[0]['Count'], result_df.iloc[0]['makers'], result_df.iloc[0]['average_year'],\
        result_df.iloc[0]['min_year'], result_df.iloc[0]['max_year']


@st.cache_data()
def top5manufacturers(maker_options):
    cursor, connection = sql_functions.createConnection_Cursor()
    # Check if the user_selected_makers list is empty
    if maker_options:
        # If not empty, generate a parameterized query with the IN clause
        placeholders = ', '.join(['%s'] * len(maker_options))
        query = f"SELECT tozeret_nm as Manufacturer, COUNT(tozeret_nm) AS Count " \
                f"FROM cars_general " \
                f"WHERE tozeret_nm IN ({placeholders}) " \
                f"GROUP BY tozeret_nm " \
                f"ORDER BY Count DESC " \
                f"LIMIT 5;"
    else:
        # If user didn't pick any maker = empty,
        # retrieve all makers without a WHERE clause
        # query = """
        #         SELECT tozeret_nm as Manufacturer, COUNT(tozeret_nm) AS Count
        #         FROM cars_general
        #         GROUP BY tozeret_nm
        #         ORDER BY Count DESC
        #         LIMIT 5;
        #             """
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
        query = f"SELECT CONCAT(`kinuy_mishari`, ' ', `tozeret_nm`) as Model, COUNT(`kinuy_mishari`) AS Count " \
                f"FROM cars_general " \
                f"WHERE tozeret_nm IN ({placeholders}) " \
                f"GROUP BY Model " \
                f"ORDER BY Count DESC " \
                f"LIMIT 5;"
    else:
        # If user didn't pick any maker = empty,
        # retrieve all makers without a WHERE clause
        # query = """
        #         SELECT CONCAT(`kinuy_mishari`, ' ', `tozeret_nm`) as Model, COUNT(`kinuy_mishari`) AS Count
        #         FROM cars_general
        #         GROUP BY Model
        #         ORDER BY Count DESC
        #         LIMIT 5;
        #         """
        query = "SELECT * FROM aggregated_table_top5cars;"
    top_models_df = pd.read_sql_query(query, connection, params=maker_options)
    connection.close()
    return top_models_df


@st.cache_data()
def pass_test(maker_options):
    cursor, connection = sql_functions.createConnection_Cursor()
    if maker_options:
        placeholders = ', '.join(['%s'] * len(maker_options))
        query = f"SELECT COUNT(CASE WHEN tokef_dt > CURDATE() THEN 1 END) as pass_test, " \
                f"COUNT(CASE WHEN tokef_dt <= CURDATE() THEN 1 END) as do_not " \
                f"FROM cars_specific " \
                f"JOIN cars_general ON cars_specific._id = cars_general._id " \
                f"WHERE tozeret_nm IN ({placeholders}) "
    else:
        # query = f"SELECT COUNT(CASE WHEN tokef_dt > CURDATE() THEN 1 END) as pass_test, " \
        #         f"COUNT(CASE WHEN tokef_dt <= CURDATE() THEN 1 END) as do_not " \
        #         f"FROM cars_specific "
        query = "SELECT * FROM aggregated_table_passtest;"
    pass_test_df = pd.read_sql_query(query, connection, params=maker_options)
    connection.close()
    return pass_test_df


@st.cache_data()
def ownership(maker_options):
    cursor, connection = sql_functions.createConnection_Cursor()
    placeholders = ', '.join(['%s'] * len(maker_options))
    if maker_options:
        query = f"SELECT `baalut`, count(`baalut`) as count  " \
                f"from cars_general " \
                f"WHERE tozeret_nm IN ({placeholders}) " \
                f"GROUP BY `baalut` " \
                f"ORDER BY count DESC;"
    else:
        # query = """
        #     SELECT `baalut`, count(`baalut`) as count
        #     FROM cars_general
        #     GROUP BY `baalut`
        #     ORDER BY count DESC;
        #     """
        query = "SELECT * FROM aggregated_table_ownership;"
    ownership_df = pd.read_sql_query(query, connection, params=maker_options)
    connection.close()
    return ownership_df


def time_series(maker_options):
    cursor, connection = sql_functions.createConnection_Cursor()
    if maker_options:
        placeholders = ', '.join(['%s'] * len(maker_options))
        time_series_query = f"SELECT CONCAT(YEAR(moed_aliya_lakvish), '-', MONTH(moed_aliya_lakvish)) AS " \
                            f"year__month, " \
                            f"COUNT(*) AS Count " \
                            f"FROM cars_specific " \
                            f"JOIN cars_general ON cars_specific._id = cars_general._id " \
                            f"WHERE YEAR(moed_aliya_lakvish) > 2007 " \
                            f"AND tozeret_nm IN ({placeholders}) " \
                            f"GROUP BY year__month " \
                            f"ORDER BY YEAR(moed_aliya_lakvish), MONTH(moed_aliya_lakvish);"

        year_query = f"SELECT YEAR(moed_aliya_lakvish) Year, " \
                     f"COUNT(*) AS Count " \
                     f"FROM cars_specific " \
                     f"JOIN cars_general ON cars_specific._id = cars_general._id " \
                     f"WHERE YEAR(moed_aliya_lakvish) > 2007 " \
                     f"AND tozeret_nm IN ({placeholders}) " \
                     f"GROUP BY Year " \
                     f"ORDER BY YEAR(moed_aliya_lakvish);"
    else:
        # time_series_query = """
        #                 SELECT CONCAT(YEAR(moed_aliya_lakvish), '-', MONTH(moed_aliya_lakvish)) AS year__month,
        #                 COUNT(*) AS Count
        #                 FROM cars_specific
        #                 WHERE YEAR(moed_aliya_lakvish) > 2007
        #                 GROUP BY year__month
        #                 ORDER BY YEAR(moed_aliya_lakvish), MONTH(moed_aliya_lakvish);
        #                 """
        # year_query = """
        #                 SELECT YEAR(moed_aliya_lakvish) Year,
        #                 COUNT(*) AS Count
        #                 FROM cars_specific
        #                 WHERE YEAR(moed_aliya_lakvish) > 2007
        #                 GROUP BY Year
        #                 ORDER BY YEAR(moed_aliya_lakvish);
        #                 """
        time_series_query = "SELECT * FROM aggregated_table_time_series_yearmonth;"
        year_query = "SELECT * FROM aggregated_table_time_series;"

    time_series_df = pd.read_sql_query(time_series_query, connection, params=maker_options)
    year_series_df = pd.read_sql_query(year_query, connection, params=maker_options)
    connection.close()
    return year_series_df, time_series_df


# Function to extract the first word from a sentence because we don't need the production state
def extract_first_word(sentence):
    words = re.split(r'[ -]', sentence)
    if words:
        return words[0].strip()

    # sql.dropTables()
    cursor, connection = sql_functions.creatingTables()

    # API endpoint URL
    api_url = "https://data.gov.il/api/3/action/datastore_search?resource_id=053cea08-09bc-40ec-8f7a-156f0677aff3&q="
    response = requests.get(api_url)  # Initial request to get the total number of records
    data = response.json()
    start_time = time.time()  # Measure the start time

    total_records = data.get('result', {}).get('total', 0)
    print(f"Number of rows to fetch: {total_records:,}")

    limit = 10000  # Specify the limit per request, let's go for 10k per request

    # Calculate the number of requests needed
    num_requests = (total_records + limit - 1) // limit

    # Make sequential requests with pagination, divide all the data into pages that contains 10k rows
    for page in range(num_requests):
        offset = page * limit
        params = {'limit': limit, 'offset': {offset}}
        print(params)
        response = requests.get(api_url, params=params)
        # load
        page_data = response.json()
        records = page_data.get('result', {}).get('records', [])  # Process the data from the current page (page_data)
        # Insert data into the MySQL table using parameterized query and batch insert

        insert_general_query = """
        INSERT INTO cars_general (_id, mispar_rechev,tozeret_nm, kinuy_mishari, baalut, degem_nm, ramat_gimur, 
        ramat_eivzur_betihuty, kvutzat_zihum, shnat_yitzur)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        # transform with extract_first_word

        values_general_query = [
            (record.get('_id'), record.get('mispar_rechev'),
             extract_first_word(record.get('tozeret_nm')),
             record.get('kinuy_mishari'),
             record.get('baalut'),
             record.get('degem_nm'),
             record.get('ramat_gimur'),
             record.get('ramat_eivzur_betihuty'),
             record.get('kvutzat_zihum'),
             record.get('shnat_yitzur'))
            for record in records]

        cursor.executemany(insert_general_query, values_general_query)

        insert_specific_query = """
        INSERT INTO cars_specific (_id, 'mispar_rechev', degem_manoa, moed_aliya_lakvish, mivchan_acharon_dt, tokef_dt, 
        tzeva_rechev, sug_delek_nm)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        values_specific_query = [(record.get('_id'),
                                  record.get('mispar_rechev'),
                                  record.get('degem_manoa'),
                                  record.get('moed_aliya_lakvish'),
                                  record.get('mivchan_acharon_dt'),
                                  record.get('tokef_dt'),
                                  record.get('tzeva_rechev'),
                                  record.get('sug_delek_nm'))
                                 for record in records]

        cursor.executemany(insert_specific_query, values_specific_query)
        # Convert the existing VARCHAR type column to DATE type column
        query = """
            UPDATE your_table_name
            SET your_date_column = STR_TO_DATE(your_varchar_column, '%Y-%m-%d');
        """
        cursor.execute(query)
        # Commit the changes
        connection.commit()
        print(f"Inserted {len(records)} records into MySQL (Page {page + 1}/{num_requests}).")

    # Measure the end time
    end_time = time.time()
    # Calculate and print the total time taken
    time_seconds = (end_time - start_time) / 60
    print(f"Total time taken: {time_seconds:.2f} ,minutes.")

    # Close the cursor and connection
    cursor.close()
    connection.close()


st.set_page_config(
    page_title="Cars Dashboard",
    page_icon="🚗",
    layout="wide"
)
st.title("Israel Cars Dashboard🚗")
with st.sidebar:
    maker_options = st.multiselect('Please choose Makers:', multi_choice_maker())
total_cars, total_makers, average_year,  min_year, max_year = home_statistics_data(maker_options)
# Display summary statistics in a single row
colmetric1, colmetric2, colmetric3, colmetric4, colmetric5 = st.columns(5)
colmetric1.metric(f"## Total cars:", f"{total_cars:,}")
colmetric2.metric(f"## Total makers:", f"{total_makers}")
colmetric3.metric(f"## Average year:", round(float(f"{average_year}")))
colmetric4.metric(f"## Min year:", min_year)
colmetric5.metric(f"## Max year:", max_year)

col1, col2, col3 = st.columns([0.5, 0.25, 0.25])
#################################################################################################################
year_series_data, year__month_series_data = time_series(maker_options)
# Create a line chart using Plotly Express with custom axis formatting
fig = px.line(year_series_data, x='Year', y='Count', title="Time series")
fig.update_xaxes(tickformat="", tickmode="linear")  # Disable comma formatting for thousands
# Display the chart using st.plotly_chart
# use_container_width=True will align the chart according to the container(column)
col1.plotly_chart(fig, use_container_width=True)

# tab1, tab2 = st.tabs(["Year time series", "Year-Month time series"])
# with tab1:
#     fig = px.line(year_series_data, x="Year", y="Count", width=700, height=400,
#                   title="Car registrations over time")
#
#     st.plotly_chart(fig, theme="streamlit")
# with tab2:
#     fig = px.line(year__month_series_data, x="year__month", y="Count", width=700, height=400,
#                   title="Car registrations over time")
#     st.plotly_chart(fig, theme="streamlit")
#################################################################################################################

#################################################################################################################

total_tests_df = pass_test(maker_options)
# Convert the fetched data to a Pandas DataFrame
# Create a pie chart with Plotly Express
fig = px.pie(total_tests_df, names=total_tests_df.columns, values=total_tests_df.iloc[0],
             color_discrete_sequence=["#3559E0", "#EF4040"],
             title='Test Distribution')
#fig.update_layout(height=400, width=400)
# Display the chart in Streamlit
col2.plotly_chart(fig, use_container_width=True)

#################################################################################################################

ownership_df = ownership(maker_options)
# Create a pie chart with Plotly Express
fig = px.pie(ownership_df, names='baalut', values='count', hole=.5,
             color_discrete_sequence=["#711DB0", "#508D69", "#EF4040", "#FFA732", "#3559E0"],  # 9ADE7B
             title='Ownership Distribution')
# Rotate the pie chart by adjusting the direction
fig.update_traces(rotation=90, direction='clockwise')

# Display the chart in Streamlit
col3.plotly_chart(fig, use_container_width=True)

#################################################################################################################
col4, col5 = st.columns([1, 1])
top_manufacturers_df = top5manufacturers(maker_options)
# To sort by manufacturer_count (y-axis) used the altair library for better customization
# Altair chart N:nominal (categorical) and Q:quantitative (numeric)
# chart_manu = alt.Chart(top_manufacturers_df).mark_bar(size=20).encode(
#     x=alt.X('Manufacturer:N', sort='-y', axis=alt.Axis(labelAngle=0)),  # Sort x-axis by manufacturer_count
#     y=alt.Y('Count:Q', axis=alt.Axis(format=',d'))
# ).properties(width=500, height=300, title='Top Manufacturers')
# col2.altair_chart(chart_manu, use_container_width=True)
fig = px.bar(top_manufacturers_df, y='Count', x='Manufacturer', text_auto='.2s', title="Top Manufacturers")
fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
fig.update_layout(showlegend=True)
col4.plotly_chart(fig, theme="streamlit", use_container_width=True)

#################################################################################################################

top_models_df = top5cars(maker_options)
# chart_model = (alt.Chart(top_models_df).mark_bar(size=20).encode(
#     x=alt.X('Count:Q', axis=alt.Axis(format=',d')),
#     y=alt.Y('Model:N', sort='-x'),
#     color=alt.Color('Model:N', legend=None)).properties(width=500, height=300, title='Top Car Models'))
# col3.altair_chart(chart_model, use_container_width=True)

fig = px.bar(top_models_df, y='Model', x='Count', text_auto='.2s', title="Top Car Models",
             color='Model', color_discrete_sequence=["#508D69", "#711DB0", "#EF4040", "#FFA732", "#3559E0"])
fig.update_traces(textfont_size=12, textposition='inside', cliponaxis=False)
fig.update_layout(showlegend=True)  #False = Hide the legend
fig.update_xaxes(tickformat=',s')
col5.plotly_chart(fig, theme="streamlit", use_container_width=True)

#################################################################################################################
