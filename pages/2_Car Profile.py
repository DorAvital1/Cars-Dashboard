import pandas as pd
import streamlit as st
import plotly.express as px
import sql_functions
import time
from datetime import datetime, date
import os
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

st.set_page_config(
    page_title="Car report",
    layout="centered",
    page_icon="📜",
)
st.title("📜 Car report")


# Take input from the user
def input_from_user(car_number):
    cursor, connection = sql_functions.create_Connection_Cursor()
    query = f"SELECT 1 FROM cars_general WHERE mispar_rechev = %s LIMIT 1"
    cursor.execute(query, (car_number,))  # this syntax present tuple that arrive from cursor.execute()
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    return result is not None


def data_about_car(car_number):
    cursor, connection = sql_functions.create_Connection_Cursor()
    # Create the SQL query with placeholders

    query = """
            SELECT cg.*, ct.*
            FROM (SELECT * FROM cars_general WHERE mispar_rechev = %s) cg
            NATURAL JOIN cars_technical ct;
            """
    # Use a tuple to pass the car_number as a parameter
    cursor.execute(query, (car_number,))
    result = cursor.fetchone()

    # Extract column names from cursor description
    columns_to_retrieve = []
    for column_type in cursor.description:
        columns_to_retrieve.append(column_type[0])
    # Close the database connection
    cursor.close()
    connection.close()

    # Create a dictionary with column names as keys using a key-value loop
    car_data = {}
    for i in range(len(columns_to_retrieve)):
        column_name = columns_to_retrieve[i]
        column_value = result[i]
        car_data[column_name] = column_value
    return car_data


def date_differ_today(date_for_differ):
    # Convert the string to a date object
    expire_test_date = datetime.strptime(date_for_differ, "%Y-%m-%d").date()
    today_date = date.today()
    # Calculate the difference between the two dates
    date_difference = expire_test_date - today_date
    return date_difference.days


car_number = st.text_input("Enter the Car Number:", value="")
Submit_Car, reset_button = st.button("Submit Car Number", use_container_width=True), \
    st.button("Reset", use_container_width=True)
if Submit_Car:
    if car_number is not None:
        # Check if the car number exists in the database
        if input_from_user(car_number):
            st.success("The entered car number exists in the database.")
            st.spinner('Loading...')
            time.sleep(2)
            car_data = data_about_car(car_number)

            # Create a container for the main content
            main_container = st.container()

            with main_container:
                st.subheader(f"General information about: {car_data['mispar_rechev']}", divider='rainbow')
                col1, col2, col3 = st.columns(3)
                col1.metric("Manufacturer", car_data['tozar'])
                col1.metric("Manufacturer code", car_data['tozeret_cd'])
                col1.metric("Model code", car_data['degem_cd'])
                col2.metric("Commercial name", car_data['kinuy_mishari'])
                col2.metric("Color", car_data['tzeva_rechev'])
                col2.metric("Year", car_data['shnat_yitzur'])
                col3.metric("Ownership", car_data['baalut'])
                col3.metric("Model", car_data['degem_nm'])
                col3.metric("Fuel", car_data['sug_delek_nm'])

                st.divider()
                st.subheader("Technical information", divider='rainbow')
                col4, col5, col6 = st.columns(3)
                col4.metric("Horse power", car_data['koah_sus'])
                col4.metric("Ignition", car_data['hanaa_nm'])
                col4.metric("Nu. of seats", car_data['mispar_moshavim'])
                col4.metric("Nu. of doors", car_data['mispar_dlatot'])
                col4.metric("Nu. of electric windows", car_data['mispar_halonot_hashmal'])
                col5.metric("Nu. of airbags", car_data['mispar_kariot_avir'])
                col5.metric("Finish", car_data['ramat_gimur'])
                col5.metric("Weight(kg)", car_data['mishkal_kolel'])
                col5.metric("Engine model", car_data['degem_manoa'])
                col5.metric("Engine size", car_data['nefah_manoa'])
                col6.metric("Is automatic", "✔" if car_data['automatic_ind'] else "❌")
                col6.metric("Has air condition", "✔" if car_data['mazgan_ind'] else "❌")
                col6.metric("Has abs", "✔" if car_data['abs_ind'] else "❌")
                col6.metric("Has power wheel", "✔" if car_data['hege_koah_ind'] else "❌")
                col6.metric("Has stability control", "✔" if car_data['bakarat_yatzivut_ind'] else "❌")

                st.divider()
                st.subheader("On the road", divider='rainbow')
                col7, col8, col9 = st.columns(3)
                col7.metric("First Ride", car_data['moed_aliya_lakvish'])
                col8.metric("Last test", car_data['mivchan_acharon_dt'])
                col9.metric("Test expired on", car_data['tokef_dt'], delta=date_differ_today(car_data['tokef_dt']))

            if car_data['ramat_eivzur_betihuty']:
                st.slider('Safety score', min_value=0, max_value=8
                          , value=int(car_data['ramat_eivzur_betihuty']), label_visibility="visible", disabled=True)
            else:
                st.write("Safety score")
                st.warning("there is no data available for this value")

            if car_data['kvutzat_zihum']:
                st.slider('Pollution score', min_value=1, max_value=15
                          , value=int(car_data['kvutzat_zihum']), label_visibility="visible", disabled=True)
            else:
                st.write("Pollution score")
                st.warning("there is no data available for this value")
            st.balloons()

        else:
            st.warning("That doesn't exist in the database.")
# Reset button to clear the input
if reset_button:
    # Clear the input field on the screen
    car_number = None


st.markdown(
    f"""
    <div style="display: flex; justify-content: center; align-items: center; height: 50vh;">
        <p style="text-align: center;">Made with ❤️ by Dor Avital</p>
    </div>
    """,
    unsafe_allow_html=True
)