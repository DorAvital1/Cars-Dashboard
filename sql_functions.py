import time
import pymysql
import requests
import streamlit as st
import re
import os
from dotenv import load_dotenv
import cryptography


# Function to extract the first word from a sentence because we don't need the production state
def extract_first_word(sentence):
    words = re.split(r'[ -]', sentence)
    if words:
        return words[0].strip()



def create_Connection_Cursor():
    timeout = 1000000
    connection = pymysql.connect(
        charset="utf8mb4",
        connect_timeout=timeout,
        db=st.secrets["MYSQL_DATABASE"],
        host=st.secrets["MYSQL_HOST"],
        password=st.secrets["MYSQL_PASSWORD"],
        read_timeout=timeout,
        port=14903,
        user=st.secrets["MYSQL_USER"],
        write_timeout=timeout,
    )
    return connection.cursor(), connection


def insert_gen_data_sync_v():
    cursor, connection = createConnection_Cursor()
    # API endpoint URL - תוצרים ודגמים של כלי רכב WLTP
    api_url = "https://data.gov.il/api/3/action/datastore_search?resource_id=053cea08-09bc-40ec-8f7a-156f0677aff3&q"

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
        print(params, "page:", page, "/", num_requests)
        response = requests.get(api_url, params=params)
        # load
        page_data = response.json()
        records = page_data.get('result', {}).get('records', [])  # Process the data from the current page (page_data)
        # Insert data into the MySQL table using parameterized query and batch insert

        insert_general_query = """
               INSERT INTO cars_general (
                   _id, mispar_rechev, tozeret_nm, kinuy_mishari, baalut, degem_nm, ramat_gimur, 
                   ramat_eivzur_betihuty, kvutzat_zihum, tozeret_cd, degem_cd, shnat_yitzur,
                   degem_manoa, moed_aliya_lakvish, mivchan_acharon_dt, tokef_dt, tzeva_rechev, sug_delek_nm
               )
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
                   mispar_rechev=VALUES(mispar_rechev), tozeret_nm=VALUES(tozeret_nm), kinuy_mishari=VALUES(kinuy_mishari),
                   baalut=VALUES(baalut), degem_nm=VALUES(degem_nm), ramat_gimur=VALUES(ramat_gimur),
                   ramat_eivzur_betihuty=VALUES(ramat_eivzur_betihuty), kvutzat_zihum=VALUES(kvutzat_zihum),
                   tozeret_cd=VALUES(tozeret_cd), degem_cd=VALUES(degem_cd), shnat_yitzur=VALUES(shnat_yitzur),
                   degem_manoa=VALUES(degem_manoa), moed_aliya_lakvish=VALUES(moed_aliya_lakvish),
                   mivchan_acharon_dt=VALUES(mivchan_acharon_dt),
                   tokef_dt=VALUES(tokef_dt), tzeva_rechev=VALUES(tzeva_rechev), sug_delek_nm=VALUES(sug_delek_nm);
           """

        values_general_query = [(
            record.get('_id'),
            record.get('mispar_rechev'),
            extract_first_word(record.get('tozeret_nm')),
            record.get('kinuy_mishari'),
            record.get('baalut'),
            record.get('degem_nm'),
            record.get('ramat_gimur'),
            record.get('ramat_eivzur_betihuty'),
            record.get('kvutzat_zihum'),
            record.get('tozeret_cd'),
            record.get('degem_cd'),
            record.get('shnat_yitzur'),
            record.get('degem_manoa'),
            record.get('moed_aliya_lakvish'),
            record.get('mivchan_acharon_dt'),
            record.get('tokef_dt'),
            record.get('tzeva_rechev'),
            record.get('sug_delek_nm'))
            for record in records]

        cursor.executemany(insert_general_query, values_general_query)
        connection.commit()
    # Measure the end time
    end_time = time.time()
    # Calculate and print the total time taken
    time_seconds = (end_time - start_time)
    # Calculate and print the total time taken
    if time_seconds < 60:
        print(f"Execution time: {time_seconds:.5f} seconds")
    else:
        time_minutes = time_seconds / 60
        print(f"Execution time: {time_minutes:.2f} minutes")


def add_tozar_to_gen():
    cursor, connection = create_Connection_Cursor()
    with connection.cursor() as cursor:
        # Define the SQL query to update the column in destination_table
        sql = """
            UPDATE cars_general
            JOIN cars_technical ON cars_general.tozeret_cd = cars_technical.tozeret_cd
            AND cars_general.degem_cd = cars_technical.degem_cd
            AND cars_general.shnat_yitzur = cars_technical.shnat_yitzur
            SET cars_general.tozar = cars_technical.tozar;
              """
        # Execute the SQL query
        cursor.execute(sql)
