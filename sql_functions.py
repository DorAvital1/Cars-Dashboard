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
        db=os.environ.get(MYSQL_DATABASE),
        host=os.environ.get("MYSQL_HOST"),
        password=os.environ.get(MYSQL_PASSWORD),
        read_timeout=timeout,
        port=14903,
        user=os.environ.get(MYSQL_USER),
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









# import time
# import mysql.connector
# import requests
# import streamlit as st
# import re
# import os
# from dotenv import load_dotenv
# import cryptography
#
# # Load environment variables from .env file
# load_dotenv()
#
#
# def createConnection_Cursor():
#     # MySQL connection configuration
#     connection = mysql.connector.connect(
#         # host=st.secrets["MYSQL_HOST"],
#         # user=st.secrets["MYSQL_USER"],
#         # password=st.secrets["MYSQL_PASSWORD"],
#         # database=st.secrets["MYSQL_DATABASE"],
#         host=os.getenv("MYSQL_HOST"),
#         user=os.getenv("MYSQL_USER"),
#         password=os.getenv("MYSQL_PASSWORD"),
#         database=os.getenv("MYSQL_DATABASE")
#     )
#     cursor = connection.cursor()  # Create a cursor object to interact with the database
#     return cursor, connection
#
#
# # Function to extract the first word from a sentence because we don't need the production state
# def extract_first_word(sentence):
#     words = re.split(r'[ -]', sentence)
#     if words:
#         return words[0].strip()
#
#
# def insert_gen_data_sync_v():
#     cursor, connection = createConnection_Cursor()
#     # API endpoint URL - תוצרים ודגמים של כלי רכב WLTP
#     api_url = "https://data.gov.il/api/3/action/datastore_search?resource_id=053cea08-09bc-40ec-8f7a-156f0677aff3&q"
#
#     response = requests.get(api_url)  # Initial request to get the total number of records
#     data = response.json()
#     start_time = time.time()  # Measure the start time
#
#     total_records = data.get('result', {}).get('total', 0)
#     print(f"Number of rows to fetch: {total_records:,}")
#
#     limit = 5000  # Specify the limit per request, let's go for 10k per request
#
#     # Calculate the number of requests needed
#     num_requests = (total_records + limit - 1) // limit
#
#     # Make sequential requests with pagination, divide all the data into pages that contains 10k rows
#     for page in range(num_requests):
#         offset = page * limit
#         params = {'limit': limit, 'offset': {offset}}
#         print(params)
#         response = requests.get(api_url, params=params)
#         # load
#         page_data = response.json()
#         records = page_data.get('result', {}).get('records', [])  # Process the data from the current page (page_data)
#         # Insert data into the MySQL table using parameterized query and batch insert
#
#         insert_general_query = """
#                INSERT INTO cars_general (
#                    _id, mispar_rechev, tozeret_nm, kinuy_mishari, baalut, degem_nm, ramat_gimur,
#                    ramat_eivzur_betihuty, kvutzat_zihum, tozeret_cd, degem_cd, shnat_yitzur,
#                    degem_manoa, moed_aliya_lakvish, mivchan_acharon_dt, tokef_dt, tzeva_rechev, sug_delek_nm
#                )
#                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#                ON DUPLICATE KEY UPDATE
#                    mispar_rechev=VALUES(mispar_rechev), tozeret_nm=VALUES(tozeret_nm), kinuy_mishari=VALUES(kinuy_mishari),
#                    baalut=VALUES(baalut), degem_nm=VALUES(degem_nm), ramat_gimur=VALUES(ramat_gimur),
#                    ramat_eivzur_betihuty=VALUES(ramat_eivzur_betihuty), kvutzat_zihum=VALUES(kvutzat_zihum),
#                    tozeret_cd=VALUES(tozeret_cd), degem_cd=VALUES(degem_cd), shnat_yitzur=VALUES(shnat_yitzur),
#                    degem_manoa=VALUES(degem_manoa), moed_aliya_lakvish=VALUES(moed_aliya_lakvish),
#                    mivchan_acharon_dt=VALUES(mivchan_acharon_dt),
#                    tokef_dt=VALUES(tokef_dt), tzeva_rechev=VALUES(tzeva_rechev), sug_delek_nm=VALUES(sug_delek_nm);
#            """
#
#         values_general_query = [(
#             record.get('_id'),
#             record.get('mispar_rechev'),
#             extract_first_word(record.get('tozeret_nm')),
#             record.get('kinuy_mishari'),
#             record.get('baalut'),
#             record.get('degem_nm'),
#             record.get('ramat_gimur'),
#             record.get('ramat_eivzur_betihuty'),
#             record.get('kvutzat_zihum'),
#             record.get('tozeret_cd'),
#             record.get('degem_cd'),
#             record.get('shnat_yitzur'),
#             record.get('degem_manoa'),
#             record.get('moed_aliya_lakvish'),
#             record.get('mivchan_acharon_dt'),
#             record.get('tokef_dt'),
#             record.get('tzeva_rechev'),
#             record.get('sug_delek_nm'))
#             for record in records]
#
#         cursor.executemany(insert_general_query, values_general_query)
#         connection.commit()
#     # Measure the end time
#     end_time = time.time()
#     # Calculate and print the total time taken
#     time_seconds = (end_time - start_time)
#     # Calculate and print the total time taken
#     if time_seconds < 60:
#         print(f"Execution time: {time_seconds:.5f} seconds")
#     else:
#         time_minutes = time_seconds / 60
#         print(f"Execution time: {time_minutes:.2f} minutes")
#
#
# def insert_gen_data_Async_v():
#     async def fetch_data(session, api_url, params):
#         async with session.get(api_url, params=params) as response:
#             return await response.json()
#
#     async def process_data(cursor, records):
#         insert_general_query = """
#             INSERT INTO cars_general (
#                 _id, mispar_rechev, tozeret_nm, kinuy_mishari, baalut, degem_nm, ramat_gimur,
#                 ramat_eivzur_betihuty, kvutzat_zihum, tozeret_cd, degem_cd, shnat_yitzur,
#                 degem_manoa, moed_aliya_lakvish, mivchan_acharon_dt, tokef_dt, tzeva_rechev, sug_delek_nm
#             )
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#             ON DUPLICATE KEY UPDATE
#                 mispar_rechev=VALUES(mispar_rechev), tozeret_nm=VALUES(tozeret_nm), kinuy_mishari=VALUES(kinuy_mishari),
#                 baalut=VALUES(baalut), degem_nm=VALUES(degem_nm), ramat_gimur=VALUES(ramat_gimur),
#                 ramat_eivzur_betihuty=VALUES(ramat_eivzur_betihuty), kvutzat_zihum=VALUES(kvutzat_zihum),
#                 tozeret_cd=VALUES(tozeret_cd), degem_cd=VALUES(degem_cd), shnat_yitzur=VALUES(shnat_yitzur),
#                 degem_manoa=VALUES(degem_manoa), moed_aliya_lakvish=VALUES(moed_aliya_lakvish),
#                 mivchan_acharon_dt=VALUES(mivchan_acharon_dt),
#                 tokef_dt=VALUES(tokef_dt), tzeva_rechev=VALUES(tzeva_rechev), sug_delek_nm=VALUES(sug_delek_nm);
#         """
#
#         values_general_query = [
#             (
#                 record.get('_id'),
#                 record.get('mispar_rechev'),
#                 extract_first_word(record.get('tozeret_nm')),
#                 record.get('kinuy_mishari'),
#                 record.get('baalut'),
#                 record.get('degem_nm'),
#                 record.get('ramat_gimur'),
#                 record.get('ramat_eivzur_betihuty'),
#                 record.get('kvutzat_zihum'),
#                 record.get('tozeret_cd'),
#                 record.get('degem_cd'),
#                 record.get('shnat_yitzur'),
#                 record.get('degem_manoa'),
#                 record.get('moed_aliya_lakvish'),
#                 record.get('mivchan_acharon_dt'),
#                 record.get('tokef_dt'),
#                 record.get('tzeva_rechev'),
#                 record.get('sug_delek_nm')
#             )
#             for record in records
#         ]
#
#         cursor.executemany(insert_general_query, values_general_query)
#
#     async def main():
#         cursor, connection = sql.creatingTables()
#         # API endpoint URL - מאגר מספרי רישוי של כלי רכב
#         api_url = "https://data.gov.il/api/3/action/datastore_search?resource_id=053cea08-09bc-40ec-8f7a-156f0677aff3&q"
#
#         async with aiohttp.ClientSession() as session:
#             response = await fetch_data(session, api_url, {'limit': 1000, 'offset': 0})
#             data = response.get('result', {})
#             total_records = data.get('total', 0)
#             print(f"Number of rows to fetch: {total_records:,}")
#
#             limit = 1000  # Specify the limit per request, let's go for 1k per request
#
#             # Calculate the number of requests needed
#             num_requests = (total_records + limit - 1) // limit
#
#             tasks = []
#             for page in range(num_requests):
#                 offset = page * limit
#                 params = {'limit': limit, 'offset': offset}
#                 print(params, page, "/", num_requests)
#                 tasks.append(fetch_data(session, api_url, params))
#
#             # Gather responses from all tasks
#             responses = await asyncio.gather(*tasks)
#
#             # Process and insert data
#             for page_data in responses:
#                 records = page_data.get('result', {}).get('records', [])
#                 await process_data(cursor, records)
#
#             # Commit and close connection
#             connection.commit()
#             cursor.close()
#             connection.close()
#
#         # Measure the end time
#         end_time = time.time()
#         # Calculate and print the total time taken
#         time_seconds = end_time - start_time
#         # Calculate and print the total time taken
#         if time_seconds < 60:
#             print(f"Execution time: {time_seconds:.5f} seconds")
#         else:
#             time_minutes = time_seconds / 60
#             print(f"Execution time: {time_minutes:.2f} minutes")
#
#     if __name__ == "__main__":
#         start_time = time.time()
#         asyncio.run(main())
#
#
# def insert_gen_spec_Data():
#     cursor, connection = sql.creatingTables()
#     # API endpoint URL - מאגר מספרי רישוי של כלי רכב
#     api_url = "https://data.gov.il/api/3/action/datastore_search?resource_id=142afde2-6228-49f9-8a29-9b6c3a0cbe40"
#
#     response = requests.get(api_url)  # Initial request to get the total number of records
#     data = response.json()
#     start_time = time.time()  # Measure the start time
#
#     total_records = data.get('result', {}).get('total', 0)
#     print(f"Number of rows to fetch: {total_records:,}")
#
#     limit = 10000  # Specify the limit per request, let's go for 10k per request
#
#     # Calculate the number of requests needed
#     num_requests = (total_records + limit - 1) // limit
#
#     # Make sequential requests with pagination, divide all the data into pages that contains 10k rows
#     for page in range(num_requests):
#         offset = page * limit
#         params = {'limit': limit, 'offset': {offset}}
#         print(params)
#         response = requests.get(api_url, params=params)
#         # load
#         page_data = response.json()
#         records = page_data.get('result', {}).get('records', [])  # Process the data from the current page (page_data)
#         # Insert data into the MySQL table using parameterized query and batch insert
#
#         insert_general_query = """
#         INSERT INTO cars_general (_id, mispar_rechev, tozeret_nm, kinuy_mishari, baalut, degem_nm, ramat_gimur,
#         ramat_eivzur_betihuty, kvutzat_zihum, tozeret_cd, degem_cd, shnat_yitzur)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         ON DUPLICATE KEY UPDATE
#         mispar_rechev=VALUES(mispar_rechev), tozeret_nm=VALUES(tozeret_nm), kinuy_mishari=VALUES(kinuy_mishari),
#         baalut=VALUES(baalut), degem_nm=VALUES(degem_nm), ramat_gimur=VALUES(ramat_gimur),
#         ramat_eivzur_betihuty=VALUES(ramat_eivzur_betihuty), kvutzat_zihum=VALUES(kvutzat_zihum),
#         tozeret_cd=VALUES(tozeret_cd), degem_cd=VALUES(degem_cd), shnat_yitzur=VALUES(shnat_yitzur);
#         """
#
#         values_general_query = [
#             (record.get('_id'), record.get('mispar_rechev'),
#              extract_first_word(record.get('tozeret_nm')),
#              record.get('kinuy_mishari'),
#              record.get('baalut'),
#              record.get('degem_nm'),
#              record.get('ramat_gimur'),
#              record.get('ramat_eivzur_betihuty'),
#              record.get('kvutzat_zihum'),
#              record.get('tozeret_cd'),
#              record.get('degem_cd'),
#              record.get('shnat_yitzur'))
#             for record in records]
#
#         cursor.executemany(insert_general_query, values_general_query)
#
#         insert_specific_query = """
#         INSERT INTO cars_specific (_id, mispar_rechev, degem_manoa, moed_aliya_lakvish, mivchan_acharon_dt,
#         tokef_dt, tzeva_rechev, sug_delek_nm)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#         ON DUPLICATE KEY UPDATE
#         mispar_rechev=VALUES(mispar_rechev), degem_manoa=VALUES(degem_manoa),
#         moed_aliya_lakvish=VALUES(moed_aliya_lakvish), mivchan_acharon_dt=VALUES(mivchan_acharon_dt),
#         tokef_dt=VALUES(tokef_dt), tzeva_rechev=VALUES(tzeva_rechev), sug_delek_nm=VALUES(sug_delek_nm);
#         """
#         values_specific_query = [(record.get('_id'),
#                                   record.get('mispar_rechev'),
#                                   record.get('degem_manoa'),
#                                   record.get('moed_aliya_lakvish'),
#                                   record.get('mivchan_acharon_dt'),
#                                   record.get('tokef_dt'),
#                                   record.get('tzeva_rechev'),
#                                   record.get('sug_delek_nm'))
#                                  for record in records]
#
#         cursor.executemany(insert_specific_query, values_specific_query)
#         connection.commit()
#     # Measure the end time
#     end_time = time.time()
#     # Calculate and print the total time taken
#     time_seconds = (end_time - start_time)
#     # Calculate and print the total time taken
#     if time_seconds < 60:
#         print(f"Execution time: {time_seconds:.5f} seconds")
#     else:
#         time_minutes = time_seconds / 60
#         print(f"Execution time: {time_minutes:.2f} minutes")
#
#
# def insert_technical_data():
#     cursor, connection = sql.creatingTables()
#     # API endpoint URL - תוצרים ודגמים של כלי רכב WLTP
#     api_url = "https://data.gov.il/api/3/action/datastore_search?resource_id=142afde2-6228-49f9-8a29-9b6c3a0cbe40&"
#     response = requests.get(api_url)  # Initial request to get the total number of records
#     data = response.json()
#     total_records = data.get('result', {}).get('total', 0)
#     print(f"Number of rows to fetch: {total_records:,}")
#     limit = 10000
#     # Calculate the number of requests needed
#     num_requests = (total_records + limit - 1) // limit
#     # Make sequential requests with pagination, divide all the data into pages that contains 10k rows
#     start_time = time.time()  # Measure the start time
#     for page in range(num_requests):
#         offset = page * limit
#         params = {'limit': limit, 'offset': {offset}}
#         response = requests.get(api_url, params=params)
#         print(params)
#         # load
#         page_data = response.json()
#         records = page_data.get('result', {}).get('records', [])  # Process the data from the current page (page_data)
#         # Insert data into the MySQL table using parameterized query and batch insert
#         insert_command = """
#            INSERT INTO cars_technical
#            (tozeret_cd, degem_cd, shnat_yitzur, tozar, nefah_manoa, merkav, mishkal_kolel, hanaa_nm, mazgan_ind, abs_ind,
#            mispar_kariot_avir, hege_koah_ind, automatic_ind, mispar_halonot_hashmal, delek_nm,
#            mispar_dlatot, koah_sus, mispar_moshavim, bakarat_yatzivut_ind)
#            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#            ON DUPLICATE KEY UPDATE
#            tozeret_cd=VALUES(tozeret_cd), degem_cd=VALUES(degem_cd), shnat_yitzur=VALUES(shnat_yitzur),
#            tozar=VALUES(tozar), nefah_manoa=VALUES(nefah_manoa), merkav=VALUES(merkav),
#            mishkal_kolel=VALUES(mishkal_kolel),
#            hanaa_nm=VALUES(hanaa_nm), mazgan_ind=VALUES(mazgan_ind), abs_ind=VALUES(abs_ind),
#            mispar_kariot_avir=VALUES(mispar_kariot_avir), hege_koah_ind=VALUES(hege_koah_ind),
#            automatic_ind=VALUES(automatic_ind), mispar_halonot_hashmal=VALUES(mispar_halonot_hashmal),
#            delek_nm=VALUES(delek_nm), mispar_dlatot=VALUES(mispar_dlatot), koah_sus=VALUES(koah_sus),
#            mispar_moshavim=VALUES(mispar_moshavim), bakarat_yatzivut_ind=VALUES(bakarat_yatzivut_ind);
#            """
#
#         values = [
#             (record.get('tozeret_cd'), record.get('degem_cd'), record.get('shnat_yitzur'),
#              record.get('tozar'),
#              record.get('nefah_manoa'),
#              record.get('merkav'),
#              record.get('mishkal_kolel'),
#              record.get('hanaa_nm'),
#              record.get('mazgan_ind'),
#              record.get('abs_ind'),
#              record.get('mispar_kariot_avir'),
#              record.get('hege_koah_ind'),
#              record.get('automatic_ind'),
#              record.get('mispar_halonot_hashmal'),
#              record.get('delek_nm'),
#              record.get('mispar_dlatot'),
#              record.get('koah_sus'),
#              record.get('mispar_moshavim'),
#              record.get('bakarat_yatzivut_ind'))
#             for record in records]
#
#         cursor.executemany(insert_command, values)
#         connection.commit()
#     # Measure the end time
#     end_time = time.time()
#     # Calculate and print the total time taken
#     time_seconds = (end_time - start_time)
#     # Calculate and print the total time taken
#     if time_seconds < 60:
#         print(f"Execution time: {time_seconds:.5f} seconds")
#     else:
#         time_minutes = time_seconds / 60
#         print(f"Execution time: {time_minutes:.2f} minutes")
#
#     # Close the cursor and connection
#     cursor.close()
#     connection.close()
#
#
# def dropTables():
#     # MySQL connection configuration
#     connection = mysql.connector.connect(
#         host="localhost",
#         user="root",
#         password="123456",
#         database="cars_in_israel"
#
#     )
#     cursor = connection.cursor()  # Create a cursor object to interact with the database
#     drop_specific_table = "DELETE FROM cars_specific WHERE _id > 0;"
#     drop_general_table = "DELETE FROM cars_general WHERE _id > 0;"
#     cursor.execute(drop_specific_table)
#     cursor.execute(drop_general_table)
#     connection.commit()
#     print("tables have been deleted successfully.")
#
#
# def creatingTables():
#     # MySQL connection configuration
#     connection = mysql.connector.connect(
#         host=os.getenv("MYSQL_HOST"),
#         user=os.getenv("MYSQL_USER"),
#         password=os.getenv("MYSQL_PASSWORD"),
#         database=os.getenv("MYSQL_DATABASE")
#     )
#
#     cursor = connection.cursor()  # Create a cursor object to interact with the database
#
#     # Define the table creation queries
#     create_table_general = """
#     CREATE TABLE IF NOT EXISTS cars_general (
#         _id INT PRIMARY KEY,
#         mispar_rechev VARCHAR(255),
#         kinuy_mishari VARCHAR(255),
#         tozeret_nm VARCHAR(255),
#         baalut VARCHAR(255),
#         degem_nm VARCHAR(255),
#         ramat_gimur VARCHAR(255),
#         ramat_eivzur_betihuty VARCHAR(255),
#         kvutzat_zihum VARCHAR(255),
#         shnat_yitzur VARCHAR(255)
#     );
#     """
#
#     # Execute the table creation queries
#     cursor.execute(create_table_general)
#
#     # Commit the changes
#     connection.commit()
#     return cursor, connection
#
#
