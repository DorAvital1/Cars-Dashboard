import time
import pymysql
import requests
import os
from dotenv import load_dotenv


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
    cursor, connection = create_Connection_Cursor()
    api_url = "https://data.gov.il/api/3/action/datastore_search?resource_id=053cea08-09bc-40ec-8f7a-156f0677aff3&q"

    try:
        print("Start transaction")
        # Start transaction
        connection.begin()
        print("Deleting all rows")
        # Delete existing data from the table
        cursor.execute("DELETE FROM cars_general;")

        # Initial request to get the total number of records
        response = requests.get(api_url)
        data = response.json()
        total_records = data.get('result', {}).get('total', 0)
        print(f"Number of rows to fetch: {total_records:,}")

        limit = 50000  # Specify the limit per request
        num_requests = (total_records + limit - 1) // limit

        start_time = time.time()  # Measure the start time

        for page in range(num_requests):
            offset = page * limit
            params = {'limit': limit, 'offset': offset}
            print(params, "page:", page + 1, "/", num_requests)
            response = requests.get(api_url, params=params)
            page_data = response.json()
            records = page_data.get('result', {}).get('records', [])

            insert_general_query = """
                INSERT INTO cars_general (
                    _id, mispar_rechev, tozeret_nm, kinuy_mishari, baalut, degem_nm, ramat_gimur,
                    ramat_eivzur_betihuty, kvutzat_zihum, tozeret_cd, degem_cd, shnat_yitzur,
                    degem_manoa, moed_aliya_lakvish, mivchan_acharon_dt, tokef_dt, tzeva_rechev, sug_delek_nm
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

            values_general_query = [
                (
                    record.get('_id'),
                    record.get('mispar_rechev'),
                    record.get('tozeret_nm'),
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
                    record.get('sug_delek_nm')
                )
                for record in records
            ]

            cursor.executemany(insert_general_query, values_general_query)
            connection.commit()

        # Commit the transaction
        connection.commit()

    except pymysql.MySQLError as err:
        # Rollback the transaction in case of an error
        print(f"Error: {err}")
        connection.rollback()

    finally:
        # Measure the end time
        end_time = time.time()
        time_seconds = (end_time - start_time)
        print("Finished extracting general data.")
        if time_seconds < 60:
            print(f"Execution time: {time_seconds:.5f} seconds")
        else:
            time_minutes = time_seconds / 60
            print(f"Execution time: {time_minutes:.2f} minutes")

        # Close the cursor and connection
        cursor.close()
        connection.close()


def insert_technical_data():
    cursor, connection = create_Connection_Cursor()
    api_url = "https://data.gov.il/api/3/action/datastore_search?resource_id=142afde2-6228-49f9-8a29-9b6c3a0cbe40&"

    try:
        # Start transaction
        connection.begin()

        # Delete existing data from the table
        cursor.execute("DELETE FROM cars_technical;")

        # Initial request to get the total number of records
        response = requests.get(api_url)
        data = response.json()
        total_records = data.get('result', {}).get('total', 0)
        print(f"Number of rows to fetch: {total_records:,}")

        limit = 30000
        num_requests = (total_records + limit - 1) // limit

        start_time = time.time()  # Measure the start time

        for page in range(num_requests):
            offset = page * limit
            params = {'limit': limit, 'offset': offset}
            response = requests.get(api_url, params=params)
            print(params)

            page_data = response.json()
            records = page_data.get('result', {}).get('records', [])

            insert_command = """
               INSERT INTO cars_technical
               (tozeret_cd, degem_cd, shnat_yitzur, tozar, nefah_manoa, merkav, mishkal_kolel, hanaa_nm, mazgan_ind, abs_ind,
               mispar_kariot_avir, hege_koah_ind, automatic_ind, mispar_halonot_hashmal, delek_nm,
               mispar_dlatot, koah_sus, mispar_moshavim, bakarat_yatzivut_ind)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               """

            values = [
                (
                    record.get('tozeret_cd'),
                    record.get('degem_cd'),
                    record.get('shnat_yitzur'),
                    record.get('tozar'),
                    record.get('nefah_manoa'),
                    record.get('merkav'),
                    record.get('mishkal_kolel'),
                    record.get('hanaa_nm'),
                    record.get('mazgan_ind'),
                    record.get('abs_ind'),
                    record.get('mispar_kariot_avir'),
                    record.get('hege_koah_ind'),
                    record.get('automatic_ind'),
                    record.get('mispar_halonot_hashmal'),
                    record.get('delek_nm'),
                    record.get('mispar_dlatot'),
                    record.get('koah_sus'),
                    record.get('mispar_moshavim'),
                    record.get('bakarat_yatzivut_ind')
                )
                for record in records
            ]

            cursor.executemany(insert_command, values)
            connection.commit()

        end_time = time.time()
        time_seconds = (end_time - start_time)
        print("Finished extracting technical data.")
        if time_seconds < 60:
            print(f"Execution time: {time_seconds:.5f} seconds")
        else:
            time_minutes = time_seconds / 60
            print(f"Execution time: {time_minutes:.2f} minutes")

    except pymysql.MySQLError as err:
        print(f"Error: {err}")
        connection.rollback()

    finally:
        cursor.close()
        connection.close()


def add_tozar_to_gen():
    cursor, connection = create_Connection_Cursor()
    start_time = time.time()  # Measure the start time

    batch_size = 1000000  # Number of rows to update per batch

    try:
        # Get the total number of rows
        cursor.execute("SELECT COUNT(*) FROM cars_general")
        total_rows = cursor.fetchone()[0]
    except pymysql.MySQLError as e:
        print(f"Error getting total row count: {e}")
        cursor.close()
        connection.close()
        return

    num_batches = (total_rows + batch_size - 1) // batch_size  # Calculate the number of batches without reminders

    try:
        connection.autocommit(False)  # Disable autocommit to manage transactions manually
        # divide the data into chunks to prevent binary logging from remote server supplier...
        for batch_index in range(num_batches):
            offset = batch_index * batch_size
            start_id = offset + 1
            end_id = min(offset + batch_size, total_rows)

            try:
                sql = """
                    UPDATE cars_general cg
                    JOIN cars_technical ct
                    ON cg.tozeret_cd = ct.tozeret_cd
                    AND cg.degem_cd = ct.degem_cd
                    AND cg.shnat_yitzur = ct.shnat_yitzur
                    AND cg._id BETWEEN %s AND %s
                    SET cg.tozar = ct.tozar;
                """
                # Execute the SQL query with parameters
                cursor.execute(sql, (start_id, end_id))
                connection.commit()

                rows_updated = cursor.rowcount
                print(f"Updated {rows_updated} rows with range {start_id} to {end_id}")

            except pymysql.MySQLError as e:
                print(f"Error during update: {e}")
                connection.rollback()
                break

        connection.autocommit(True)  # Re-enable autocommit

    except pymysql.MySQLError as e:
        print(f"Error managing transactions: {e}")
    finally:
        # Measure the end time
        end_time = time.time()
        time_seconds = (end_time - start_time)
        print("Finish transferring tozar_nm column.")
        if time_seconds < 60:
            print(f"Execution time: {time_seconds:.5f} seconds")
        else:
            time_minutes = time_seconds / 60
            print(f"Execution time: {time_minutes:.2f} minutes")

        # Close the cursor and connection
        cursor.close()
        connection.close()


def update_aggregated_tables():
    cursor, connection = create_Connection_Cursor()
    start_time = time.time()  # Measure the start time

    try:
        # Disable autocommit to manage transactions manually
        connection.autocommit(False)

        # Aggregate table: aggregate_table_homestats
        cursor.execute("DELETE FROM aggregate_table_homestats;")
        update_agg_homstats = """
            INSERT INTO aggregate_table_homestats (Count, makers, average_year, min_year, max_year)
            SELECT
                COUNT(*) AS Count,
                COUNT(DISTINCT tozar) AS makers,
                AVG(shnat_yitzur) AS average_year,
                MIN(shnat_yitzur) AS min_year,
                MAX(shnat_yitzur) AS max_year
            FROM cars_general;
        """
        cursor.execute(update_agg_homstats)
        connection.commit()
        print("Successfully updated aggregate_table_homestats.")

        # Aggregate table: aggregated_table_time_series
        cursor.execute("DELETE FROM aggregated_table_time_series;")
        update_agg_time_series = """
            INSERT INTO aggregated_table_time_series (Year, Count)
            SELECT
                CAST(LEFT(moed_aliya_lakvish, 4) AS UNSIGNED) AS Year,
                COUNT(*) AS Count
            FROM cars_general
            WHERE moed_aliya_lakvish > '2007-01-01'
            GROUP BY Year;
        """
        cursor.execute(update_agg_time_series)
        connection.commit()
        print("Successfully updated aggregated_table_time_series.")

        # Aggregate table: aggregated_table_top5manufacturers
        cursor.execute("DELETE FROM aggregated_table_top5manufacturers;")
        update_agg_top5manufacturers = """
            INSERT INTO aggregated_table_top5manufacturers (Manufacturer, count)
            SELECT
                tozar AS Manufacturer,
                COUNT(*) AS count
            FROM cars_general
            GROUP BY Manufacturer
            ORDER BY count DESC
            LIMIT 5;
        """
        cursor.execute(update_agg_top5manufacturers)
        connection.commit()
        print("Successfully updated aggregated_table_top5manufacturers.")

        # Aggregate table: aggregated_table_top5cars
        cursor.execute("DELETE FROM aggregated_table_top5cars;")
        update_agg_top5cars = """
            INSERT INTO aggregated_table_top5cars (Model, Count)
            SELECT CONCAT(kinuy_mishari, ' ', tozar) AS Model,
                COUNT(kinuy_mishari) AS Count
            FROM cars_general
            GROUP BY CONCAT(kinuy_mishari, ' ', tozar)
            ORDER BY Count DESC
            LIMIT 5;
        """
        cursor.execute(update_agg_top5cars)
        connection.commit()
        print("Successfully updated aggregated_table_top5cars.")

        # Aggregate table: aggregated_table_passtest
        cursor.execute("DELETE FROM aggregated_table_passtest;")
        update_agg_pass_test = """
            INSERT INTO aggregated_table_passtest (pass_test, do_not_pass)
            SELECT
                COUNT(CASE WHEN tokef_dt > CURDATE() THEN 1 END) AS pass_test,
                COUNT(CASE WHEN tokef_dt <= CURDATE() THEN 1 END) AS do_not_pass
            FROM cars_general;
        """
        cursor.execute(update_agg_pass_test)
        connection.commit()
        print("Successfully updated aggregated_table_passtest.")

        # Aggregate table: aggregated_table_ownership
        cursor.execute("DELETE FROM aggregated_table_ownership;")
        update_agg_ownership = """
            INSERT INTO aggregated_table_ownership (baalut, count)
            SELECT
                baalut,
                COUNT(baalut) AS count
            FROM cars_general
            GROUP BY baalut
            ORDER BY count DESC;
        """
        cursor.execute(update_agg_ownership)
        connection.commit()
        print("Successfully updated aggregated_table_ownership.")

    except pymysql.MySQLError as e:
        print(f"Error during aggregation: {e}")
        connection.rollback()  # Roll back the transaction if an error occurs

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()
        print("Finish updating all tables.")
        # Measure the end time
        end_time = time.time()
        time_seconds = (end_time - start_time)
        if time_seconds < 60:
            print(f"Execution time: {time_seconds:.5f} seconds")
        else:
            time_minutes = time_seconds / 60
            print(f"Execution time: {time_minutes:.2f} minutes")
        print("Finish ETL process")


