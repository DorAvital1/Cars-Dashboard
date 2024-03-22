try:
    import time
    import pymysql
    import requests
    import streamlit as st
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.email_operator import EmailOperator
    from datetime import datetime
    from airflow.utils.email import send_email_smtp

except Exception as e:
    print("Error  {} ".format(e))



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
    # after considering, I decide that instead of updating existing rows,
    # I'll truncate the table and re-insert instead of using UPDATE clause for better performance.
    cursor.execute("TRUNCATE cars_general;")
    # API endpoint URL
    api_url = "https://data.gov.il/api/3/action/datastore_search?resource_id=053cea08-09bc-40ec-8f7a-156f0677aff3&q"
    response = requests.get(api_url)  # Initial request to get the total number of records
    data = response.json()
    start_time = time.time()  # Measure the start time
    total_records = data.get('result', {}).get('total', 0)
    print(f"Number of rows to fetch: {total_records:,}")
    limit = 50000  # Specify the limit per request, let's go for 10k per request
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
        records = page_data.get('result', {}).get('records',
                                                  [])  # Process the data from the current page (page_data)
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
            record.get('sug_delek_nm'))
            for record in records]

        cursor.executemany(insert_general_query, values_general_query)
        connection.commit()
    # Measure the end time
    end_time = time.time()
    # Calculate and print the total time taken
    time_seconds = (end_time - start_time)
    print("finish extract general data.")
    # Calculate and print the total time taken
    if time_seconds < 60:
        print(f"Execution time: {time_seconds:.5f} seconds")
    else:
        time_minutes = time_seconds / 60
        print(f"Execution time: {time_minutes:.2f} minutes")


def insert_technical_data():
    cursor, connection = create_Connection_Cursor()
    # Again, I'll truncate the table and re-insert instead of using UPDATE clause for better performance.
    cursor.execute("TRUNCATE cars_technical;")
    # API endpoint URL - תוצרים ודגמים של כלי רכב WLTP
    api_url = "https://data.gov.il/api/3/action/datastore_search?resource_id=142afde2-6228-49f9-8a29-9b6c3a0cbe40&"
    response = requests.get(api_url)  # Initial request to get the total number of records
    data = response.json()
    total_records = data.get('result', {}).get('total', 0)
    print(f"Number of rows to fetch: {total_records:,}")
    limit = 10000
    # Calculate the number of requests needed
    num_requests = (total_records + limit - 1) // limit
    # Make sequential requests with pagination, divide all the data into pages that contains 10k rows
    start_time = time.time()  # Measure the start time
    for page in range(num_requests):
        offset = page * limit
        params = {'limit': limit, 'offset': offset}
        response = requests.get(api_url, params=params)
        print(params)
        # load
        page_data = response.json()
        records = page_data.get('result', {}).get('records',
                                                  [])  # Process the data from the current page (page_data)
        # Insert data into the MySQL table using parameterized query and batch insert
        insert_command = """
           INSERT INTO cars_technical
           (tozeret_cd, degem_cd, shnat_yitzur, tozar, nefah_manoa, merkav, mishkal_kolel, hanaa_nm, mazgan_ind, abs_ind,
           mispar_kariot_avir, hege_koah_ind, automatic_ind, mispar_halonot_hashmal, delek_nm,
           mispar_dlatot, koah_sus, mispar_moshavim, bakarat_yatzivut_ind)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON DUPLICATE KEY UPDATE
           tozeret_cd=VALUES(tozeret_cd), degem_cd=VALUES(degem_cd), shnat_yitzur=VALUES(shnat_yitzur),
           tozar=VALUES(tozar), nefah_manoa=VALUES(nefah_manoa), merkav=VALUES(merkav),
           mishkal_kolel=VALUES(mishkal_kolel),
           hanaa_nm=VALUES(hanaa_nm), mazgan_ind=VALUES(mazgan_ind), abs_ind=VALUES(abs_ind),
           mispar_kariot_avir=VALUES(mispar_kariot_avir), hege_koah_ind=VALUES(hege_koah_ind),
           automatic_ind=VALUES(automatic_ind), mispar_halonot_hashmal=VALUES(mispar_halonot_hashmal),
           delek_nm=VALUES(delek_nm), mispar_dlatot=VALUES(mispar_dlatot), koah_sus=VALUES(koah_sus),
           mispar_moshavim=VALUES(mispar_moshavim), bakarat_yatzivut_ind=VALUES(bakarat_yatzivut_ind);
           """

        values = [
            # the PK of every model, will help us to switch 'tozar' values for every car in cars_general with joins
            (record.get('tozeret_cd'), record.get('degem_cd'), record.get('shnat_yitzur'),
             # rest of the attributes needed
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
             record.get('bakarat_yatzivut_ind'))
            for record in records]

        cursor.executemany(insert_command, values)
        connection.commit()
    # Measure the end time
    end_time = time.time()
    # Calculate and print the total time taken
    time_seconds = (end_time - start_time)
    print("finish extract technical data.")
    # Calculate and print the total time taken
    if time_seconds < 60:
        print(f"Execution time: {time_seconds:.5f} seconds")
    else:
        time_minutes = time_seconds / 60
        print(f"Execution time: {time_minutes:.2f} minutes")

    # Close the cursor and connection
    cursor.close()
    connection.close()


def add_tozar_to_gen():
    cursor, connection = create_Connection_Cursor()
    start_time = time.time()  # Measure the start time
    # Define the SQL query to update the column in destination_table.
    # 'tozar' in cars_technical is much more accurate than 'tozar_nm" in cars_general,
    # So I decide to map fields using inner joins with the PK of cars_technical to fit
    # the FK of cars_general - record.get('tozeret_cd'), record.get('degem_cd'), record.get('shnat_yitzur')
    sql = """
        UPDATE cars_general
        JOIN cars_technical 
        ON cars_general.tozeret_cd = cars_technical.tozeret_cd
        AND cars_general.degem_cd = cars_technical.degem_cd
        AND cars_general.shnat_yitzur = cars_technical.shnat_yitzur
        SET cars_general.tozar = cars_technical.tozar;
          """
    # Execute the SQL query
    cursor.execute(sql)
    connection.commit()
    cursor.close()
    connection.close()
    # Measure the end time
    end_time = time.time()
    # Calculate and print the total time taken
    time_seconds = (end_time - start_time)
    print("finish extract technical data.")
    # Calculate and print the total time taken
    if time_seconds < 60:
        print(f"Execution time: {time_seconds:.5f} seconds")
    else:
        time_minutes = time_seconds / 60
        print(f"Execution time: {time_minutes:.2f} minutes")
    print("finish transfer tozar column.")


def update_aggregated_tables():
    cursor, connection = create_Connection_Cursor()
    start_time = time.time()  # Measure the start time
    # Because we are processing a lot of data and the loading will take some time,
    # I created "materialized tables" to view the data as quickly as possible when a user opens the Dashboard home page.
    # In addition, truncate the tables and re-insert instead
    # of using UPDATE clause for better performance for all of aggregated_tables.
    cursor.execute("TRUNCATE aggregate_table_homestats;")
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

    cursor.execute("TRUNCATE aggregated_table_time_series;")
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

    cursor.execute("TRUNCATE aggregated_table_top5manufacturers;")
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

    cursor.execute("TRUNCATE aggregated_table_top5cars;")
    update_agg_top5cars = """
        INSERT INTO aggregated_table_top5cars (Model, Count)
        SELECT CONCAT(kinuy_mishari, ' ', tozar) as Model,
        COUNT(kinuy_mishari) AS Count
        FROM cars_general
        GROUP BY CONCAT(kinuy_mishari, ' ', tozar)
        ORDER BY Count DESC
        LIMIT 5;
        """
    cursor.execute(update_agg_top5cars)
    connection.commit()

    cursor.execute("TRUNCATE aggregated_table_passtest;")
    update_agg_pass_test = """
        INSERT INTO aggregated_table_passtest (pass_test, do_not_pass)
        SELECT
        COUNT(CASE WHEN tokef_dt > CURDATE() THEN 1 END) as pass_test,
        COUNT(CASE WHEN tokef_dt <= CURDATE() THEN 1 END) as do_not_pass
        FROM cars_general;
        """
    cursor.execute(update_agg_pass_test)
    connection.commit()

    cursor.execute("TRUNCATE aggregated_table_ownership;")
    update_agg_ownership = """
        INSERT INTO aggregated_table_ownership (baalut, count)
        SELECT
        baalut,
        COUNT(baalut) as count
        FROM cars_general
        GROUP BY baalut
        ORDER BY count DESC;
        """
    cursor.execute(update_agg_ownership)
    connection.commit()
    cursor.close()
    connection.close()
    print("finish updating all tables.")
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


def etl_function1():
    insert_gen_data_sync_v()
    print("Complete insert general data into MySql.")


def etl_function2():
    insert_technical_data()
    print("Complete insert technical data into MySql.")


def etl_function3():
    add_tozar_to_gen()
    print("Complete mapping all 'tozar' column values based on PK-FK into MySql.")


def etl_function4():
    update_aggregated_tables()
    print("Complete all aggregate tables into MySql.\n"
          "ETL is complete.")


def task_success_callback(context):
    # Define the recipient email address
    email = 'doravital4@gmail.com'
    # Extract DAG ID and Task ID from the context and format the subject
    dag_id = context['task_instance_key_str'].split('__')[0]
    task_id = context['task_instance_key_str'].split('__')[1]
    subject = f"[Airflow] DAG {dag_id} - Task {task_id}: Success"

    # Create the HTML content for the email body
    html_content = """
    DAG: {0}<br>
    Task: {1}<br>
    Succeeded on: {2}
    """.format(
        dag_id,
        task_id,
        datetime.now()  # Include the current datetime
    )
    # Send the email using SMTP
    send_email_smtp(email, subject, html_content)


dag = DAG(
    dag_id="ETL_dag",
    schedule_interval="@monthly",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2023, 2, 24),
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        'on_success_callback': task_success_callback,
        'email': 'doravital4@gmail.com',
        'email_on_failure': True,
        'email_on_retry': False,
    },
    catchup=False
)

etl_task1 = PythonOperator(
    task_id='etl_function1',
    python_callable=etl_function1,
    dag=dag
)

etl_task2 = PythonOperator(
    task_id='etl_function2',
    python_callable=etl_function2,
    dag=dag
)

etl_task3 = PythonOperator(
    task_id='etl_function3',
    python_callable=etl_function3,
    dag=dag
)

etl_task4 = PythonOperator(
    task_id='etl_function4',
    python_callable=etl_function4,
    dag=dag
)

email_success = EmailOperator(
    task_id='email_on_success',
    to='doravital4@gmail.com',
    subject='Airflow DAG Succeeded',
    html_content=task_success_callback,
    dag=dag,
    trigger_rule='all_success'  # Only run if all upstream tasks succeed
)

etl_task1 >> etl_task2 >> etl_task3 >> etl_task4 >> email_success
