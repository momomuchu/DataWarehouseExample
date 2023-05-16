import psycopg2
import datetime

# Function to extract data from the source table
def extract_data(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, name, dob, city, country FROM users WHERE active = true")
    rows = cursor.fetchall()
    cursor.close()
    return rows

# Function to transform data
def transform_data(rows):
    transformed_rows = []
    for row in rows:
        user_id = row[0]
        name = row[1]
        dob = datetime.datetime.strptime(row[2], "%Y-%m-%d")
        age = (datetime.datetime.now() - dob).days // 365
        location = f"{row[3]}, {row[4]}"
        transformed_row = {'user_id': user_id, 'name': name, 'age': age, 'location': location}
        transformed_rows.append(transformed_row)
    return transformed_rows

# Function to load data into Redshift
def load_into_redshift(conn, transformed_rows):
    cursor = conn.cursor()
    for row in transformed_rows:
        insert_query = "INSERT INTO redshift_users (user_id, name, age, location) VALUES (%s, %s, %s, %s)"
        cursor.execute(insert_query, (row['user_id'], row['name'], row['age'], row['location']))
    conn.commit()
    cursor.close()

# Function to perform the ETL process
def perform_etl(conn):
    extracted_data = extract_data(conn)
    transformed_data = transform_data(extracted_data)
    load_into_redshift(conn, transformed_data)

# Function to execute optimized queries
def execute_optimized_queries(conn):
    cursor = conn.cursor()

    # Example: Count of active users from each location
    cursor.execute("SELECT location, COUNT(*) FROM redshift_users WHERE active = true GROUP BY location")
    result = cursor.fetchall()
    print("Number of active users from each location:", result)

    # Example: Average age of active users from each location
    cursor.execute("SELECT location, AVG(age) FROM redshift_users WHERE active = true GROUP BY location")
    result = cursor.fetchall()
    print("Average age of active users from each location:", result)

    cursor.close()

# Function to perform the entire ETL process and execute optimized queries
def perform_etl_and_execute_queries():
    try:
        # Connect to the database
        conn = psycopg2.connect(
            database="your_database",
            user="your_username",
            password="your_password",
            host="your_host",
            port="your_port"
        )

        # Perform ETL process
        perform_etl(conn)

        # Execute optimized queries
        execute_optimized_queries(conn)

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        # Close the database connection
        if conn:
            conn.close()
            print("PostgreSQL connection is closed")

# Call the function to perform ETL and execute queries
perform_etl_and_execute_queries()
