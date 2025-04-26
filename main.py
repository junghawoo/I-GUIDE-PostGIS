import psycopg2

# Connection parameters
db_params = {
    "dbname": "gisdb",
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": 5432
}

# Connect to the database
try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Check PostGIS version
    cursor.execute("SELECT postgis_version();")
    version = cursor.fetchone()
    print(f"Connected to PostGIS version: {version[0]}")

    # Close connection
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}")
