import psycopg2

DB_NAME = "piscineds"
DB_USER = "axlee"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "localhost"
DB_PORT = "5432"
SQL_FILE = "remove_duplicates.sql"

def connect_to_postgresql(db_params):
    """
    To connect to PostgreSQL database
    """
    connection = None
    try:
        connection = psycopg2.connect(**db_params)
        print("Connection to PostgreSQL DB successful")
        return connection
    except Exception as e:
        print(f"The error '{e}' occurred")
        return None

def execute_sql_file(connection, sql_file_path):
    """
    Execute SQL commands from a file
    """
    if not connection:
        print("No valid database connection.")
        return
    try:
        with connection.cursor() as cursor:
            with open(sql_file_path, 'r') as file:
                sql_script = file.read()
            cursor.execute(sql_script)
            connection.commit()
            print(f"SQL script '{sql_file_path}' executed successfully")
    except Exception as e:
        print(f"Error executing SQL script: {e}")

if __name__ == "__main__":
    db_params = {
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "host": DB_HOST,
        "port": DB_PORT
    }
    connection = connect_to_postgresql(db_params)
    execute_sql_file(connection, SQL_FILE)
    try:
        if connection:
            connection.close()
            print("Database connection closed")
    except Exception as e:
        print(f"Error closing connection: {e}")