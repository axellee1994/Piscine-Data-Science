import psycopg2
import matplotlib.pyplot as plt

from ex01.chart import plot_line_chart

DB_NAME = "piscineds"
DB_USER = "axlee"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "localhost"
DB_PORT = "5432"
SQL_FILE = "chart_1.sql"

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

def fetch_bar_chart_data(connection, sql_file_path):
    """
    Fetch data for line chart from the database
    """
    months = []
    totals = []
    try:
        with open(sql_file_path, "r") as file:
            sql_script = file.read()
        with connection.cursor() as cursor:
            cursor.execute(sql_script)
            results = cursor.fetchall()
            for row in results:
                months.append(row[0])
                totals.append(row[1])
        return months, totals
    except Exception as e:
        print(f"Error fetching data: {e}")
        return months, totals

def plot_bar_chart(months, totals):
    """
    Plot a bar chart using the provided months and totals
    """
    if not months or not totals:
        print("No data available to plot.")
        return
    plt.figure(figsize=(12, 6))
    plt.bar(months, totals, color="blue")
    plt.xlabel('month')
    plt.ylabel('Total Sales (in millions)')
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    db_params = {
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "host": DB_HOST,
        "port": DB_PORT
    }
    connection = connect_to_postgresql(db_params)
    if connection:
        months, totals = fetch_bar_chart_data(connection, SQL_FILE)
        if months and totals:
            plot_bar_chart(months, totals)
        else:
            print("No data retrieved for bar chart.")
        try:
            connection.close()
            print("Database connection closed.")
        except Exception as e:
            print(f"Error closing connection: {e}")