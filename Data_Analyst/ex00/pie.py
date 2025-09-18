import psycopg2
import matplotlib.pyplot as plt

DB_NAME = "piscineds"
DB_USER = "axlee"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "localhost"
DB_PORT = "5432"
SQL_FILE = "pie.sql"

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

def fetch_pie_chart_data(connection, sql_file_path):
    """
    Fetch data for pie chart from the database
    """
    labels = []
    values = []
    try:
        with open(sql_file_path, "r") as file:
            sql_script = file.read()
        with connection.cursor() as cursor:
            cursor.execute(sql_script)
            results = cursor.fetchall()
            for row in results:
                labels.append(row[0])
                values.append(row[1])
        return labels, values
    except Exception as e:
        print(f"Error fetching data: {e}")
        return labels, values

def plot_pie_chart(labels, values):
    """
    Plot a pie chart using the provided labels and values
    """
    if not labels or not values:
        print("No data available to plot.")
        return
    plt.figure(figsize=(8, 8))
    plt.pie(values, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title('Pie Chart from Database Data')
    plt.axis('equal')
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
        labels, values = fetch_pie_chart_data(connection, SQL_FILE)
        if labels and values:
            plot_pie_chart(labels, values)
        else:
            print("No data retrieved for pie chart.")
        try:
            connection.close()
            print("Database connection closed.")
        except Exception as e:
            print(f"Error closing connection: {e}")