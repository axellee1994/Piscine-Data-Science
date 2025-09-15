import psycopg2

CUSTOMER_FOLDER = "../ex01/customer/"
DB_USER = "axlee"
DB_NAME = "piscineds"
DB_PASSWORD = "mysecretpassword"
DB_PORT = "5432"
DB_HOST = "localhost"


def find_duplicates_in_customers():
    """
    Detect duplicate entries in the 'customers' table by email.
    Returns a list of duplicate emails.
    """
    with psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME) as connection:
        with connection.cursor() as cursor:
            query = """
            SELECT *, COUNT(*) as count
            FROM customers
            GROUP BY event_time, event_type, product_id, price, user_id, user_session
            HAVING COUNT(*) > 1;
            """
            cursor.execute(query)
            duplicates = cursor.fetchall()
            if duplicates:
                print("Duplicate emails found:")
                for row in duplicates:
                    print(row)
            else:
                print("No duplicate emails found.")
            return duplicates


def drop_duplicates_in_customers():
    """
    Remove duplicate entries from the 'customers' table based on all columns using a temporary table.
    """
    with psycopg2.connect(
        user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_NAME
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_customers AS
                SELECT *
                FROM (
                    SELECT *,
                        event_time - LAG(event_time) OVER (
                            PARTITION BY event_type, product_id, price, user_id, user_session
                            ORDER BY event_time
                        ) AS time_diff
                    FROM customers
                ) sub
                WHERE time_diff IS NULL OR EXTRACT(EPOCH FROM time_diff) > 1;
            """)
            cursor.execute("TRUNCATE customers;")
            cursor.execute("INSERT INTO customers SELECT * FROM temp_customers;")
            print("Duplicates removed from 'customers' table using temporary table.")
        connection.commit()


def main():
    try:
        find_duplicates_in_customers()
        drop_duplicates_in_customers()
        print("Duplicates removed successfully.")
    except Exception as e:
        print(f"Error removing duplicates: {e}")

if __name__ == "__main__":
    main()