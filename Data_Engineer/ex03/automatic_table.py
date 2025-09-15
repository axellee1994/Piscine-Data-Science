import glob
import os
import pandas as pd
import psycopg2
import io
import sys
import uuid

CUSTOMER_FOLDER = "../ex03/customer/"
DB_USER = "axlee"
DB_NAME = "piscineds"
DB_PASSWORD = "mysecretpassword"
DB_PORT = "5432"
DB_HOST = "localhost"

def is_uuid_column(series):
    """Return True if all non-null values in the series are valid UUIDs."""
    return series.dropna().apply(lambda x: isinstance(x, str) and is_uuid(x)).all()

def is_uuid(val):
    """
    Check if a value is a valid UUID.
    """
    try:
        uuid.UUID(str(val))
        return True
    except Exception:
        return False

def infer_pg_type(dtype, series=None):
    """
    This functions translates PostgreSQL data types
    to pandas data types. UUID is handled separately
    as in pandas it is treated as an object
    (string) type
    """
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    elif pd.api.types.is_integer_dtype(dtype):
        if series is not None and series.max() < 9999999:
            return 'INTEGER'
        else:
            return 'BIGINT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'NUMERIC'
    elif series is not None and is_uuid_column(series):
        return 'UUID'
    else:
        return 'VARCHAR'


def create_table(cursor, table_name, df):
    """
    This creates a table in PostgreSQL based on the
    dataframe scheme that is passed to it
    """
    columns = [f'"{df.columns[0]}" TIMESTAMP']
    for col in df.columns[1:]:
        columns.append(f'"{col}" {infer_pg_type(df[col].dtype, df[col])}')
    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
    cursor.execute(f'CREATE TABLE "{table_name}" ({", ".join(columns)});')


def insert_data(cursor, table_name, df):
    """
    This function inserts the data from the dataframe
    into the PostgreSQL table using COPY

    StringIO helps to avoid writing to disk
    """
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cursor.copy_expert(f'COPY "{table_name}" FROM STDIN WITH CSV', buffer)
    buffer.close()


def main():
    """
    Main function that connects to the database,
    processes each CSV file in the specified folder,
    creates corresponding tables, and inserts data into them.
    """
    try:
        with psycopg2.connect(
            user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_NAME
        ) as connection:
            with connection.cursor() as cursor:
                for file in glob.glob(os.path.join(CUSTOMER_FOLDER, "*.csv")):
                    print("CSV files found:", file)
                    table_name = os.path.splitext(os.path.basename(file))[0]
                    print(f"Processing {table_name}...")
                    df = pd.read_csv(file)
                    if df.empty or df.shape[1] == 0:
                        print(f"Error: {file} is empty or has no columns. Skipping it")
                        continue
                    print(f"Rows to insert: {len(df)}")
                    df[df.columns[0]] = pd.to_datetime(
                        df[df.columns[0]], errors='coerce')
                    create_table(cursor, table_name, df)
                    print(f"Table {table_name} created.")
                    insert_data(cursor, table_name, df)
                    print(f"Data inserted into {table_name}.")
                connection.commit()
        print("Tables created")
    except Exception as e:
        print(f"Error during database operation: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
