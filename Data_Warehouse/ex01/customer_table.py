import glob
import os
import pandas as pd
import psycopg2
import io
import sys
import uuid

CUSTOMER_FOLDER = "../ex01/customer/"
DB_USER = "axlee"
DB_NAME = "piscineds"
DB_PASSWORD = "mysecretpassword"
DB_PORT = "5432"
DB_HOST = "localhost"


def is_uuid_column(series):
    """Return True if all non-null values in the series are valid UUIDs."""
    for x in series.dropna():
        if not (isinstance(x, str) and is_uuid(x)):
            return False
    return True


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


def load_and_concat_csvs():
    """
    Load and join all multiple CSV files into a single dataframe (optimized).
    """
    all_csv_files = glob.glob(os.path.join(CUSTOMER_FOLDER, "*.csv"))
    if not all_csv_files:
        return None
    # Use generator expression for memory efficiency
    df_iter = (pd.read_csv(f) for f in all_csv_files)
    combined_df = pd.concat(df_iter, ignore_index=True)
    if combined_df.empty or combined_df.shape[1] == 0:
        return None
    return combined_df


def create_and_insert_customers(df):
    """Create 'customers' table and insert data."""
    with psycopg2.connect(
        user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_NAME
    ) as connection:
        with connection.cursor() as cursor:
            create_table(cursor, "customers", df)
            print("Table 'customers' created.")
            insert_data(cursor, "customers", df)
            print("Data inserted into 'customers'.")
        connection.commit()


def process_and_insert_csvs():
    all_csv_files = glob.glob(os.path.join(CUSTOMER_FOLDER, "*.csv"))
    if not all_csv_files:
        print("No CSV files found.")
        return
    # Create table from the first CSV
    first_df = pd.read_csv(all_csv_files[0])
    with psycopg2.connect(
        user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_NAME
    ) as connection:
        with connection.cursor() as cursor:
            create_table(cursor, "customers", first_df)
            print("Table customers created.")
            # Insert first CSV
            insert_data(cursor, "customers", first_df)
            # Insert remaining CSVs
            for csv_file in all_csv_files[1:]:
                df = pd.read_csv(csv_file)
                insert_data(cursor, "customers", df)
            print("All data inserted into customers.")
        connection.commit()


def main():
    """
    Main function that connects to the database,
    processes each CSV file in the specified folder,
    creates corresponding tables, and inserts data into them.
    """
    try:
        process_and_insert_csvs()
        print("Table created and populated correctly")
    except Exception as e:
        print(f"Error loading CSV files: {e}")


if __name__ == "__main__":
    main()
