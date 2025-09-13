import glob
import os
import pandas as pd
import psycopg2

CUSTOMER_FOLDER = "../ex02/subject/item/"
DB_USER = "axlee"
DB_NAME = "piscineds"
DB_PASSWORD = "mysecretpassword"
DB_PORT = "5432"
DB_HOST = "localhost"


def infer_pg_type(dtype):
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    elif pd.api.types.is_integer_dtype(dtype):
        return 'BIGINT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'NUMERIC'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    else:
        return 'VARCHAR'


def create_table(cursor, table_name, df):
    columns = [f'"{df.columns[0]}" TIMESTAMP']
    for col in df.columns[1:]:
        columns.append(f'"{col}" {infer_pg_type(df[col].dtype)}')
    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
    cursor.execute(f'CREATE TABLE "{table_name}" ({", ".join(columns)});')


def insert_data(cursor, table_name, df):
    tmp_csv = f"/tmp/{table_name}.csv"
    df.to_csv(tmp_csv, index=False, header=False)
    with open(tmp_csv, "r") as f:
        cursor.copy_expert(f'COPY "{table_name}" FROM STDIN WITH CSV', f)
    os.remove(tmp_csv)


def main():
    with psycopg2.connect(
        user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_NAME
    ) as connection:
        with connection.cursor() as cursor:
            for file in glob.glob(os.path.join(CUSTOMER_FOLDER, "*.csv")):
                print("CSV files found:", file)
                table_name = os.path.splitext(os.path.basename(file))[0]
                print(f"Processing {table_name}...")
                df = pd.read_csv(file)
                print(f"Rows to insert: {len(df)}")
                df[df.columns[0]] = pd.to_datetime(
                    df[df.columns[0]], errors='coerce')
                create_table(cursor, table_name, df)
                print(f"Table {table_name} created.")
                insert_data(cursor, table_name, df)
                print(f"Data inserted into {table_name}.")
            connection.commit()
    print("Tables created")


if __name__ == "__main__":
    main()
