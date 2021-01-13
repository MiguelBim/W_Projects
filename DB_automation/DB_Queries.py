from dotenv import load_dotenv
import mysql.connector
import pandas as pd
import warnings
import os.path
import os

pd.set_option('display.max_rows', 700)
pd.set_option('display.max_columns', 700)
pd.set_option('display.width', 1000)

load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_1A_HOST = os.getenv("DB_1A_HOST")
DB_2A_HOST = os.getenv("DB_2A_HOST")
DB_3A_HOST = os.getenv("DB_3A_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_SCHEMA = "INFORMATION_SCHEMA"
warnings.filterwarnings("ignore")


def run_query(query, column_names=[]):

    print("\nquery to be executed: \n\t{}".format(query))
    db_connection = mysql.connector.connect(host=DB_2A_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_SCHEMA)

    db_cursor = db_connection.cursor()
    db_cursor.execute(query)
    query_result = db_cursor.fetchall()
    if column_names:
        df = pd.DataFrame(query_result, columns=column_names)
    else:
        df = pd.DataFrame(query_result)

    return df


def extract_records(tables_df):

    number_records_df = pd.DataFrame()

    for index, row in tables_df.iterrows():
        query = "SELECT COUNT(DISTINCT(" + row['COLUMN'] + ")) FROM " + row['SCHEMA'] + "." + row['TABLE'] + ";"
        table_counts = run_query(query)
        print("Number of records: {} ".format(table_counts))



    print(number_records_df)


def main():
    query = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME " \
            "FROM INFORMATION_SCHEMA.COLUMNS " \
            "WHERE LOWER(COLUMN_NAME) like '%prospect_finder_number%'" \
            "ORDER BY TABLE_NAME;"
    columns = ['SCHEMA', 'TABLE', 'COLUMN']
    results = run_query(query, columns)

    extract_records(results)


if __name__ == '__main__':
    main()
