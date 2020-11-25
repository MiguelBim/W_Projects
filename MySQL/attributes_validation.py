from dotenv import load_dotenv
import pandas as pd
import warnings
import os
import mysql.connector

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


def query_db(query, column_names):

    db_connection = mysql.connector.connect(host=DB_2A_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_SCHEMA)

    db_cursor = db_connection.cursor()
    db_cursor.execute(query)
    query_result = db_cursor.fetchall()
    # for x in query_result:
    #     print(x)

    df = pd.DataFrame(query_result, columns=column_names)
    return df


def main():

    query = "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where UPPER(COLUMN_NAME) like '%GOV%' order by TABLE_NAME"
    column_names = ['TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME']
    q_results_df = query_db(query, column_names)
    q_results_df.to_csv('results.csv', header=True)


if __name__ == '__main__':
    main()