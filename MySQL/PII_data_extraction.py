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

    db_connection = mysql.connector.connect(host=DB_1A_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_SCHEMA)

    db_cursor = db_connection.cursor()
    db_cursor.execute(query)
    query_result = db_cursor.fetchall()
    if column_names:
        df = pd.DataFrame(query_result, columns=column_names)
    else:
        df = pd.DataFrame(query_result)

    return df


def filter_by_states(df, states):

    try:
        # ---- USE FOR CLAUSE -> IN ----
        # filtered_df = df.loc[df['STATE'].isin(states)]
        # ---- USE FOR CLAUSE -> NOT IN ----
        filtered_df = df.loc[~df['STATE'].isin(states)]

        filtered_df.dropna(subset=['SSN'], inplace=True)
        filtered_df.drop_duplicates(subset='STATE')
    except Exception as e:
        print('There was an issue when filtering for those values. Please check')
        print(e)

    return filtered_df

def main():

    query = "select * from INFORMATION_SCHEMA.COLUMNS where UPPER(TABLE_NAME) like 'PROGRESO_FINANCIERO' order by TABLE_NAME"
    query_2 = "SELECT `Social Security Number`, STATE FROM PFE.progreso_financiero"
    column_names = ['SSN', 'STATE']
    q_results_df = query_db(query_2, column_names)
    states = ['California']
    results_filtered = filter_by_states(q_results_df, states)
    results_filtered.to_csv('filtered_results_Others.csv', header=True)


if __name__ == '__main__':
    main()