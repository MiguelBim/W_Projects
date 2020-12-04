from dotenv import load_dotenv
import mysql.connector
import pandas as pd
import warnings
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


def extract_possible_fields(attributes_df):

    types_list = []
    columns_needed = ['SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'EXAMPLE', 'ENCRYPTED']
    correct_selection = True

    groups = attributes_df.groupby(['TYPE'])
    for group_name, df_group in groups:
        types_list.append(group_name)
        print("\t-> "+group_name)

    while correct_selection:
        attribute_election = input("\nAttribute selection: ").strip().lower()
        if attribute_election in types_list:
            correct_selection = False
        else:
            print("\t--> Incorrect attribute selection. Please check and retry")

    df_subset = attributes_df.loc[(attributes_df['TYPE'] == attribute_election) & (attributes_df['ENCRYPTED'] == 'No'), columns_needed]
    print("\nThese are the fields found for selected criteria: \n")
    print(df_subset)
    print("\n"+"-"*110)

    return df_subset, attribute_election


def delete_columns_from_df(df):

    removal_flag = True
    col_schema_list = df['SCHEMA_NAME'].tolist()
    col_table_list = df['TABLE_NAME'].tolist()
    col_name_list = df['COLUMN_NAME'].tolist()

    while removal_flag:
        schema_to_delete_from = input("Schema name: ").strip()
        if schema_to_delete_from in col_schema_list:
            table_to_delete_from = input("Table name: ").strip()
            if table_to_delete_from in col_table_list:
                column_to_be_deleted = input("Column name: ").strip()
                if column_to_be_deleted in col_name_list:
                    try:
                        df.drop(df[df.COLUMN_NAME == column_to_be_deleted].index, inplace=True)
                        print("\t--> Column {} deleted".format(column_to_be_deleted.upper()))
                        remove_ans = input("\nWould you like to remove another(yes/no): ").strip().lower()
                        if remove_ans == 'no':
                            removal_flag = False
                            print("-"*110+"\nThe fields to be found are: \n")
                            print(df)
                            print("\n"+"-"*110)
                    except Exception as e:
                        print("It was not possible to delete the column, please check error:")
                        print(e)
                else:
                    print("\t--> Not able to found that column. Please check\n")
            else:
                print("\t--> Not able to found that table. Please check\n")
        else:
            print("\t--> Not able to found that schema. Please check\n")

    return df


def run_query(query, column_names=[]):

    db_connection = mysql.connector.connect(host=DB_1A_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_SCHEMA)

    db_cursor = db_connection.cursor()
    db_cursor.execute(query)
    query_result = db_cursor.fetchall()
    if column_names:
        df = pd.DataFrame(query_result, columns=column_names)
    else:
        df = pd.DataFrame(query_result)

    return df


def look_for_state_column(df, schema_name, table_name):

    state_field_list = []
    correct_answer = True
    has_state_column = False

    for index, row in df.iterrows():
        val = row['COLUMN_NAME']
        if 'state' in val.lower():
            state_field_list.append(val)

    if state_field_list and len(state_field_list ) > 1:
        print("\nThe following state column(s) were found in {}.{} table".format(schema_name.upper(), table_name.upper()))
        for val in state_field_list:
            print("\t--> {}".format(val))
        while correct_answer:
            column_election = input("\n\tWhat column do you want to use for US State filter: ").strip()
            if column_election in state_field_list:
                print("\t\t--> Column '{}' was selected for {}.{} table.".format(column_election, schema_name.upper(), table_name.upper()))
                correct_answer = False
                has_state_column = True
            else:
                print("\t\t--> That column does not exist, please check and try again.")
    elif state_field_list and len(state_field_list) == 1:
        column_election = state_field_list[0]
        has_state_column = True
    else:
        print("There was not found any column with state value in {}.{}".format(schema_name.upper(), table_name.upper()))

    return has_state_column, column_election


def get_attributes_count(df, attribute_election):

    for index, row in df.iterrows():
        print("Table No. {}".format(index))
        print("\t - {}.{}".format(row['SCHEMA_NAME'], row['TABLE_NAME']))
        try:
            columns_query = "SELECT COLUMN_NAME " \
                            "FROM INFORMATION_SCHEMA.COLUMNS " \
                            "WHERE TABLE_SCHEMA LIKE '" + row['SCHEMA_NAME'] + "' " \
                            "AND TABLE_NAME LIKE '" + row['TABLE_NAME'] + "';"
            column_query_results = run_query(columns_query, ['COLUMN_NAME'])
            has_state, state_column_value = look_for_state_column(column_query_results, row['SCHEMA_NAME'], row['TABLE_NAME'])
            if has_state:
                final_df_column_names = [attribute_election.upper(), state_column_value.upper()]
                data_query = "SELECT `" + state_column_value + "`, COUNT(`" + row['COLUMN_NAME'] + "`) AS Total_Count " +\
                             "FROM " + row['SCHEMA_NAME'] + "." + row['TABLE_NAME'] + " " +\
                             "GROUP BY `" + state_column_value + "`;"
                print("\nQuery executed: \n\t--> {}\n".format(data_query))
                data_query_results = run_query(data_query, final_df_column_names)
                data_query_results.to_csv(row['SCHEMA_NAME'] + "_" + row['TABLE_NAME'] + "_count.csv", header=True)
                print(data_query_results)
            else:
                final_df_column_names = [attribute_election.upper()]
                data_query = "SELECT COUNT(DISTINCT (`" + row['COLUMN_NAME'] + "`)) " + \
                             "FROM " + row['SCHEMA_NAME'] + "." + row['TABLE_NAME'] + ";"
                print("\nQuery executed: \n\t--> {}\n".format(data_query))
                data_query_results = run_query(data_query, final_df_column_names)
                data_query_results.to_csv(row['SCHEMA_NAME'] + "_" + row['TABLE_NAME'] + "_count.csv", header=True)
                print(data_query_results)
        except Exception as e:
            print("\nError for table {}.{}".format(row['SCHEMA_NAME'], row['TABLE_NAME']))
            print("\t--> {}".format(e))
        print("\n" + "-" * 110)

    return


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

    original_attributes_df = pd.read_csv('DB_attributes_details.csv')
    print("What attribute(s) you want to extract count from:\n")
    df_type_filtered, attribute_election = extract_possible_fields(original_attributes_df)
    delete_column = input("Would you like to delete not needed columns (yes/no): ").strip().lower()
    if delete_column == 'yes':
        df_type_filtered = delete_columns_from_df(df_type_filtered)
    print("\n"+"-"*110+"\nStarting Attributes Count\n")
    get_attributes_count(df_type_filtered, attribute_election)



    ## PAST WORK ##
    # query = "select * from INFORMATION_SCHEMA.COLUMNS where UPPER(TABLE_NAME) like 'PROGRESO_FINANCIERO' order by TABLE_NAME"
    # query_2 = "SELECT `Social Security Number`, STATE FROM PFE.progreso_financiero"
    # column_names = ['SSN', 'STATE']
    # q_results_df = run_query(query_2, column_names)
    # states = ['California']
    # results_filtered = filter_by_states(q_results_df, states)
    # results_filtered.to_csv('filtered_results_Others.csv', header=True)


if __name__ == '__main__':
    main()