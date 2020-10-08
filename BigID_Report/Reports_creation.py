#%%
import numpy as np
import pandas as pd
import json
import os
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def join_reports(dir_name):
    reports = os.listdir(dir_name)
    df = pd.DataFrame()
    for r in reports:
        df_aux = pd.read_csv('{}/{}'.format(dir_name, r))
        if not df.empty:
            df = df.append(df_aux)
        else:
            df = df_aux
    return df


def semicolon_format(df):
    rows = []
    for group_name, df_group in df.groupby(['Proximity Id']):
        row = (df_group['Data Source'].iloc[0], df_group['Full Object Name'].iloc[0],
            ';'.join(df_group['Value'].to_list()), ';'.join(df_group['Attribute'].to_list()))
        rows.append(row)
    new_df = pd.DataFrame(rows, columns=['Data Source', 'Full Object Name', 'Value', 'Attribute'])
    return new_df


def json_format(df):
    df = df.replace({np.nan: None})
    records = []
    for group_name, df_group in df.groupby(['Proximity Id']):
        attributes = df_group[['Value', 'Attribute', 'Attribute Type', 'Primary Key', 'Linked Path']].to_dict('records')
        record = {
            'Proximity Id': group_name,
            'Data Source': df_group['Data Source'].iloc[0],
            'Full Object Name': df_group['Full Object Name'].iloc[0],
            'Attributes': attributes
        }
        records.append(record)
    return records


def pivot_table_format(df):

    df_list = []

    reduced_df = df[['Proximity Id', 'Attribute', 'Value']]
    for group_name, df_group in reduced_df.groupby(['Proximity Id']):
        df_subset = df.loc[(df['Proximity Id'] == group_name), ['Proximity Id', 'Attribute', 'Value']]
        try:
            pivot_df = (df_subset.pivot(index='Proximity Id', columns='Attribute', values='Value').reset_index(drop=True))
            pivot_df.insert(loc=0, column='Source', value=group_name)
            pivot_df = pivot_df.columns.to_frame().T.append(pivot_df, ignore_index=True)
            pivot_df.columns = range(len(pivot_df.columns))
        except:
            print("Dataframe with proximity ID: " + str(group_name) + " has duplicate columns. It is not posibble to pivot this datasource!")
            continue
        df_list.append(pivot_df)
        final_df = pd.concat(df_list)

    print("\nReport 2 (Pivot table) was already completed")

    return final_df


if __name__ == '__main__':

    # Data load
    df = pd.read_csv('full_subject.csv')
    # df = join_reports('reports')

    # Meaningful columns extraction from full report and filter for BigID view
    df = df.drop_duplicates(['Value', 'Attribute Original Name', 'Proximity Id'])
    df = df[df['Full Object Name'] != '72_10_197_zeus.progreso_production.BigID Client Entity Clients Addresses']

    # REPORT 1 JSON FORMAT
    records = json_format(df)
    with open('report_1_json_format.json', 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=4)
    df.to_csv('bigID_original_format.csv', index=False)

    # REPORT 2 PIVOT TABLE
    final_df = pivot_table_format(df)
    final_df.to_csv('report_2_pivot_table_format.csv', header=False, index=False)

    # REPORT 3 SEMICOLON TABLE
    semicolon_df = semicolon_format(df)
    semicolon_df.to_csv('report_3_semicolon_separated_format.csv', index=False)