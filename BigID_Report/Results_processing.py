import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


# FILE LOAD FROM CSV EXPORTED REPORT
def read_file(file_name):

    bigid_report_df = pd.read_csv(file_name)

    return bigid_report_df


# EXTRACTING MEANINGFUL FEATURES FROM REPORT
def extract_features(raw_df):
    reduce_df = raw_df[['Proximity Id', 'Attribute', 'Value']]
    columns_no = reduce_df.shape[1]
    rows_no = reduce_df.shape[0]
    print("There are " + str(rows_no) + " records in the read file")

    return reduce_df, rows_no


# RAW DUPLICATES IDENTITY AND DELETION
def duplicates_deletion(features_df, rows_no):

    features_df.duplicated()
    clean_df = features_df.drop_duplicates()
    dup_rows_no = clean_df.shape[0]
    print("There were " + str(rows_no - dup_rows_no) + " duplicates in the read file")
    # clean_df.to_csv(r'clean_df.csv')
    # Duplicates delete validation
    # validation = clean_df.duplicated()
    # print(validation)

    return clean_df


# WORKING WITH SUBSETS
def subsets_processing(clean_df, record_identifier):
    df_subset = clean_df.loc[(clean_df['Proximity Id'] == record_identifier), ['Proximity Id','Attribute','Value']]
    print('\nThe subset is: ')
    print(df_subset)
    # df_subset.to_csv(r'clean_subset_df.csv')

    return df_subset


# PIVOTING DF DATAFRAME
def pivot_dataframe(df_subset):
    # processed_df = pd.read_csv(r'clean_subset_df.csv')
    # print('\nDF Column names: ')
    # print (processed_df.columns.tolist())
    pivot_df = (df_subset.pivot(index = 'Proximity Id', columns = 'Attribute', values = 'Value').reset_index(drop=True))
    pivot_df.to_csv('pivot_table.csv', mode='w', encoding='utf-8')

    return pivot_df


if __name__ == '__main__':

    file_name = 'Subject Access Report.csv'
    record_identifier = 'lead_source.lead_phone.132171347'
    file_df = read_file(file_name)
    features_df, rows_no = extract_features(file_df)
    clean_df = duplicates_deletion(features_df, rows_no)
    subset_df = subsets_processing(clean_df, record_identifier)
    pivot_df = pivot_dataframe(subset_df)
    print(pivot_df)

