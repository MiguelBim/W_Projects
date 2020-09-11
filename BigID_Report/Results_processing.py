import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# FILE LOAD
bigid_report_df = pd.read_csv(r'Subject Access Report.csv')

# EXTRACTING MEANINGFUL FEATURES
reduce_df = bigid_report_df[['Proximity Id', 'Attribute', 'Value']]
columns_no = reduce_df.shape[1]
rows_no = reduce_df.shape[0]
print("There are " + str(rows_no) + " records in the read file")

# RAW DUPLICATES IDENTITY
duplicates = reduce_df.duplicated()

# DUPLICATES DELETION
clean_df = reduce_df.drop_duplicates()
# clean_df.set_index('Proximity Id', inplace=True)
dup_rows_no = clean_df.shape[0]
print("There were " + str(rows_no - dup_rows_no) + " duplicates in the read file")
clean_df.to_csv(r'clean_df.csv')

# DUPLICATES DELETE VALIDATION
# validation = clean_df.duplicated()

# WORKING WITH SUBSETS
df_subset = clean_df.loc[(clean_df['Proximity Id'] == 'cis.customer.-783909969'), ['Proximity Id','Attribute','Value']]
# df_subset = clean_df.loc[['cis.customer.-783909969','cis.customer_history_pre.441124143']]
print('\nThe subset is: ')
print(df_subset)
df_subset.to_csv(r'clean_subset_df.csv')

# PIVOTING DF
processed_df = pd.read_csv(r'clean_subset_df.csv')
print('\nDF Column names: ')
print (processed_df.columns.tolist())
pivot_df = (df_subset.pivot(index = 'Proximity Id', columns = 'Attribute', values = 'Value').reset_index(drop=True))
pivot_df.to_csv('pivot_table.csv', mode='w', encoding='utf-8')

# TRANSPOSE DF
# clean_df.set_index('Proximity Id', inplace=True)
# df_tr = clean_df.transpose()
# print(df_tr)
# df_tr.to_csv(r'tr_df.csv')
# After the file is proccessed, it is saved as csv file
