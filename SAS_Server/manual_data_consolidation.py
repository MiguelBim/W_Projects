import pandas as pd


# FUNCTION TO FILTER MEANINGFUL FILES PATHS FROM BIGID INVENTORY REPORT
def extract_clarity_reports(report_name):

    df = pd.read_csv(report_name)
    reduced_df = df[['Full Object Name', 'Object Name', 'PII Count']]
    filtered_reduced_df = reduced_df[reduced_df['Full Object Name'].str.contains("clarity_inquiry")]
    filtered_reduced_df.to_csv('clarity_files_paths.csv', header=True, index=True)

    return filtered_reduced_df


# FIRST FUNCTION CREATED FOR MANUAL (LOCAL) PROCESSING OF DATA CONSOLIDATION
def consolidate_data_manual():

    df_1 = pd.read_csv('2017_08_clarity_inquiry.csv')
    # df_1_subset = df_1.loc[(df_1['state'] == 'CA'), ['social_security_number']].fillna(0).astype(int)
    df_1_subset = df_1[['social_security_number', 'state']]
    df_1_subset.social_security_number = df_1_subset.social_security_number.fillna(0).astype(int)
    df_1_subset.insert(loc=0, column='Source', value='2017_08_clarity_inquiry.csv')
    df_1_subset.insert(loc=0, column='Path', value='Square/RAP/2017/08/clarity/aggregation/2017_08_clarity_inquiry.csv')
    df_1_subset.to_csv('new_test.csv', header=True, index=False)

    df_2 = pd.read_csv('2017_09_clarity_inquiry.csv')
    # df_2_subset = df_2.loc[(df_1['state'] == 'CA'), ['social_security_number']].fillna(0).astype(int)
    df_2_subset = df_2[['social_security_number', 'state']]
    df_2_subset.social_security_number = df_2_subset.social_security_number.fillna(0).astype(int)
    df_2_subset.insert(loc=0, column='Source', value='2017_09_clarity_inquiry.csv')
    df_2_subset.insert(loc=0, column='Path', value='Square/RAP/2017/09/clarity/aggregation/2017_09_clarity_inquiry.csv')
    df_2_subset.to_csv('new_test.csv', header=True, index=False)

    df_3 = pd.read_csv('2017_10_clarity_inquiry.csv')
    # df_3_subset = df_3.loc[(df_1['state'] == 'CA'), ['social_security_number']].fillna(0).astype(int)
    df_3_subset = df_3[['social_security_number', 'state']]
    df_3_subset.social_security_number = df_3_subset.social_security_number.fillna(0).astype(int)
    df_3_subset.insert(loc=0, column='Source', value='2017_10_clarity_inquiry.csv')
    df_3_subset.insert(loc=0, column='Path', value='Square/RAP/2017/10/clarity/aggregation/2017_10_clarity_inquiry.csv')
    df_3_subset.to_csv('new_test.csv', header=True, index=False)

    df_4 = pd.read_csv('2017_11_clarity_inquiry.csv')
    # df_4_subset = df_4.loc[(df_1['state'] == 'CA'), ['social_security_number']].fillna(0).astype(int)
    df_4_subset = df_4[['social_security_number', 'state']]
    df_4_subset.social_security_number = df_4_subset.social_security_number.fillna(0).astype(int)
    df_4_subset.insert(loc=0, column='Source', value='2017_11_clarity_inquiry.csv')
    df_4_subset.insert(loc=0, column='Path', value='Square/RAP/2017/11/clarity/aggregation/2017_11_clarity_inquiry.csv')
    df_4_subset.to_csv('new_test.csv', header=True, index=False)

    df_5 = pd.read_csv('2017_12_clarity_inquiry.csv')
    # df_5_subset = df_5.loc[(df_1['state'] == 'CA'), ['social_security_number']].fillna(0).astype(int)
    df_5_subset = df_5[['social_security_number', 'state']]
    df_5_subset.social_security_number = df_5_subset.social_security_number.fillna(0).astype(int)
    df_5_subset.insert(loc=0, column='Source', value='2017_12_clarity_inquiry.csv')
    df_5_subset.insert(loc=0, column='Path', value='Square/RAP/2017/12/clarity/aggregation/2017_12_clarity_inquiry.csv')
    df_5_subset.to_csv('new_test.csv', header=True, index=False)

    # df_6 = pd.read_csv('2017_08_clarity_xml_response.csv')
    # df_6 = df_6.rename(columns={'inquiry_social_security_number': 'social_security_number', 'inquiry_state': 'state'})
    # df_6_subset = df_6.loc[(df_6['state'] == 'CA'), ['social_security_number']].fillna(0).astype(int)
    #
    # df_7 = pd.read_csv('2017_09_clarity_xml_response.csv')
    # df_7 = df_7.rename(columns={'inquiry_social_security_number': 'social_security_number', 'inquiry_state': 'state'})
    # df_7_subset = df_7.loc[(df_7['state'] == 'CA'), ['social_security_number']].fillna(0).astype(int)
    #
    # df_8 = pd.read_csv('2017_10_clarity_xml_response.csv')
    # df_8 = df_8.rename(columns={'inquiry_social_security_number': 'social_security_number', 'inquiry_state': 'state'})
    # df_8_subset = df_8.loc[(df_8['state'] == 'CA'), ['social_security_number']].fillna(0).astype(int)
    #
    # df_9 = pd.read_csv('2017_11_clarity_xml_response.csv')
    # df_9 = df_9.rename(columns={'inquiry_social_security_number': 'social_security_number', 'inquiry_state': 'state'})
    # df_9_subset = df_9.loc[(df_9['state'] == 'CA'), ['social_security_number']].fillna(0).astype(int)
    #
    # df_10 = pd.read_csv('2017_12_clarity_xml_response.csv')
    # df_10 = df_10.rename(columns={'inquiry_social_security_number': 'social_security_number', 'inquiry_state': 'state'})
    # df_10_subset = df_10.loc[(df_10['state'] == 'CA'), ['social_security_number']].fillna(0).astype(int)

    # final_df = pd.concat([df_1_subset, df_2_subset, df_3_subset, df_4_subset, df_5_subset, df_6_subset, df_7_subset, df_8_subset, df_9_subset, df_10_subset], ignore_index=True).drop_duplicates().reset_index(drop=True)
    final_df = pd.concat([df_1_subset, df_2_subset, df_3_subset, df_4_subset, df_5_subset], ignore_index=True).drop_duplicates(subset='social_security_number').reset_index(drop=True)
    final_df.to_csv('data_consolidation_details.csv', header=True, index=True)

    return


if __name__ == '__main__':

    bigid_rep_name = "Objects (DS).csv"

    # EXTRACT MEANINGFUL FILES PATHS
    clarity_files_path = extract_clarity_reports(bigid_rep_name)
    # clarity_files_path.to_csv('clarity_files_paths_ds.csv', header=True, index=True)

    # CREATE UNIFIED DATAFRAME MANUALLY
    consolidate_data_manual()