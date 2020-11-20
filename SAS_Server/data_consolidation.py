from smb.SMBConnection import SMBConnection
from dotenv import load_dotenv
import pandas as pd
import tempfile
import warnings
import csv
import os

pd.set_option('display.max_rows', 700)
pd.set_option('display.max_columns', 700)
pd.set_option('display.width', 1000)

load_dotenv()
SMB_USER = os.getenv("SMB_USER")
SMB_PASSWORD = os.getenv("SMB_PASSWORD")
SMB_SERVER_NAME = os.getenv("SMB_SERVER_NAME")
SMB_SERVER_IP = os.getenv("SMB_SERVER_IP")
warnings.filterwarnings("ignore")


# FUNCTION TO FILTER MEANINGFUL FILES PATHS FROM BIGID INVENTORY REPORT
def extract_clarity_reports(report_name, sas_folder):
    df = pd.read_csv(report_name)
    reduced_df = df[['Full Object Name']]
    # filtered_reduced_df = reduced_df[reduced_df['Full Object Name'].str.contains("clarity")]
    reduced_df['Full Object Name'] = reduced_df['Full Object Name'].map(lambda x: x.lstrip(sas_folder))
    reduced_df.to_csv('filtered_files_paths.csv', header=True, index=True)

    return reduced_df


# FUNCTION TO READ AND RETRIEVE FILES FROM SAS SERVER
def retrieve_file_fromsmb(service_name, path):
    user_name = SMB_USER
    password = SMB_PASSWORD
    local_machine_name = "laptop"
    server_machine_name = SMB_SERVER_NAME
    server_ip = SMB_SERVER_IP
    path = path.replace('\\', '/')


    print("\nFile retrieval process was requested for file -> " + path)
    try:
        conn = SMBConnection(user_name, password, local_machine_name, server_machine_name, use_ntlm_v2=True, is_direct_tcp=True)
        assert conn.connect(server_ip, 445)
        tmp = tempfile.NamedTemporaryFile(delete=True)
        conn.retrieveFile(service_name, path, tmp)
        chunksize = 100000
        try:
            tfr = pd.read_csv(tmp.name, chunksize=chunksize,
                              iterator=True, dtype=str, error_bad_lines=False)
        except:
            tfr = pd.read_csv(tmp.name, chunksize=chunksize, quoting=csv.QUOTE_NONE,
                              iterator=True, dtype=str, error_bad_lines=False)

        df = pd.concat(tfr, ignore_index=True)

        return df, path

    except Exception as e:
        print("\t---> File retrieval process was FAILED for file -> " + path)
        print("Error: {}". format(e))


# FUNCTION CREATED FOR CONSOLIDATION OF ALL FILES FROM SERVER
def consolidate_data(bigid_rep_df, service_name):
    consolidate_df = pd.DataFrame([])
    for path in bigid_rep_df['Full Object Name']:
        try:
            sas_file_df, real_path = retrieve_file_fromsmb(service_name, path)
            sas_df_subset = sas_file_df[['SocialSecurityNumber', 'IDState']]
            sas_df_subset.SocialSecurityNumber = sas_df_subset.SocialSecurityNumber.fillna(0).astype(int)
            sas_df_subset.insert(loc=0, column='Path', value=real_path)
            # sas_df_subset.to_csv('processed_file.csv', header=True, index=False)
            consolidate_df = consolidate_df.append(sas_df_subset, ignore_index=True)
            print("\t---> Process of data consolidation was done successfully for file -> " + real_path)
        except Exception as e:
            print("\t---> Process of data consolidation FAILED for file -> " + path)
            print("Error: {}".format(e))

    consolidate_df = consolidate_df[consolidate_df['SocialSecurityNumber'] != 0]
    final_df = consolidate_df.drop_duplicates(subset='SocialSecurityNumber', keep='first').reset_index(drop=True)
    final_df.to_csv('data_consolidation.csv', header=True, index=True)

    return


# EXTRACT DATA FROM LOCAL FILE
def extract_data_locally(bigid_rep_name, real_path):
    bigid_rep_df = pd.read_csv(bigid_rep_name)
    consolidate_df = pd.DataFrame([])
    try:
        sas_df_subset = bigid_rep_df[['SocialSecurityNumber', 'State']]
        sas_df_subset.SocialSecurityNumber = sas_df_subset.SocialSecurityNumber.fillna(0).astype(int)
        sas_df_subset.insert(loc=0, column='Path', value=real_path)
        consolidate_df = consolidate_df.append(sas_df_subset, ignore_index=True)
        print("\t---> Process of data consolidation was done successfully for file -> " + real_path)
    except Exception as e:
        print("Error: {}".format(e))

    consolidate_df = consolidate_df[consolidate_df['SocialSecurityNumber'] != 0]
    final_df = consolidate_df.drop_duplicates(subset='SocialSecurityNumber', keep='first').reset_index(drop=True)
    final_df.to_csv('data_consolidation_details_2.csv', header=True, index=True)

    return


# READ AND CONSOLIDATE DIFFERENT "SUBCONSOLIDATED FILES" LOCALLY
def integrate_local_files(number_of_files):

    consolidate_df = pd.DataFrame([])

    for file_num in range(1, number_of_files + 1):
        try:
            file_name = input("\nWhat is the name of the file NUMBER {}: ".format(file_num)).strip()
            file_df = pd.read_csv(file_name, index_col=False) #, skiprows=[1]
            file_df.columns = ['', 'Path', 'Social_security_number', 'State']
            consolidate_df = consolidate_df.append(file_df, ignore_index=True)
        except Exception as e:
            print("Consolidation process failed for {}".format(file_name))
            print("Error: {}".format(e))

    consolidate_df = consolidate_df[consolidate_df['Social_security_number'] != 0]
    final_df = consolidate_df.drop_duplicates(subset='Social_security_number', keep='first').reset_index(drop=True)
    final_df.to_csv('data_consolidation_details_final.csv', header=True, index=True)

    return


def delete_column_from_csv(csv_file_name, column_to_remove):

    original_df = pd.read_csv(csv_file_name)
    changed_df = original_df.drop(column_to_remove, axis=1)
    changed_df.to_csv('data_consolidation_no_ssn.csv', header=True, index=False)

    return


def filter_by_states(df, states):

    try:
        # IN
        # filtered_df = df.loc[df['State'].isin(states)]
        # NOT IN
        filtered_df = df.loc[~df['State'].isin(states)]
        # filtered_df.insert(loc=0, column='Num', value=range(1, len(filtered_df) + 1))
        filtered_df.drop_duplicates(subset='State')
        filtered_df.to_csv('data_consolidation (Others).csv', header=True, index=False)
    except Exception as e:
        print('There was an issue when filtering for those values. Please check')
        print(e)

    return


if __name__ == '__main__':

    # EXTRACT MEANINGFUL FILES PATHS
    # bigid_rep_name = "Objects.csv"
    # sas_server = 'Workspaces'
    # clarity_files_path = extract_clarity_reports(bigid_rep_name, sas_server)
    # clarity_files_path.to_csv('clarity_files_paths_rap.csv', header=True, index=False)

    # CREATE UNIFIED DATAFRAME AUTOMATICALLY
    # consolidate_data(clarity_files_path, sas_server)
    # files_to_integrate = 6
    # integrate_local_files(files_to_integrate)

    # DELETE COLUMN (NORMALLY SSN)
    # csv_name = 'data_consolidation_details_final.csv'
    # column_name = 'Social_security_number'
    # delete_column_from_csv(csv_name, column_name)

    # FILTER BY STATES (LIST OF VALUE) THE CONSOLIDATION FILE
    df = pd.read_csv("data_consolidation_no_ssn.csv")
    print(df)
    states = ['TX', 'CA']
    filter_by_states(df, states)



    ####### LOCAL PROCESS #######

    # EXTRACT SSN FROM LOCAL FILE
    # file_path = "/RAP/2017/06/alltrust/aggregation/2017_06_alltrust_CustomerInformation.csv"
    # file_name = "2017_06_alltrust_CustomerInformation.csv"
    # extract_data_locally(file_name, file_path)

    # CONSOLIDATE LOCAL FILES ALREADY PROCESSED
    # number_of_files = 3
    # consolidate_local_files(number_of_files)
