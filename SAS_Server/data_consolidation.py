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
    reduced_df = df[['Full Object Name', 'Object Name', 'PII Count']]
    filtered_reduced_df = reduced_df[reduced_df['Full Object Name'].str.contains("clarity")]
    filtered_reduced_df['Full Object Name'] = filtered_reduced_df['Full Object Name'].map(lambda x: x.lstrip(sas_folder))
    # filtered_reduced_df.to_csv('clarity_files_paths.csv', header=True, index=True)

    return filtered_reduced_df


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

    except:
        print("\t---> File retrieval process was FAILED for file -> " + path)


# FUNCTION CREATED FOR CONSOLIDATION OF ALL CLARITY_INQUIRY FILES
def consolidate_data(bigid_rep_df, service_name):
    consolidate_df = pd.DataFrame([])
    for path in bigid_rep_df['Full Object Name']:
        try:
            sas_file_df, real_path = retrieve_file_fromsmb(service_name, path)
            sas_df_subset = sas_file_df[['social_security_number', 'state']]
            sas_df_subset.social_security_number = sas_df_subset.social_security_number.fillna(0).astype(int)
            sas_df_subset.insert(loc=0, column='Path', value=real_path)
            # sas_df_subset.to_csv('processed_file.csv', header=True, index=False)
            consolidate_df = consolidate_df.append(sas_df_subset, ignore_index=True)
            print("\t---> Process of data consolidation was done successfully for file -> " + real_path)
        except:
            print("\t---> Process of data consolidation FAILED for file -> " + path)
    consolidate_df = consolidate_df[consolidate_df['social_security_number'] != 0]
    final_df = consolidate_df.drop_duplicates(subset='social_security_number', keep='first').reset_index(drop=True)
    final_df.to_csv('data_consolidation_details.csv', header=True, index=True)

    return


def filter_by_states(df, states):

    try:
        filtered_df = df.loc[df['state'].isin(states)]
        filtered_df.insert(loc=0, column='Num', value=range(1, len(filtered_df) + 1))
        filtered_df.drop_duplicates(subset='social_security_number')
        filtered_df.to_csv('data_consolidation (Others).csv', header=True, index=False)
    except:
        print('There was an issue when filtering for those values. Please check')

    return


if __name__ == '__main__':
    bigid_rep_name = "Objects (DS).csv"
    sas_server = 'Workspaces'

    # EXTRACT MEANINGFUL FILES PATHS
    clarity_files_path = extract_clarity_reports(bigid_rep_name, sas_server)
    clarity_files_path.to_csv('clarity_files_paths_ds.csv', header=True, index=False)

    # CREATE UNIFIED DATAFRAME AUTOMATICALLY
    # consolidate_data(clarity_files_path, sas_server)

    # FILTER BY STATES (LIST OF VALUE) THE CONSOLIDATION FILE
    # df = pd.read_csv("data_consolidation_details.csv", index_col=0)
    # states = ['CA','TX']
    # filter_by_states(df, states)
