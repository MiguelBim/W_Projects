from smb.SMBConnection import SMBConnection
import pandas as pd
import tempfile
import warnings
import csv


warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', 700)
pd.set_option('display.max_columns', 700)
pd.set_option('display.width', 1000)


# FUNCTION TO FILTER MEANINGFUL FILES PATHS FROM BIGID INVENTORY REPORT
def extract_clarity_reports(report_name, sas_folder):

    df = pd.read_csv(report_name)
    reduced_df = df[['Full Object Name', 'Object Name', 'PII Count']]
    filtered_reduced_df = reduced_df[reduced_df['Full Object Name'].str.contains("clarity_inquiry")]
    filtered_reduced_df['Full Object Name'] = filtered_reduced_df['Full Object Name'].map(lambda x: x.lstrip(sas_folder))
    filtered_reduced_df.to_csv('clarity_files_paths.csv', header=True, index=True)

    return filtered_reduced_df


# FUNCTION TO READ AND RETRIEVE FILES FROM SAS SERVER
def retrieve_file_fromsmb(service_name, path):

    user_name = "XXXX"
    password = "XXXX"
    local_machine_name = "XXXX"
    server_machine_name = "XXXX"
    server_ip = "XXXX"
    path = path.replace('\\', '/')

    print("\nFile retrieval process was requested for file -> " + path)
    try:
        conn = SMBConnection(user_name, password, local_machine_name, server_machine_name, use_ntlm_v2=True, is_direct_tcp=True)
        assert conn.connect(server_ip, 445)
        tmp = tempfile.NamedTemporaryFile(delete=True)
        conn.retrieveFile(service_name, path, tmp)
        df = pd.read_csv(tmp.name)
        return df, path
    except:
        print("\t---> File retrieval process was FAILED for file -> " + path)

    # df.to_csv('readed_file.csv', header=True, index=True)


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
    consolidate_df.drop_duplicates(subset='social_security_number').reset_index(drop=True)
    consolidate_df.to_csv('data_consolidation_details.csv', header=True, index=True)

    return


if __name__ == '__main__':

    bigid_rep_name = "Objects (DS).csv"
    sas_server = 'Workspaces'

    # EXTRACT MEANINGFUL FILES PATHS
    clarity_files_path = extract_clarity_reports(bigid_rep_name, sas_server)
    # clarity_files_path.to_csv('clarity_files_paths_ds.csv', header=True, index=True)

    # CREATE UNIFIED DATAFRAME AUTOMATICALLY
    consolidate_data(clarity_files_path, sas_server)