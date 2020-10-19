from smb.SMBConnection import SMBConnection
from dotenv import load_dotenv
import pandas as pd
import warnings
import os
import math


pd.set_option('display.max_rows', 700)
pd.set_option('display.max_columns', 700)
pd.set_option('display.width', 1000)

load_dotenv()
SMB_USER = os.getenv("SMB_USER")
SMB_PASSWORD = os.getenv("SMB_PASSWORD")
SMB_SERVER_NAME = os.getenv("SMB_SERVER_NAME")
SMB_SERVER_IP = os.getenv("SMB_HOST")
warnings.filterwarnings("ignore")


# CONVERTION FROM BYTES TO MB
def convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(float(size_bytes), 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])


# FUNCTION TO READ AND GET FILES SIZE
def sort_files_from_server(service_name, csv_name):
    print("Files sorting process was initiated")
    user_name = SMB_USER
    password = SMB_PASSWORD
    local_machine_name = "laptop"
    server_machine_name = SMB_SERVER_NAME
    server_ip = SMB_SERVER_IP

    paths_df = pd.read_csv(csv_name)
    paths_df = paths_df['Full Object Name'].map(lambda x: x.lstrip(service_name))
    files = []
    sizes = []

    for path in paths_df:
        print(".")
        path = path.replace('\\', '/')
        files.append(path)
        try:
            # print("\nFile information retrieval process was requested for file -> " + path)
            conn = SMBConnection(user_name, password, local_machine_name, server_machine_name, use_ntlm_v2=True, is_direct_tcp=True)
            assert conn.connect(server_ip, 445)
            attributes = conn.getAttributes(service_name, path)
            file_size = attributes.file_size
            sizes.append(file_size)
        except:
            print("Information retrieval process failed for file " + path)
            sizes.append(0)
            continue
    size_df = pd.DataFrame()
    size_df['file_paths'] = pd.Series(files)
    size_df['file_size'] = pd.Series(sizes)

    # SORTING AND CONVERTING DATA
    sorted_df = size_df.sort_values(by='file_size', ascending=True).reset_index(drop=True)
    sorted_df['file_size'] = sorted_df['file_size'].map(lambda val: convert_size(val))

    # SAVE DATAFRAME TO CSV
    sorted_df.to_csv('Objects_sorted.csv')
    print(sorted_df)


if __name__ == '__main__':
    bigid_rep_name = "Objects.csv"
    sas_server = 'Workspaces'
    sort_files_from_server(sas_server, bigid_rep_name)