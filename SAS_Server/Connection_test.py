import tempfile
import pandas as pd
from smb.SMBConnection import SMBConnection

share_name = "XXXX"
user_name = "XXXX"
password = "XXXX"
local_machine_name = "XXXX"
server_machine_name = "XXXX"
server_IP  = "XXXX"

# create and establish connection
conn = SMBConnection(user_name, password, local_machine_name, server_machine_name, use_ntlm_v2 = True, is_direct_tcp=True)
assert conn.connect(server_IP, 445)


def retrieve_file_fromsmb(service_name, path):
    tmp = tempfile.NamedTemporaryFile(delete=True)
    conn.retrieveFile(service_name, path, tmp)
    df = pd.read_csv(tmp.name)
    return df


df = retrieve_file_fromsmb('Square', "Driver_v5.csv")
print(len(df))