import dask
import os
import re
import csv
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from smb.SMBConnection import SMBConnection
import tempfile
import math


load_dotenv()
SMB_USER = os.getenv("SMB_USER")
SMB_PASSWORD = os.getenv("SMB_PASSWORD")
SMB_HOST = os.getenv("SMB_HOST")


def csv_df(path):
    chunksize = 100000
    try:
        tfr = pd.read_csv(path, chunksize=chunksize,
                          iterator=True, dtype=str, error_bad_lines=False)
    except:
        tfr = pd.read_csv(path, chunksize=chunksize, quoting=csv.QUOTE_NONE,
                          iterator=True, dtype=str, error_bad_lines=False)

    df = pd.concat(tfr, ignore_index=True)
    return df


class SMBHandler:

    def __init__(self, user_name=SMB_USER, password=SMB_PASSWORD, host=SMB_HOST):
        self.user_name = user_name
        self.password = password
        self.local_machine_name = "laptop"
        self.server_machine_name = host
        self.server_ip = host

    def retrieve_text_file_from_smb(self, service_name, path, attemps=5):
        path = path.replace('\\', '/')
        file_size = 0
        self.conn = SMBConnection(self.user_name, self.password, self.local_machine_name, self.server_machine_name,
                                  use_ntlm_v2=True, is_direct_tcp=True)
        self.conn.connect(self.server_ip, 445)
        for i in range(attemps):
            try:

                tmp = tempfile.NamedTemporaryFile(suffix=path.replace('/', '_'))#, dir='/tmp/oportun')
                attributes = self.conn.getAttributes(service_name, path)
                file_size = attributes.file_size  # tmp.file.tell()
                self.conn.retrieveFile(service_name, path, tmp)
                print("func=retrieve_text_file_from_smb, msg=downloaded, file_size={}, file={}".format(self.convert_size(file_size), path))
                self.conn.close()
                return tmp.name, path
            except Exception as e:
                if i == attemps-1:
                    print("func=retrieve_text_file_from_smb, msg={}, file_size={}, file={}".format(e.__str__()[:100], file_size, path))
                    self.conn.close()
                    return '', path
        print("func=retrieve_text_file_from_smb, msg=ConnectionError, file_size={}, file={}".format(0, path))
        self.conn.close()
        return '', path

    def retrieve_csv_from_smb(self, service_name, path, attempts=5):
        path = path.replace('\\', '/')
        file_size = 0
        self.conn = SMBConnection(self.user_name, self.password, self.local_machine_name, self.server_machine_name,
                                  use_ntlm_v2=True, is_direct_tcp=True)
        self.conn.connect(self.server_ip, 445)
        for i in range(attempts):
            try:

                tmp = tempfile.NamedTemporaryFile(suffix=path.replace('/', '_'))#, dir='/tmp/oportun')
                attributes = self.conn.getAttributes(service_name, path)
                file_size = attributes.file_size  # tmp.file.tell()
                self.conn.retrieveFile(service_name, path, tmp)
                df = csv_df(tmp.name)
                print("func=retrieve_file_from_smb, msg=downloaded, file_size={}, file={}".format(self.convert_size(file_size), path))
                self.conn.close()
                return df, path
            except Exception as e:
                if i == attempts-1:
                    print("func=retrieve_file_from_smb, msg={}, file_size={}, file={}".format(e.__str__()[:100], self.convert_size(file_size), path))
                    self.conn.close()
                    return pd.DataFrame(), path
        print("func=retrieve_file_from_smb, msg=ConnectionError, file_size={}, file={}".format(0, path))
        self.conn.close()
        return pd.DataFrame(), path

    def convert_size(self, size_bytes):
        if size_bytes == 0 or np.isnan(size_bytes):
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(float(size_bytes), 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])

    def split_smb_path(self, path):
        return path.split('/')[0], '/'.join(path.split('/')[1:])

    # FUNCTION TO READ AND GET FILES SIZE
    def sort_files_from_server(self, list_paths):
        files = []
        failed_files = []
        sizes = []
        self.conn = SMBConnection(self.user_name, self.password, self.local_machine_name, self.server_machine_name,
                                  use_ntlm_v2=True, is_direct_tcp=True)
        self.conn.connect(self.server_ip, 445)
        for full_path in list_paths:


            try:
                service_name, path = self.split_smb_path(full_path.replace('\\', '/'))
                attributes = self.conn.getAttributes(service_name, path)
                file_size = attributes.file_size
                sizes.append(file_size)
                files.append(full_path)
            except Exception as e:
                sizes.append(0)
                files.append(full_path)
                print("func=sort_files_from_server, path={}, msg={}".format(full_path, e.__str__[:100]))
                continue
        size_df = pd.DataFrame()
        size_df['file_paths'] = pd.Series(files)
        size_df['file_size'] = pd.Series(sizes)

        # SORTING AND CONVERTING DATA
        sorted_df = size_df.sort_values(by='file_size', ascending=True).reset_index(drop=True)
        sorted_df['file_size'] = sorted_df['file_size'].map(lambda val: self.convert_size(val))

        # SAVE DATAFRAME TO CSV
        # sorted_df.to_csv('Objects_sorted.csv')
        return sorted_df['file_paths'].tolist()


@dask.delayed
def retrieve_file_from_smb_concurrent(smb_handler, service_name, path, attemps=5):

    return smb_handler.retrieve_csv_from_smb(service_name, path, attemps)


retrieve_file_from_smb_concurrent = dask.delayed(retrieve_file_from_smb_concurrent)


def retrieve_files_concurrently(paths, service_name):
    smb_handler = SMBHandler()
    data = [retrieve_file_from_smb_concurrent(smb_handler, service_name, name) for name in paths]
    result_set = dask.compute(data)
    smb_handler.conn.close()
    return result_set


if __name__ == '__main__':
    paths = open('paths.csv')
    lines = paths.readlines()[:2]
    lines = [re.sub(r"\s+$", "", i, flags=re.UNICODE) for i in lines]
    service_name = "Workspaces"
    results = retrieve_files_concurrently(paths, service_name)
    print(results)
