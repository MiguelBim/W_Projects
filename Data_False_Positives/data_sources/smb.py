from smb.SMBConnection import SMBConnection
import tempfile
import math
import csv
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
# import pyreadstat

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
        self.conn = None
        self.connect()

    def retrieve_file(self, path, attempts=5):
        service_name, path = self.split_path(path)
        path = path.replace('\\', '/')
        file_size = 0

        for i in range(attempts):
            try:
                self.connect()
                tmp = tempfile.NamedTemporaryFile(suffix=path.replace('/', '_'))#, dir='/tmp/oportun')
                attributes = self.conn.getAttributes(service_name, path)
                file_size = attributes.file_size  # tmp.file.tell()
                self.conn.retrieveFile(service_name, path, tmp)
                print("func=retrieve_file_from_smb, msg=downloaded, file_size={}, file={}".format(self.convert_size(file_size), path))
                self.close()
                return tmp.name
            except Exception as e:
                if i == attempts-1:
                    print("func=retrieve_file_from_smb, msg={}, file_size={}, file={}".format(e.__str__()[:100], self.convert_size(file_size), path))
                    self.close()
                    return ''
        print("func=retrieve_file_from_smb, msg=ConnectionError, file_size={}, file={}".format(0, path))
        self.close()
        return ''

    def retrieve_text_file(self, path, attemps=5):
        service_name, path = self.split_path(path)

        path = path.replace('\\', '/')
        file_size = 0

        for i in range(attemps):
            try:
                self.connect()
                tmp = tempfile.NamedTemporaryFile(suffix=path.replace('/', '_'))#, dir='/tmp/oportun')
                attributes = self.conn.getAttributes(service_name, path)
                file_size = attributes.file_size  # tmp.file.tell()
                self.conn.retrieveFile(service_name, path, tmp)
                print("func=retrieve_text_file, msg=downloaded, file_size={}, file={}".format(self.convert_size(file_size), path))
                self.close()
                return tmp.name
            except Exception as e:
                if i == attemps-1:
                    self.close()
                    print("func=retrieve_text_file, msg={}, file_size={}, file={}".format(e.__str__()[:100], file_size, path))
                    return ''
        print("func=retrieve_text_file, msg=ConnectionError, file_size={}, file={}".format(0, path))
        self.close()
        return ''

    def retrieve_csv_file(self, path, attempts=5):
        service_name, path = self.split_path(path)
        path = path.replace('\\', '/')
        file_size = 0

        for i in range(attempts):
            try:
                self.connect()
                tmp = tempfile.NamedTemporaryFile(suffix=path.replace('/', '_'))#, dir='/tmp/oportun')
                attributes = self.conn.getAttributes(service_name, path)
                file_size = attributes.file_size  # tmp.file.tell()
                self.conn.retrieveFile(service_name, path, tmp)
                df = csv_df(tmp.name)
                print("func=retrieve_file_from_smb, msg=downloaded, file_size={}, file={}".format(self.convert_size(file_size), path))
                self.close()
                return df, path
            except Exception as e:
                if i == attempts-1:
                    self.close()
                    print("func=retrieve_file_from_smb, msg={}, file_size={}, file={}".format(e.__str__()[:100], self.convert_size(file_size), path))
                    return pd.DataFrame(), path
        print("func=retrieve_file_from_smb, msg=ConnectionError, file_size={}, file={}".format(0, path))
        self.close()
        return pd.DataFrame(), path

    def retrieve_sas_file(self, path, attempts=5):

        try:
            name = self.retrieve_file(path, attempts=attempts)
            if not name:
                raise ConnectionError('File Not found')
            df, _ = pyreadstat.read_sas7bdat(name)
            for c in df.columns:
                df[c] = df[c].astype(str)
            print("func=retrieve_file_from_smb, msg=downloaded, file_size={}, file={}".format(None, path))
            self.close()
            return df
        except Exception as e:
            self.close()
            print("func=retrieve_file_from_smb, msg={}, file_size={}, file={}".format(e.__str__()[:100], None, path))
            return pd.DataFrame()
        self.close()
        print("func=retrieve_file_from_smb, msg=ConnectionError, file_size={}, file={}".format(0, path))
        return pd.DataFrame()

    def convert_size(self, size_bytes):
        if size_bytes == 0 or np.isnan(size_bytes):
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(float(size_bytes), 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])

    @classmethod
    def split_path(cls, path):
        return path.split('/')[0], '/'.join(path.split('/')[1:])

    # FUNCTION TO READ AND GET FILES SIZE
    def sort_files_from_server(self, list_paths):
        files = []
        failed_files = []
        sizes = []
        self.connect()
        for full_path in list_paths:

            try:
                service_name, path = self.split_path(full_path.replace('\\', '/'))
                attributes = self.conn.getAttributes(service_name, path)
                file_size = attributes.file_size
                sizes.append(file_size)
                files.append(full_path)
            except Exception as e:
                sizes.append(0)
                files.append(full_path)
                print("func=sort_files_from_server, path={}, msg={}".format(full_path, e.__str__()[:100]))
                continue
        size_df = pd.DataFrame()
        size_df['file_paths'] = pd.Series(files)
        size_df['file_size'] = pd.Series(sizes)

        # SORTING AND CONVERTING DATA
        sorted_df = size_df.sort_values(by='file_size', ascending=True).reset_index(drop=True)
        sorted_df['file_size'] = sorted_df['file_size'].map(lambda val: self.convert_size(val))

        # SAVE DATAFRAME TO CSV
        # sorted_df.to_csv('Objects_sorted.csv')
        self.close()
        return sorted_df['file_paths'].tolist()

    def close(self):
        self.conn.close()

    def connect(self):
        self.conn = SMBConnection(self.user_name, self.password, self.local_machine_name,
                                  self.server_machine_name,
                                  use_ntlm_v2=True, is_direct_tcp=True)
        self.conn.connect(self.server_ip, 445)
