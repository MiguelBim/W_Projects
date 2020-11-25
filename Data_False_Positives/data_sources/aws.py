import tempfile
import pandas as pd
import math
import numpy as np
import csv
from dotenv import load_dotenv
load_dotenv()
import boto3


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


class S3Handler:

    def __init__(self, max_file_size=536870912):
        self.max_file_size = max_file_size

    def retrieve_file(self, full_path, attempts=5):
        bucket_name, path = self.split_path(full_path)
        file_size = 0

        for i in range(attempts):
            try:

                tmp = tempfile.NamedTemporaryFile(suffix=path.replace('/', '_')[-200:], delete=False)#, dir='/tmp/oportun')
                obj = boto3.resource('s3').Object(bucket_name, path)
                file_size = obj.content_length
                stream = obj.get(Range='bytes=0-{}'.format(self.max_file_size))['Body']
                tmp.write(stream.read())
                print("func=retrieve_file_from_smb, msg=downloaded, file_size={}, file={}".format(self.convert_size(file_size), path))
                return tmp.name
            except Exception as e:
                if i == attempts-1:
                    print("func=retrieve_file_from_smb, msg={}, file_size={}, file={}".format(e.__str__()[:100], self.convert_size(file_size), path))
                    return ''
        print("func=retrieve_file_from_smb, msg=ConnectionError, file_size={}, file={}".format(0, path))
        return ''

    def retrieve_text_file(self, path, attempts=5):
        return self.retrieve_file(path, attempts=attempts)

    def retrieve_csv_file(self, path, attempts=5):

        try:
            name = self.retrieve_file(path, attempts=attempts)
            df = csv_df(name)
            print("func=retrieve_file_from_smb, msg=downloaded, file_size={}, file={}".format(None, path))
            return df, path
        except Exception as e:
            print("func=retrieve_file_from_smb, msg={}, file_size={}, file={}".format(e.__str__()[:100], None, path))
            return pd.DataFrame(), path
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
        path = path.lstrip('/')
        return path.split('/')[0], '/'.join(path.split('/')[1:])

    # FUNCTION TO READ AND GET FILES SIZE
    def sort_files_from_server(self, list_paths):
        files = []
        failed_files = []
        sizes = []

        for full_path in list_paths:

            try:
                bucket_name, path = self.split_path(full_path)
                obj = boto3.resource('s3').Object(bucket_name, path)
                file_size = obj.content_length
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
        return sorted_df['file_paths'].tolist()

    def close(self):
        pass
