import dask
import os
import re
from data_sources.smb import SMBHandler
from dotenv import load_dotenv


load_dotenv()


@dask.delayed
def retrieve_file_from_smb_concurrent(smb_handler, service_name, path, attemps=5):
    full_path = os.path.join(service_name, path)
    return smb_handler.retrieve_csv_file(full_path, attemps=attemps)


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
