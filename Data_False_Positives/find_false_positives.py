from zipfile import ZipFile
import tempfile
import pandas as pd
import confuse
import json
from data_sources.smb import SMBHandler, csv_df
from data_sources.aws import S3Handler
import dask
from timeit import default_timer as timer
import re
import os
from collections import Counter
from io import StringIO
import csv


INVALID_FILES = ['xls', 'xlsx', 'pdf']


def get_config():
    import argparse

    parser = argparse.ArgumentParser(description='Validator or patterns in csv files')
    parser.add_argument('--report_file', help='Inventory Report provided by BigID', dest='BigID.report_file')
    parser.add_argument('--output_file', help='File name to dump the output', dest='scan_files.output_file')
    parser.add_argument('--offset', help='Position to start the scanning', dest='scan_files.pagination.offset', type=int)
    parser.add_argument('--limit', help='Position to finish the scanning', dest='scan_files.pagination.limit', type=int)
    parser.add_argument('--page_size', help='Number of parallel downloads', dest='scan_files.pagination.page_size', type=int)
    parser.add_argument('--data_source_type', help='Data source type: smb or s3', dest='BigID.data_source_type', type=str)
    args = parser.parse_args()

    config = confuse.Configuration("FindFalsePositves", __name__)
    config.set_file('config.yaml')

    config.set_args(args, dots=True)
    return config

class FileProcessor():

    def __init__(self, paths, offset, limit, data_source_class):
        zip_paths, paths = FileUtils.split_zip_and_files(paths)
        self.zip_paths = zip_paths
        self.file_paths = paths
        self.full_paths = paths + list(zip_paths.keys())
        self.data_source_class = data_source_class
        self.zip_objects = [{'remote_path': i, "inside_paths": self.zip_paths[i]}
                            for i in self.data_source_class().sort_files_from_server(self.zip_paths.keys())]
        self.offset = offset
        self.limit = limit

    @classmethod
    def find_samples_for_data_frame(cls, df, classifiers):
        results = {}

        for pattern_name, pattern in classifiers.items():
            for column_name in df.columns:
                match_samples = df[df[column_name].str.contains(pattern, na=False, regex=True)][column_name].head().tolist()
                if match_samples:
                    if column_name in results:
                        results[column_name][pattern_name] = match_samples
                    else:
                        results[column_name] = {pattern_name: match_samples}
                    #results[(column_name, pattern_name)] = match_samples
        return results

    def find_false_positives(self, paths, page_size=50):
        start_time = timer()
        output_file = config['scan_files']['output_file'].get()
        attempts = config['SMB_connection']['attempts'].get()
        paged_paths = [paths[i:i + page_size] for i in range(0, len(paths), page_size)]
        invalid_files = "zero_matches_{}".format(output_file)
        not_samples = FileUtils.read_json_file(invalid_files)
        if not not_samples:
            not_samples = []
        results_by_file = FileUtils.read_json_file(output_file)
        for k, p in enumerate(paged_paths):
            data = [find_samples_concurrently(name, classifiers, attempts, self.zip_paths, self.data_source_class)
                    for name in p]
            sample_result = dask.compute(data)[0]
            for aux in sample_result:
                if type(aux) is list:
                    for i in aux:
                        if 'samples' in i and 'path' in i:
                            path = i['path']
                            samples = i['samples']
                            results_by_file[path] = samples
                        elif 'path' in i:
                            not_samples.append(i['path'])
                elif 'samples' in aux and 'path' in aux:
                    path = aux['path']
                    samples = aux['samples']
                    results_by_file[path] = samples
                elif 'path' in aux:
                    not_samples.append(aux['path'])

            print("func=find_false_positives, duration={}, scanned_files={}, zero_matches_files={}".format(
                timer() - start_time, (1 + k) * len(p), len(not_samples)))
            column_names = {column_name for file_name, results in results_by_file.items() for column_name in results if
                            not column_name.isdigit()}
            results_by_file['column_names'] = list(column_names)
            with open(output_file, 'w') as fp:
                json.dump(results_by_file, fp, indent=4)
            if invalid_files:
                with open(invalid_files, 'w') as fp:
                    json.dump(not_samples, fp, indent=4)
        end_time = timer()
        duration_seconds = end_time - start_time
        print("func=find_false_positives, duration={}, scanned_files={}".format(duration_seconds, len(paths)))
        return results_by_file

    def find_sample_for_plain_text(self, service_name, path, classifiers):
        local_path = self.data_source_class.retrieve_text_file(os.path.join(service_name, path))
        samples = self.scan_plain_text(local_path, classifiers)
        if samples:
            # return {"path": path, "samples": samples}
            return {"path": "{}/{}".format(service_name, path), "samples": samples}
        else:
            return {"path": "{}/{}".format(service_name, path)}

    @classmethod
    def scan_plain_text(cls, path, classifiers):
        results_by_file = {}
        import copy

        classifiers_copy = copy.deepcopy(classifiers)
        print_log_progress = 1000000
        last_line = 0
        offset = 15
        counters = {k: 0 for k in classifiers_copy}

        try:
            with open(path) as infile:
                for i, line in enumerate(infile):
                    if i == 0:
                        headers = line
                    last_line = i
                    if not classifiers_copy:
                        break
                    for pattern_name, pattern in classifiers_copy.items():
                        match = re.search(pattern, line)
                        if match:
                            results_by_file[i] = results_by_file[i] if i in results_by_file else {}
                            results_by_file[i][pattern_name] = match.group()
                            index_of_match = line.find(match.group())
                            results_by_file[i]['value'] = line[index_of_match - offset:index_of_match + len(
                                match.group()) + offset]
                            possible_header = FileUtils.extract_possible_header(line, headers, pattern)
                            if possible_header:
                                if 'possible_headers' in results_by_file[i]:
                                    results_by_file[i]['possible_headers'] = results_by_file[i][
                                        'possible_headers'].append(possible_header)
                                else:
                                    results_by_file[i]['possible_headers'] = possible_header
                            counters[pattern_name] += 1
                            if counters[pattern_name] >= 5:
                                del classifiers_copy[pattern_name]
                                break
                    if i != 0 and i % print_log_progress == 0:
                        print("func=scan_plain_text, found_patterns={}, line={}".format(len(results_by_file.keys()),
                                                                                        last_line))
        except Exception as e:
            print("func=scan_plain_text, found_patterns={}, msg={}, line={}".format(len(results_by_file.keys()), e,
                                                                                    last_line))
        return results_by_file

    def fail_over_control(self, output_file, paths_to_scan):
        invalid_files = "zero_matches_{}".format(output_file)
        scanned_paths = FileUtils.read_json_file(output_file)
        scanned_paths_errors = FileUtils.read_json_file(output_file)
        # Delete Service Name
        # paths_to_scan = ['/'.join(path.split('/')[1:]) for path in paths_to_scan]
        scanned_paths_set = set(scanned_paths.keys()).union(set(scanned_paths_errors))
        unscanned_paths = set(paths_to_scan).difference(scanned_paths_set)
        print("func=fail_over_control, output_file: {}, paths_to_scan={}, scanned_paths={}, skipped_paths={}".format(
            output_file, len(paths_to_scan), len(scanned_paths), len(paths_to_scan) - len(unscanned_paths))
        )
        return list(unscanned_paths)

    @classmethod
    def scan_local_zip_file(cls, zip_object):
        samples_list = []
        for local_path, inside_path in zip(zip_object['local_paths'], zip_object['inside_paths']):
            samples = None
            if not os.path.isfile(local_path):
                print("func=scan_local_zip_file, msg=No such file or directory, path=path")
                continue

            if FileUtils.file_type(local_path) == 'txt':
                samples = cls.scan_plain_text(local_path, classifiers)
            elif FileUtils.file_type(local_path) == 'csv':
                df = csv_df(local_path)
                if not df.empty:
                    samples = cls.find_samples_for_data_frame(df, classifiers)
            if samples:
                # return {"path": path, "samples": samples}
                samples_list.append({"path": "{}/{}".format(zip_object["remote_path"], inside_path), "samples": samples})
            else:
                samples_list.append({"path": "{}/{}".format(zip_object["remote_path"], inside_path)})
        return samples_list

    def download_zip(self, zip_object):
        try:
            tmp_path = self.data_source_class.retrieve_file(zip_object["remote_path"])
            zip_object['local_paths'] = []
            with ZipFile(tmp_path, 'r') as zip:
                for inside_path in zip_object["inside_paths"]:
                    tmp = tempfile.NamedTemporaryFile(suffix=inside_path.replace('/', '_'))  # , dir='/tmp/oportun')
                    tmp.write(zip.read(inside_path))
                    zip_object['local_paths'].append(tmp.name)
        except Exception as e:
            print("func=download_zip, remote_path={}, msg={}".format(zip_object["remote_path"], e))
        return zip_object

    def process(self):
        paths = self.full_paths[self.offset:self.limit]
        paths = file_processor.fail_over_control(output_file, paths)
        paths = self.data_source_class().sort_files_from_server(paths)
        _ = file_processor.find_false_positives(paths, page_size=page_size)
        self.data_source_class.close()

@dask.delayed
def find_samples_concurrently(full_path, classifiers, attempts, zip_files, data_source_class):
    service_name, path = data_source_class().split_path(full_path)
    if FileUtils.file_type(path) == 'txt' or FileUtils.file_type(path) == 'log':
        return FileProcessor.find_sample_for_plain_text(service_name, path, classifiers)
    elif FileUtils.file_type(path) == 'csv':
        df, _ = data_source_class().retrieve_csv_file(full_path, attempts=attempts)
        if not df.empty:
            samples = FileProcessor.find_samples_for_data_frame(df, classifiers)
            if samples:
                #return {"path": path, "samples": samples}
                return {"path": "{}/{}".format(service_name, path), "samples": samples}
            else:
                return {"path": "{}/{}".format(service_name, path)}
        else:
            return data_source_class().find_sample_for_plain_text(service_name, path, classifiers)
    elif FileUtils.file_type(path) == 'zip':
        zip_object = {
            "remote_path": full_path,
            "inside_paths": zip_files[full_path]
        }
        zip_object = FileProcessor.download_zip(zip_object)
        return FileProcessor.scan_local_zip_file(zip_object)
    elif FileUtils.file_type(path) == 'sas7bdat':
        df = data_source_class().retrieve_sas_file(full_path, attempts=attempts)
        if not df.empty:
            samples = FileProcessor.find_samples_for_data_frame(df, classifiers)
            if samples:
                #return {"path": path, "samples": samples}
                return {"path": "{}/{}".format(service_name, path), "samples": samples}

    return {}


class FileUtils:
    @classmethod
    def split_smb_path(cls, path):
        return path.split('/')[0], '/'.join(path.split('/')[1:])

    @classmethod
    def file_type(cls, path):
        return path.split('.')[-1]

    @classmethod
    def get_paths_from_big_id_report(cls, file_name):
        df = pd.read_csv(file_name, dtype=str)
        paths = df['Full Object Name'].tolist()

        paths = [i for i in paths if cls.file_type(i) not in INVALID_FILES]
        return paths

    @classmethod
    def split_zip_and_files(cls, paths):
        from collections import defaultdict
        zip_paths = defaultdict(list)
        file_paths = []
        for i in paths:
            if i.find('.zip') > -1:
                zip_paths[i.split('.zip')[0] + '.zip'].append(i.split('.zip')[1].lstrip('/'))
            else:
                file_paths.append(i)
        return dict(zip_paths), file_paths

    @classmethod
    def read_json_file(cls, file_name):
        exist = os.path.isfile(file_name)
        if not exist:
            return {}
        with open(file_name) as f:
            scanned_paths = json.load(f)
        return scanned_paths

    @classmethod
    def split_csv_row(cls, line, delimiter):
        f = StringIO(line)
        reader = csv.reader(f, delimiter=delimiter)
        fields = list(reader)
        if len(fields) == 1:
            return fields[0]

    @classmethod
    def extract_possible_header(cls, line, headers, pattern):
        possible_delimiters = {k: v for k, v in Counter(line).items() if re.match('\W', k)}
        possible_delimiters = [i for i in possible_delimiters.keys() if i in [',', '\t', '|', ';']]
        if possible_delimiters:
            delimiter = possible_delimiters[0]
            index = [i for i, v in enumerate(FileUtils.split_csv_row(line, delimiter))
                     if re.search(pattern, v) and i < len(headers.split(delimiter))]
            if index:
                return [headers.split(delimiter)[i] for i in index]
        return []


def find_false_positives_zip_files(paths, page_size=50):
    pass

if __name__ == "__main__":

    config = get_config()
    classifiers = config['BigID']['classifiers'].get()
    page_size = config['scan_files']['pagination']['page_size'].get()
    offset = config['scan_files']['pagination']['offset'].get()
    limit = config['scan_files']['pagination']['limit'].get()
    file_name = config['BigID']['report_file'].get()
    data_source_type = config['BigID']['data_source_type'].get()
    output_file = config['scan_files']['output_file'].get()
    max_file_size = config['S3_connection']['max_file_size'].get()
    #file_result = scan_plain_text('/Users/oswaldo.cruz/Downloads/alltrust1.csv', classifiers)
    paths = FileUtils.get_paths_from_big_id_report(file_name)

    if data_source_type == 's3':
        data_source = S3Handler#(max_file_size)
    else:
        data_source = SMBHandler
    file_processor = FileProcessor(paths, offset, limit, data_source)
    _ = file_processor.process()


