import pandas as pd
import confuse
import json
from concurrent_processing import SMBHandler
import dask
from timeit import default_timer as timer
import re
import os
from collections import Counter


def get_config():
    import argparse

    parser = argparse.ArgumentParser(description='Validator or patterns in csv files')
    parser.add_argument('--report_file', help='Inventory Report provided by BigID', dest='BigID.report_file')
    parser.add_argument('--output_file', help='File name to dump the output', dest='scan_files.output_file')
    parser.add_argument('--offset', help='Position to start the scanning', dest='scan_files.pagination.offset', type=int)
    parser.add_argument('--limit', help='Position to finish the scanning', dest='scan_files.pagination.limit', type=int)
    parser.add_argument('--page_size', help='Number of parallel downloads', dest='scan_files.pagination.page_size', type=int)
    args = parser.parse_args()

    config = confuse.Configuration("FindFalsePositves", __name__)
    config.set_file('config.yaml')

    config.set_args(args, dots=True)
    return config


def find_samples(df, classifiers):
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


@dask.delayed
def find_samples_concurrently(service_name, path, classifiers, attempts):
    if path.find('.txt') == len(path) - 4:
        return find_sample_for_plain_text(service_name, path, classifiers)
    elif path.find('.csv') == len(path) - 4:
        smb_handler = SMBHandler()
        df, _ = smb_handler.retrieve_csv_from_smb(service_name, path, attempts=attempts)
        if not df.empty:
            samples = find_samples(df, classifiers)
            if samples:
                #return {"path": path, "samples": samples}
                return {"path": "{}/{}".format(service_name, path), "samples": samples}
            else:
                {"path": "{}/{}".format(service_name, path)}
        else:
            return find_sample_for_plain_text(service_name, path, classifiers)
    return {}


def find_false_positives(paths, page_size=50):
    start_time = timer()
    output_file = config['scan_files']['output_file'].get()
    attempts = config['SMB_connection']['attempts'].get()
    paged_paths = [paths[i:i+page_size] for i in range(0, len(paths), page_size)]
    invalid_files = "zero_matches_{}".format(output_file)
    not_samples = read_json_file(invalid_files)
    if not not_samples:
        not_samples = []
    results_by_file = read_json_file(output_file)
    for k, p in enumerate(paged_paths):
        data = [find_samples_concurrently(name.split('/')[0], '/'.join(name.split('/')[1:]), classifiers, attempts) for name in p]
        sample_result = dask.compute(data)[0]
        for aux in sample_result:
            if 'samples' in aux and 'path' in aux:
                path = aux['path']
                samples = aux['samples']
                results_by_file[path] = samples
            elif 'path' in aux:
                not_samples.append(aux['path'])

        print("func=find_false_positives, duration={}, scanned_files={}, zero_matches_files={}".format(
            timer() - start_time, (1+k) * len(p), len(not_samples)))

        with open(output_file, 'w') as fp:
            json.dump(results_by_file, fp, indent=4)
        with open(invalid_files, 'w') as fp:
            json.dump(not_samples, fp, indent=4)
    end_time = timer()
    duration_seconds = end_time - start_time
    print("func=find_false_positives, duration={}, scanned_files={}".format(duration_seconds, len(paths)))
    return results_by_file


def extract_possible_header(line, headers, pattern):
    possible_delimiters = {k: v for k, v in Counter(line).items() if not re.match(pattern, k)}
    possible_delimiters = [i for i in possible_delimiters.keys() if re.match('\W', i) and not re.match('\s|\.', i)]
    if possible_delimiters:
        delimiter = possible_delimiters[0]
        index = [i for i, v in enumerate(line.split(delimiter)) if re.match(pattern, v) and i < len(headers.split('|'))]
        if index:
            return [headers.split('|')[i] for i in index]
    return []


def find_sample_for_plain_text(service_name, path, classifiers):
    local_path = SMBHandler().retrieve_text_file_from_smb(service_name, path)
    samples = scan_plain_text(local_path[0], classifiers)
    if samples:
        #return {"path": path, "samples": samples}
        return {"path": "{}/{}".format(service_name, path), "samples": samples}
    else:
        return {"path": "{}/{}".format(service_name, path)}


def scan_plain_text(path, classifiers):
    results_by_file = {}
    import copy

    classifiers_copy = copy.deepcopy(classifiers)
    print_log_progress = 100000
    last_line = 0
    offset = 5
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
                        results_by_file[i]['value'] = line[index_of_match-offset:index_of_match+len(match.group())+offset]
                        possible_header = extract_possible_header(line, headers, pattern)
                        if possible_header:
                            if 'possible_headers' in results_by_file[i]:
                                results_by_file[i]['possible_headers'] = results_by_file[i]['possible_headers'].append(possible_header)
                            else:
                                results_by_file[i]['possible_headers'] = possible_header
                        counters[pattern_name] += 1
                        if counters[pattern_name] >= 5:
                            del classifiers_copy[pattern_name]
                            break
                if i != 0 and i % print_log_progress == 0:
                    print("func=scan_plain_text, found_patterns={}, line={}".format(len(results_by_file.keys()), last_line))
    except Exception as e:
        print("func=scan_plain_text, found_patterns={}, msg={}, line={}".format(len(results_by_file.keys()), e, last_line))
    return results_by_file

def get_paths_from_big_id_report(file_name):
    df = pd.read_csv(file_name, dtype=str)
    paths = df['Full Object Name'].tolist()
    return paths


def read_json_file(file_name):
    exist = os.path.isfile(file_name)
    if not exist:
        return {}
    with open(file_name) as f:
        scanned_paths = json.load(f)
    return scanned_paths


def fail_over_control(output_file, paths_to_scan):
    invalid_files = "zero_matches_{}".format(output_file)
    scanned_paths = read_json_file(output_file)
    scanned_paths_errors = read_json_file(output_file)
    # Delete Service Name
    #paths_to_scan = ['/'.join(path.split('/')[1:]) for path in paths_to_scan]
    scanned_paths_set = set(scanned_paths.keys()).union(set(scanned_paths_errors))
    unscanned_paths = set(paths_to_scan).difference(scanned_paths_set)
    print("func=fail_over_control, output_file: {}, paths_to_scan={}, scanned_paths={}, skipped_paths={}".format(
        output_file, len(paths_to_scan), len(scanned_paths), len(paths_to_scan)-len(unscanned_paths))
    )
    return list(unscanned_paths)


def split_smb_path(path):
    return path.split('/')[0], '/'.join(path.split('/')[1:])


if __name__ == "__main__":

    config = get_config()
    classifiers = config['BigID']['classifiers'].get()
    page_size = config['scan_files']['pagination']['page_size'].get()
    offset = config['scan_files']['pagination']['offset'].get()
    limit = config['scan_files']['pagination']['limit'].get()
    file_name = config['BigID']['report_file'].get()
    output_file = config['scan_files']['output_file'].get()

    paths = get_paths_from_big_id_report(file_name)
    paths = paths[offset:limit]
    paths = fail_over_control(output_file, paths)
    paths = SMBHandler().sort_files_from_server(paths)
    _ = find_false_positives(paths, page_size=page_size)
    len(_)


