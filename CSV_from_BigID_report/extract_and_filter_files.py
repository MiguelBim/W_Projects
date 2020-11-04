import pandas as pd


def extract_csv_txt_files(report_name):

    report_df = pd.read_csv(report_name)
    filtered_df = report_df[report_df['Full Object Name'].str.contains('.csv|.CSV|.txt|.TXT')].reset_index(drop=True)
    final_df = filtered_df.sort_values(by=['Full Object Name'])
    print(final_df)
    final_df.to_csv('Filtered_Objects.csv', index=False)

    return


def extract_excel_files(report_name):

    report_df = pd.read_csv(report_name)
    filtered_df = report_df[report_df['Full Object Name'].str.contains('.xlsx|.XLSX')].reset_index(drop=True)
    final_df = filtered_df.sort_values(by=['Full Object Name'])
    print(final_df)
    final_df.to_csv('Filtered_Objects.csv', index=False)

    return


def extract_paths_from_pattern(report_name, pattern):

    report_df = pd.read_csv(report_name)
    filtered_df = report_df[report_df['Full Object Name'].str.contains(pattern)].reset_index(drop=True)
    final_df = filtered_df.sort_values(by=['Full Object Name'])
    print(final_df)
    final_df.to_csv('Pattern_Objects.csv', index=False)

    return


if __name__ == '__main__':
    report_name = 'Objects (Whole).csv'
    # pattern = 'fraudcheck'

    #EXTRACTION PHASE
    # extract_csv_txt_files(report_name)
    extract_excel_files(report_name)
    # extract_paths_from_pattern(report_name, pattern)
