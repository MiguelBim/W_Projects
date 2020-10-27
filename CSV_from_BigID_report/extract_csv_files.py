import pandas as pd


def extract_csv_txt_files(report_name):

    report_df = pd.read_csv(report_name)
    filtered_df = report_df[report_df['Full Object Name'].str.contains('.csv|.CSV|.txt|.TXT')].reset_index(drop=True)
    print(filtered_df)
    filtered_df.to_csv('Filtered_Objects.csv', index=False)

    return


if __name__ == '__main__':
    report_name = 'Objects.csv'
    extract_csv_txt_files(report_name)