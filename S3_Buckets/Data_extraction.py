import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def find_buckets_complete(csv_name):
    buckets_matches = pd.read_csv(csv_name)
    prod_buckets_with_info = buckets_matches.dropna(subset=['technical_owner_prod', 'business_owner_prod'], how='all')
    print(prod_buckets_with_info)

    return prod_buckets_with_info


def save_to_csv(df, name):
    df.to_csv(name, mode='w', encoding='utf-8', index=False)


if __name__ == '__main__':
    csv_file_name = 'matches.csv'
    buckets_complete = find_buckets_complete(csv_file_name)
    save_to_csv(buckets_complete, 'Empty fields removed.csv')
