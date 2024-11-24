import os
from prefect import flow, task
import boto3
import pandas as pd
from io import StringIO

client = boto3.client('s3')

def get_file_path(file_path_name=None):
    return os.path.join(os.path.dirname(__file__), file_path_name)

def rename_column(df, renamed_from, renamed_to):
    """
    データフレームdfについて
    renamed_from というカラム名を renamed_to というカラム名に変更
    """
    df.rename(columns={renamed_from: renamed_to}, inplace=True)

@task(log_prints=True)
def get_data(bucket: str, key: str):
    print(f"Getting data from {bucket}/{key}...")
    response = client.get_object( Bucket=bucket, Key=key)
    body = response['Body'].read().decode('CP932')
    # body = response['Body'].read().decode('utf-8')
    return body

@task(log_prints=True)
def rename_columns(df: pd.DataFrame):
    column_list_path = get_file_path("columnList/order_column_list")
    with open(column_list_path, "r") as f:
        for line in f:
            split_line = line.strip().split(",")
            renamed_from = split_line[0]
            renamed_to = split_line[1]
            rename_column(df, renamed_from, renamed_to)

    return df

@task(log_prints=True)
def upload_data(df: pd.DataFrame, bucket: str, key: str):
    print(f"Uploading data to {bucket}/{key}...")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

@flow
def preprocess_csv(backet: str, key: str):
    data = get_data(bucket, key)
    df = pd.read_csv(StringIO(data), low_memory=False)
    df = rename_columns(df)
    print(df.head())
    upload_data(df, bucket, f"lake/{key}")
    print('Done')

if __name__ == "__main__":
    bucket = 'cdkstack-csvbucketadda1e74-xwwsrvxkbhl4'
    key = '2024-11-24/orders.csv'
    preprocess_csv(bucket, key)
