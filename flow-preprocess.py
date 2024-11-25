from prefect import flow, task
from prefect.blocks.system import Secret

import os
import boto3
import pandas as pd
from io import StringIO
from charset_normalizer import detect

key_block = Secret.load("aws-secret-access-key")
key_id_block = Secret.load("aws-access-key-id")
client = boto3.client(
    "s3",
    aws_access_key_id=key_id_block.get(),
    aws_secret_access_key=key_block.get(),
    region_name="ap-northeast-1",
)


def get_file_path(file_path_name=None):
    if file_path_name is None:
        return os.path.dirname(__file__)

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
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()

    detected = detect(body)
    encoding = detected["encoding"]
    if encoding:
        decoded_body = body.decode(encoding)
    else:
        decoded_body = body.decode("utf-8", errors="replace")

    return decoded_body


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
def preprocess_csv(bucket: str, key: str):
    body = get_data(bucket, key)

    # skip if the row is bad
    df = pd.read_csv(
        StringIO(body),
        low_memory=False,
        on_bad_lines="warn",
    )

    df = rename_columns(df)
    print(df.head())
    upload_data(df, bucket, f"lake/{key}")
    print("Done")


if __name__ == "__main__":
    bucket = "cdkstack-csvbucketadda1e74-xwwsrvxkbhl4"
    key = "2024-11-24/orders.csv"
    preprocess_csv(bucket, key)
