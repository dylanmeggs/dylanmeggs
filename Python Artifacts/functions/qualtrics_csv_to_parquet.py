import sys
import boto3
import s3fs
import re
import pandas as pd
from datetime import datetime

# Initialize S3 client
s3 = boto3.client('s3')

# S3 bucket and object names
input_bucket_name = 'qualtrics-da-focus-test'
output_bucket_name = 'aws-data-lake-prod'
input_directory = 'input-files'
output_directory = 'qualtrics_team_parquet'

# Format column names
def rename_columns(df):
    return df.rename(columns=lambda x: re.sub(r'_+', '_', 
                                              re.sub(r'[^A-Za-z0-9_]+', '', 
                                                     x.strip().replace(" ", "_").replace("-", "_")).lower()), 
                     inplace=True)

# Get the most recently uploaded csv file in the input_table object
def get_most_recent_s3_object(bucket_name, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator( "list_objects_v2" )
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    latest = None
    for page in page_iterator:
        if "Contents" in page:
            latest2 = max(page['Contents'], key=lambda x: x['LastModified'])
            if latest is None or latest2['LastModified'] > latest['LastModified']:
                latest = latest2
    return latest


# Get latest filepath
latest = get_most_recent_s3_object(input_bucket_name, input_directory) 
file_path = latest['Key']

# Read the csv file from S3 into a pandas dataframe
df = pd.read_csv(f's3://{input_bucket_name}/{file_path}')

# Delete the 'irrelevant' column ingested from Qualtrics metadata
df = df.drop(columns=['Q_DataPolicyViolations'])

# Delete the 1st and 2nd row (column header not considered a row in this context)
df = df.drop([0, 1])

rename_columns(df)


# Write the output to S3
output_file_name = f'{datetime.now().strftime("%Y%m%d-%H%M%S")}_output.csv'
df.to_parquet(f's3://{output_bucket_name}/{output_directory}/{output_file_name}', index=False)