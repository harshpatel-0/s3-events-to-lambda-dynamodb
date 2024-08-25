import boto3
import pytest
from botocore.exceptions import ClientError
import time
import os
import csv
import io

@pytest.fixture(scope="module")
def s3_client():
    return boto3.client('s3', region_name='us-east-1')

@pytest.fixture(scope="module")
def dynamodb_client():
    return boto3.client('dynamodb', region_name='us-east-1')

def test_upload_to_s3_triggers_lambda(s3_client, dynamodb_client):
    bucket_name = "hw05-harsh"
    file_path = 'hw05/demodemo.txt'
    file_key = os.path.basename(file_path)
    table_name = 'hw05-file-records'
    primary_key_name = 'fileName'

    with open(file_path, 'rb') as file:
        file_content = file.read()

    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=file_content)
    time.sleep(15)  

    object_arn = f"arn:aws:s3:::{bucket_name}/{file_key}"

    try:
        response = dynamodb_client.get_item(
            TableName=table_name,
            Key={primary_key_name: {'S': file_key}}
        )
    except ClientError as e:
        pytest.fail(f"DynamoDB error: {e.response['Error']['Message']}")

    assert 'Item' in response, "Item not found in DynamoDB"
    assert response['Item']['fileARN']['S'] == object_arn, "ARN mismatch"
    assert int(response['Item']['fileSize']['N']) == len(file_content), "File size mismatch"
    assert 'uploadDate' in response['Item'], "Upload date not present in item"

def test_delete_triggers_csv_creation(s3_client, dynamodb_client):
    bucket_name = "hw05-harsh"
    file_key = "test_delete_file.txt"
    file_content = "Test delete content"
    table_name = 'hw05-file-records'
    csv_file_key = 'bucket_contents.csv'

    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=file_content)
    s3_client.delete_object(Bucket=bucket_name, Key=file_key)
    time.sleep(30) 

    try:
        csv_object = s3_client.get_object(Bucket=bucket_name, Key=csv_file_key)
        csv_content = csv_object['Body'].read().decode('utf-8')
        csv_reader = csv.reader(io.StringIO(csv_content))
        csv_rows = list(csv_reader)
    except ClientError as e:
        pytest.fail(f"S3 error: {e.response['Error']['Message']}")

    assert len(csv_rows) > 1, "CSV file does not contain expected data"

    try:
        response = dynamodb_client.get_item(
            TableName=table_name,
            Key={'fileName': {'S': csv_file_key}}
        )
    except ClientError as e:
        pytest.fail(f"DynamoDB error: {e.response['Error']['Message']}")

    assert 'Item' in response, "CSV file record not found in DynamoDB"
