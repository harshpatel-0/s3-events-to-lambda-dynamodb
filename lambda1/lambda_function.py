# from Github!

import json
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    record = event['Records'][0]
    s3_bucket_name = record['s3']['bucket']['name']
    s3_object_key = record['s3']['object']['key']
    s3_object_size = record['s3']['object']['size']
    s3_object_etag = record['s3']['object']['eTag']
    s3_object_upload_date = record['eventTime']

    # Generate the ARN for the bucket
    bucket_arn = f"arn:aws:s3:::{s3_bucket_name}"

    # Construct the ARN for the object
    object_arn = f"{bucket_arn}/{s3_object_key}"

    # Table name from DynamoDB
    table_name = 'hw05-file-records'
    primary_key_name = 'fileName'
    
    # Connect to the DynamoDB table
    table = dynamodb.Table(table_name)

    # Construct the key with only the primary key
    key = {
        primary_key_name: s3_object_key
    }

    # Attempt to get the item from DynamoDB
    try:
        response = table.get_item(Key=key)
    except ClientError as e:
        print(e.response['Error']['Message'])
        raise
    else:
        item = response.get('Item')

        # If the item does not exist, insert it
        if not item:
            print("Inserting new item...")
            table.put_item(
                Item={
                    primary_key_name: s3_object_key,
                    'fileSize': s3_object_size,
                    'fileARN': object_arn,  # Use the constructed object ARN here
                    'eTag': s3_object_etag,
                    'uploadDate': s3_object_upload_date  # Include the upload date
                }
            )
        else:
            # If the item exists, update it
            print("Updating existing item...")
            table.update_item(
                Key=key,
                UpdateExpression='SET fileSize = :size, fileARN = :arn, eTag = :etag, uploadDate = :upldate',
                ExpressionAttributeValues={
                    ':size': s3_object_size,
                    ':arn': object_arn,  # Use the constructed object ARN here
                    ':etag': s3_object_etag,
                    ':upldate': s3_object_upload_date  # Update the upload date
                }
            )

    return {
        'statusCode': 200,
        'body': json.dumps('DynamoDB record insert/update successful!')
    }
