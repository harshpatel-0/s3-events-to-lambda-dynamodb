import boto3
import csv
from io import StringIO

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = 'hw05-harsh'
    
    deleted_object_key = event['Records'][0]['s3']['object']['key']

    response = s3_client.list_objects_v2(Bucket=bucket_name)
    objects = response.get('Contents', [])
    
    csv_file = StringIO()
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['FileName', 'LastModified', 'DeletedObject'])

    csv_writer.writerow([deleted_object_key, 'Deleted', 'Yes'])

    for obj in objects:
        csv_writer.writerow([obj['Key'], obj['LastModified'], 'No'])
    
    csv_file.seek(0)
    
    s3_client.put_object(Bucket=bucket_name, Key='bucket_contents.csv', Body=csv_file.getvalue())
    
    return {
        'statusCode': 200,
        'body': 'CSV file generated and uploaded successfully.'
    }
