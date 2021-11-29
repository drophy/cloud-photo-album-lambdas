import boto3
import os
from urllib.parse import unquote_plus

thumbnail_bucket = os.environ.get('thumbnail_bucket')

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print('Reached lambda_handler.')
    for record in event['Records']:
        key = unquote_plus(record['s3']['object']['key'])
        print(f'Deleting {key} from {thumbnail_bucket}')
        s3_client.delete_object(Bucket=thumbnail_bucket, Key=key)
        try:
            s3_client.delete_object(thumbnail_bucket, key)
        except Exception as e:
            print('An error ocurred. Thumbnail was not deleted.')
            print(e)
            raise e
    print('Thumbnail deleted.')