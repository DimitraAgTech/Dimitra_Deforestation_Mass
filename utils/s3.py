import boto3

from config import (AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY,
                    S3_RESULT_BUCKET)

s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)


def download_s3_object(object_name, local_path):
    s3_client.download_file(S3_RESULT_BUCKET, object_name, local_path)


def upload_s3_object(local_path, object_name):
    s3_client.upload_file(local_path, S3_RESULT_BUCKET, object_name)
