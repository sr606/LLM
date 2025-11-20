"""

Lightweight I/O helpers for S3 and environment wiring.

Use in Lambda or local runs.

"""

import os
import io
import json
import boto3
 
def get_s3_client():

    region = os.getenv("AWS_REGION", "us-east-1")

    return boto3.client("s3", region_name=region)
 
def s3_put_bytes(bucket: str, key: str, data: bytes, content_type: str):

    s3 = get_s3_client()

    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
 
def s3_get_bytes(bucket: str, key: str) -> bytes:

    s3 = get_s3_client()

    obj = s3.get_object(Bucket=bucket, Key=key)

    return obj["Body"].read()
 
def s3_put_json(bucket: str, key: str, obj: dict):

    s3_put_bytes(bucket, key, json.dumps(obj, indent=2).encode("utf-8"), "application/json")

 