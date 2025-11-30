import json
import os
from urllib.parse import unquote_plus

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# ===== ENV VARS =====
ES_ENDPOINT = os.environ["ES_ENDPOINT"]          # same as search_photos
ES_INDEX = os.environ.get("ES_INDEX", "photos")  # make sure this is "photos"
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# ===== CLIENTS =====
session = boto3.Session()
credentials = session.get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    AWS_REGION,
    "es",
    session_token=credentials.token,
)

es = OpenSearch(
    hosts=[{"host": ES_ENDPOINT, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
)

rek = boto3.client("rekognition")
s3 = boto3.client("s3")


def lambda_handler(event, context):
    print("EVENT:", json.dumps(event))

    records = event.get("Records", [])
    if not records:
        print("No Records in event")
        return {"statusCode": 400, "body": "No S3 records"}

    for record in records:
        bucket = record["s3"]["bucket"]["name"]
        raw_key = record["s3"]["object"]["key"]
        key = unquote_plus(raw_key)  # <-- DECODE URL-ENCODED KEY
        size = record["s3"]["object"].get("size", 0)

        print(f"Processing object s3://{bucket}/{key} (raw_key={raw_key}, size={size})")

        # Skip empty files
        if size == 0:
            print("Object is empty, skipping Rekognition and indexing")
            continue

        # Optional: log Content-Type
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            content_type = head.get("ContentType")
            print(f"Content-Type from S3: {content_type}")
        except Exception as e:
            print(f"ERROR getting head_object for {key}: {e}")
            content_type = None

        # 1) Call Rekognition
        try:
            rek_resp = rek.detect_labels(
                Image={"S3Object": {"Bucket": bucket, "Name": key}},
                MaxLabels=10,
                MinConfidence=75,
            )
        except Exception as e:
            print(f"ERROR calling Rekognition for {key}: {e}")
            continue

        labels = [lab["Name"] for lab in rek_resp.get("Labels", [])]
        print(f"Detected labels for {key}: {labels}")

        # 2) Build doc
        doc = {
            "bucket": bucket,
            "objectKey": key,
            "labels": labels,
        }
        print("Indexing doc into ES:", doc)

        # 3) Index into OpenSearch
        try:
            es_resp = es.index(index=ES_INDEX, id=key, body=doc)
            print(f"ES index response for {key}: {es_resp}")
        except Exception as e:
            print(f"ERROR indexing into ES for key {key}: {e}")

    return {"statusCode": 200, "body": "OK"}