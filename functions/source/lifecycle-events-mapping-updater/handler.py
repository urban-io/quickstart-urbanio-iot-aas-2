import boto3
import os
import json
import logging
import uuid
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper()))


def save_mappings(mappings, table_name=os.environ['DYNAMO_EVENT_MAPPING_TABLE_NAME'], region_name=os.environ.get("AWS_REGION")):
    dynamodb = boto3.resource('dynamodb', region_name=region_name)
    table = dynamodb.Table(table_name)

    for mapping in mappings:
        logger.info(f"Saving mapping: {mapping}")
        table.put_item(Item=mapping)

def handler(event, context):
    s3_client = boto3.client('s3', region_name=os.environ.get("AWS_REGION"))

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        tmpkey = key.replace('/', '')
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
        s3_client.download_file(bucket, key, download_path)

        with open(download_path) as json_file:
            data = json.load(json_file)
            save_mappings(data)