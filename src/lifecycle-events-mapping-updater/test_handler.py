import os
import uuid
import boto3
from moto import mock_s3
from moto import mock_dynamodb2
from mock import patch

@mock_s3
@mock_dynamodb2
def test_handler():
    aws_region = 'us-east-1'
    table_name = "test"

    bucket = 'mybucket'
    key = 'sitewise/test/mappings.json'
    test_file_path = 'data/mappings.json'
    s3 = boto3.client('s3', region_name=aws_region)
    s3.create_bucket(Bucket=bucket)   
    s3.put_object(Bucket=bucket, Key=key , Body=open(test_file_path).read())

    dynamo = boto3.client('dynamodb', region_name=aws_region)
    dynamo.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            { 'AttributeName': 'Model', 'AttributeType': 'S' }
        ],
        KeySchema=[
            { 'KeyType': 'HASH', 'AttributeName': 'Model' }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )    

    event = {
        "Records": [
            {
              "s3": {
                "bucket": {
                  "name": bucket,
                },
                "object": {
                  "key": key
                }
              }
            }
        ]
    }

    context = {}

    with patch.dict(os.environ, {"DYNAMO_EVENT_MAPPING_TABLE_NAME": table_name, "AWS_REGION": aws_region }):
        from handler import handler
        handler(event, context)

