import boto3
import os
import json
import logging
import uuid
from urllib.parse import unquote_plus
from sitewise_integration_points import SitewiseIntegrationPoints

logger = logging.getLogger()
logger.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper()))

integration_points_dynamo = SitewiseIntegrationPoints(os.environ['DYNAMO_INTEGRATION_POINTS_TABLE_NAME'], os.environ['AWS_REGION'])


def cache_integration_points(messages):
    operators = [message for message in messages if message.get('type') == 'lifecycle' and message.get('reading', {}).get('et') == 'operator_updated']

    for operator in operators:
        integration_points_dynamo.save(operator)


def send_sqs_messages(messages, queue_url=os.environ['LIFECICLE_EVENTS_QUEUE_URL'], batch_size=int(os.environ.get("BATCH_SIZE", 10))):
    sqs_client = boto3.client('sqs', region_name=os.environ.get("AWS_REGION"))

    for i in range(0, len(messages), batch_size):
        messages_chunk = list(messages)[i:i + batch_size]   
        entries = [{'Id': str(idx), 'MessageBody': json.dumps(msg), 'MessageGroupId': 'lifecycle_events'} for idx,msg in enumerate(messages_chunk)] 

        response = sqs_client.send_message_batch(
            QueueUrl=queue_url,
            Entries=entries
        )

        logger.info(f"Successfully {response}")


def handler(event, context):
    sqs_messages = []

    s3_client = boto3.client('s3', region_name=os.environ.get("AWS_REGION"))

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        tmpkey = key.replace('/', '')
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
        s3_client.download_file(bucket, key, download_path)

        with open(download_path) as json_file:
            data = json.load(json_file)

            sqs_messages += data.get('assetModels', [])
            sqs_messages += data.get('assets', [])

    cache_integration_points(sqs_messages)
    send_sqs_messages(sqs_messages)


if __name__ == '__main__':
    event = {
      "Records": [
        {
          "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": "testConfigRule",
            "bucket": {
              "name": "ricostg-uploads-sydney",
              "ownerIdentity": {
                "principalId": "EXAMPLE"
              },
              "arn": "arn:aws:s3:::example-bucket"
            },
            "object": {
              "key": "sitewise/test/Operator_mkGzglkBFEl7o8PCkRgx9A.json",
              "size": 1024,
              "eTag": "0123456789abcdef0123456789abcdef",
              "sequencer": "0A1B2C3D4E5F678901"
            }
          }
        }
      ]
    }

    handler(event, None)

