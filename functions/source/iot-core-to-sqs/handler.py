import boto3
import os
import json
import logging
import uuid
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper()))

def handler(event, context):    
    sqs_client = boto3.client('sqs',region_name=os.environ.get("AWS_REGION"))
    queue_url = os.environ['LIFECICLE_EVENTS_QUEUE_URL']

    # Send category_updated event before device_updated one, so that the category can be created in case it doesn't exist
    if event['type'] == 'lifecycle' and event['reading']['et'] == 'device_updated' and event['reading']['device_category'] is not None:
        categoty_event =    {
            'type': "lifecycle",
            'reading': {
                'et': "category_updated",
                'id': f"{event['reading']['device_category']}-{event['metadata']['ref']['l']}",
                'name': event['reading']['device_category'],
            },
            'metadata': {
                'ref': {
                    'l': event['metadata']['ref']['l'],
                }
            }
        }
        
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(categoty_event),
            MessageGroupId='lifecycle_events'
        )

        logger.info(f"Successfully sent category event {response}")

    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event),
        MessageGroupId='lifecycle_events'
    )

    logger.info(f"Successfully sent {response}")