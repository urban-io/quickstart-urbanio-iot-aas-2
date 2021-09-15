import os
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper()))

class Sqs:
    def __init__(self, queue_url=os.environ['ASSETS_TO_UPDATE_QUEUE_URL'], batch_size=int(os.environ.get("BATCH_SIZE", 10)), aws_region=os.environ.get("AWS_REGION")):
        self.queue_url =queue_url
        self.batch_size =batch_size
        self.aws_region = aws_region
        self.sqs_client = boto3.client('sqs', region_name=self.aws_region)

    def send_messages(self, messages):
        for i in range(0, len(messages), self.batch_size):
            messages_chunk = list(messages)[i:i + self.batch_size]   
            entries = [{'Id': str(idx), 'MessageBody': json.dumps(msg), 'MessageGroupId': 'lifecycle_events'} for idx,msg in enumerate(messages_chunk)] 

            response = self.sqs_client.send_message_batch(
                QueueUrl=self.queue_url,
                Entries=entries
            )

            logger.info(f"Successfully {response}")