import os
import uuid
import boto3
from moto import mock_s3
from moto import mock_sqs
from mock import patch

@mock_sqs
def test_send_sqs_messages():
    aws_region = 'us-east-1'

    messages = [{'id': idx, 'msg': str(uuid.uuid4())} for idx in range(25)]

    sqs = boto3.client('sqs', region_name=aws_region)
    queue = sqs.create_queue(QueueName='test-queue.fifo', Attributes={"FifoQueue": "true"})

    with patch.dict(os.environ, {"LIFECICLE_EVENTS_QUEUE_URL": queue["QueueUrl"], "AWS_REGION": aws_region, "DYNAMO_INTEGRATION_POINTS_TABLE_NAME": "test" }):
        from handler import send_sqs_messages
        send_sqs_messages(messages=messages)

    response = sqs.get_queue_attributes(QueueUrl=queue["QueueUrl"])

    assert int(response["Attributes"]["ApproximateNumberOfMessages"]) == 25


@mock_s3
@mock_sqs
def test_handler():
    aws_region = 'us-east-1'

    bucket = 'mybucket'
    key = 'sitewise/test/test.json'
    test_file_path = 'data/test.json'
    s3 = boto3.client('s3', region_name=aws_region)
    s3.create_bucket(Bucket=bucket)   
    
    s3.put_object(Bucket=bucket, Key=key , Body=open(test_file_path).read())

    sqs = boto3.client('sqs', region_name=aws_region)
    queue = sqs.create_queue(QueueName='test-queue.fifo', Attributes={"FifoQueue": "true"})

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

    with patch.dict(os.environ, {"LIFECICLE_EVENTS_QUEUE_URL": queue["QueueUrl"], "AWS_REGION": aws_region, "DYNAMO_INTEGRATION_POINTS_TABLE_NAME": "test" }):
        from handler import handler
        handler(event, context)

    response = sqs.get_queue_attributes(QueueUrl=queue["QueueUrl"])

    assert int(response["Attributes"]["ApproximateNumberOfMessages"]) == 5

