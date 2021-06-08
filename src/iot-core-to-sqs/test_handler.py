import os
import boto3
from moto import mock_sqs
from mock import patch

@mock_sqs
def test_handler():
    aws_region = 'us-east-1'

    sqs = boto3.client('sqs', region_name='us-east-1')
    queue = sqs.create_queue(QueueName='test-queue.fifo', Attributes={"FifoQueue": "true"})

    event = {
      "type": "lifecycle",
      "reading": {
        "et": "model_updated",
        "asset_model": {
            "deviceType": "(G4) Gateway",
            "assetModelName": "UrbanIO (G4) Gateway Device",
            "assetModelDescription": "UrbanIO (G4) Gateway Device",
            "assetModelProperties": []
        }
      }
    }

    context = {}

    with patch.dict(os.environ, {"LIFECICLE_EVENTS_QUEUE_URL": queue["QueueUrl"], "AWS_REGION": aws_region }):
        from handler import handler
        handler(event, context)

    response = sqs.get_queue_attributes(QueueUrl=queue["QueueUrl"])

    assert int(response["Attributes"]["ApproximateNumberOfMessages"]) == 1


@mock_sqs
def test_device_events():
    aws_region = 'us-east-1'

    sqs = boto3.client('sqs', region_name='us-east-1')
    queue = sqs.create_queue(QueueName='test-queue.fifo', Attributes={"FifoQueue": "true"})

    event = {
        "type": "lifecycle",
        "reading": {
            "et": "device_updated",
            "id": "ATNEZ6N0ZH0p8UEtZFrQrQ",
            "di": "1000c20",
            "name": "Temp probe",
            "device_type": "(G4) Temperature Probe",
            "device_category": "System"
        },
        "metadata": {
            "ref": {
                "l": "T-y57nI7tFvpf4QXEdWnTA"
            }
        }
    }

    context = {}

    with patch.dict(os.environ, {"LIFECICLE_EVENTS_QUEUE_URL": queue["QueueUrl"], "AWS_REGION": aws_region }):
        from handler import handler
        handler(event, context)

    response = sqs.get_queue_attributes(QueueUrl=queue["QueueUrl"])

    assert int(response["Attributes"]["ApproximateNumberOfMessages"]) == 2