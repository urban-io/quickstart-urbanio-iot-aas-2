import boto3
import os

class EventMappings:
    def __init__(self, table_name=os.environ.get("DYNAMO_EVENT_MAPPING_TABLE_NAME"), aws_region=os.environ.get("AWS_REGION")):
        self.table_name = table_name
        self.aws_region = aws_region
        self.dynamodb = boto3.resource('dynamodb', region_name=self.aws_region)
        self.table = self.dynamodb.Table(self.table_name)

    def get_all(self):
        response = self.table.scan()
        return response["Items"]