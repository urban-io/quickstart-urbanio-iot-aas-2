import boto3
import json
import os

class SitewiseIntegrationPoints:
    """
    Interactions with dynamo table - saving and retrieving information
    """
    def __init__(self, table_name=os.environ.get("DYNAMO_INTEGRATION_POINTS_TABLE_NAME"), aws_region=os.environ.get("AWS_REGION")):
        self.table_name = table_name
        self.aws_region = aws_region
        self.dynamodb = boto3.resource('dynamodb', region_name=self.aws_region)
        self.table = self.dynamodb.Table(self.table_name)

    def get(self, record_id):
        response = self.table.get_item(Key={ 'Id': record_id })
        return response.get('Item') if response is not None else None

    def get_all(self):
        response = self.table.scan()
        return response["Items"]

    def save(self, message):
        self.table.put_item(Item={
            'Id': message['reading']['id'],
            'Reading': self.__convert(message['reading']),
            'Metadata': self.__convert(message['metadata']),
            })

    def __convert(self, object):
        return json.loads(json.dumps(object), parse_float=str)