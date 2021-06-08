import boto3
import json
import os

class SitewiseAssetsCache:
    """
    Interactions with dynamo table - saving and retrieving information
    """
    def __init__(self, table_name=os.environ.get("DYNAMO_ASSETS_TABLE_NAME"), aws_region=os.environ.get("AWS_REGION")):
        self.table_name = table_name
        self.aws_region = aws_region
        self.dynamodb = boto3.resource('dynamodb', region_name=self.aws_region)
        self.table = self.dynamodb.Table(self.table_name)

    def get(self, record_id):
        response = self.table.get_item(Key={ 'Id': record_id })
        return response.get('Item') if response is not None else None

    def save(self, item):
        data=json.loads(json.dumps(item), parse_float=str)
        return self.table.put_item(Item=data)