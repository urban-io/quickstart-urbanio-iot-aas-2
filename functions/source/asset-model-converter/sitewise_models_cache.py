import boto3
import json
import os

class SitewiseModelsCache:
    """
    Interactions with dynamo table - saving and retrieving information
    """
    def __init__(self, table_name=os.environ.get("DYNAMO_MODELS_TABLE_NAME"), aws_region=os.environ.get("AWS_REGION")):
        self.table_name = table_name
        self.aws_region = aws_region
        self.dynamodb = boto3.resource('dynamodb', region_name=self.aws_region)
        self.table = self.dynamodb.Table(self.table_name)

    def get(self, record_id):
        response = self.table.get_item(Key={ 'Name': record_id })
        return response.get('Item') if response is not None else None

    def get_model_id(self, record_id):
        response = self.table.get_item(Key={ 'Name': record_id })
        return response.get('Item', {}).get('AssetModelId') if response is not None else None

    def get_all(self):
        response = self.table.scan()

        return response["Items"]

    def save(self, item):
        self.table.put_item(Item=item)

    @staticmethod
    def item(model_name, asset_model_id, asset_model_hierarchies, asset_model_properties):
        return  {   
                'Name': model_name,
                'AssetModelId': asset_model_id,
                'AssetModelHierarchies': asset_model_hierarchies,
                'AssetModelProperties': asset_model_properties
            }

    def __convert(self, object):
        return json.loads(json.dumps(object), parse_float=str)

