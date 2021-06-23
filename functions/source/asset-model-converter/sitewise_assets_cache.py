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
        self.table.put_item(Item=item)

    @staticmethod
    def item(record_id, asset_id, asset_name, hierarchy_name, parent_asset_id=None, parent_hierarchy_id=None, asset_data=None):
        return  {   
                'Id': record_id,
                'AssetId': asset_id,
                'AssetName': asset_name,
                'AssetData': asset_data,
                'HierarchyName': hierarchy_name,
                'ParentAssetId': parent_asset_id,
                'ParentHierarchyId': parent_hierarchy_id,
            }

    def __convert(self, object):
        return json.loads(json.dumps(object), parse_float=str)
