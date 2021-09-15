import os
import pdb
import uuid
import json
from mock import patch, Mock

os.environ['ASSETS_TO_ASSOCIATE_QUEUE_URL'] = 'test.fifo'
os.environ['DYNAMO_ASSETS_TABLE_NAME'] = 'assets'
os.environ['DYNAMO_MODELS_TABLE_NAME'] = 'models'
os.environ['DYNAMO_INTEGRATION_POINTS_TABLE_NAME'] = 'integration_points'
os.environ['AWS_REGION'] = 'us-east-1'

from handler import handler, process_event
from asset_converter import AssetConverter
from model_converter import ModelConverter

class MockSitewise(Mock):
    def __init__(self, *, aws_region=os.environ.get("AWS_REGION")):
        super(MockSitewise, self).__init__()
        self.assets = {}
        self.models = {}

    def create_asset(self, assetName, assetModelId):
        uid = str(uuid.uuid4())
        self.assets[uid] = {
            "assetId": uid,
            "assetName": assetName,
            "assetModelId": assetModelId,
            "assetProperties": []
        }

        return self.assets[uid]

    def update_asset(self, assetId, assetName):
        self.assets[assetId]['assetName'] = assetName

    def update_asset_properties(self, assetId, assetProperties):
        self.assets[assetId]['assetProperties'] = assetProperties

    def waitForActiveAssetModel(self, model_id):
        return self.assets.get(assetId)

    def describe_asset(self, assetId):
        return self.assets.get(assetId)

    def create_asset_model(self, model_name, model_description, model_properties=[], model_hierarchies=[]):
        uid = str(uuid.uuid4())
        self.models[uid] = {
            "assetModelId": uid,
            "assetModelName": model_name,
            "assetModelDescription": model_description,
            "assetModelProperties": model_properties,
            "assetModelHierarchies": model_hierarchies
        }

        return self.models[uid]

    def update_asset_model(self, model_id , model_name, model_description, model_properties=[], model_hierarchies=[]):
        self.models[model_id] = {
            "assetModelId": model_id,
            "assetModelName": model_name,
            "assetModelDescription": model_description,
            "assetModelProperties": model_properties,
            "assetModelHierarchies": model_hierarchies
        }

        return self.models[model_id]


class MockSitewiseModelsCache(Mock):
    def __init__(self, *, table_name=os.environ.get("DYNAMO_MODELS_TABLE_NAME"), aws_region=os.environ.get("AWS_REGION")):
        super(MockSitewiseModelsCache, self).__init__()
        self.models_cache = {}

    def get(self, record_id):
        return self.models_cache.get(record_id)

    def get_model_id(self, record_id):
        return self.models_cache.get(record_id)["AssetModelId"]

    def get_all(self):
        return self.models_cache.values()

    def save(self, item):
        self.models_cache[item["Name"]] = item


class MockSitewiseAssetsCache(Mock):
    def __init__(self, *, table_name=os.environ.get("DYNAMO_ASSETS_TABLE_NAME"), aws_region=os.environ.get("AWS_REGION")):
        super(MockSitewiseAssetsCache, self).__init__()
        self.assets_cache = {}

    def get(self, record_id):
        return self.assets_cache.get(record_id)

    def save(self, item):
        self.assets_cache[item["Id"]] = item


class MockSitewiseIntegrationPoints(Mock):
    def __init__(self, *, table_name=os.environ.get("DYNAMO_INTEGRATION_POINTS_TABLE_NAME"), aws_region=os.environ.get("AWS_REGION")):
        super(MockSitewiseIntegrationPoints, self).__init__()
        self.integration_points = {}

    def get(self, record_id):
        return self.integration_points.get(record_id)

    def get_all(self):
        return self.integration_points.values()

    def save(self, item):
        self.integration_points[item["Id"]] = {
            'Id': item['reading']['id'],
            'Reading': item['reading'],
            'Metadata': item['metadata']
        }


mock_assets_cache = MockSitewiseAssetsCache()
mock_models_cache = MockSitewiseModelsCache()
mock_integration_points_cache = MockSitewiseIntegrationPoints()

mock_sitewise = MockSitewise()
mock_sqs = Mock()
mock_sqs.send_messages = Mock()

asset_converter = AssetConverter(mock_assets_cache, mock_models_cache, mock_sitewise)
model_converter = ModelConverter(mock_models_cache, mock_sitewise)

@patch("handler.sqs", mock_sqs)
@patch("handler.sitewise", mock_sitewise)
@patch("handler.assets_cache", mock_assets_cache)
@patch("handler.models_cache", mock_models_cache)
@patch("handler.asset_converter", asset_converter)
@patch("handler.model_converter", model_converter)
@patch("handler.integration_points_cache", mock_integration_points_cache)
def test_handler():
    readings = json.loads(open('data/test.json').read())
    sqs_msgs = [{'body': json.dumps(msg)} for msg in readings]
    event = {
        "Records" : sqs_msgs
    }

    handler(event, {})


def test_get_cache_ids():
    pass
