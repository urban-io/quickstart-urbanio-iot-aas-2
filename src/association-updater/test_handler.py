import os
import pdb
from mock import patch, Mock

import pdb

os.environ['ASSETS_TO_UPDATE_QUEUE_URL'] = 'test.fifo'
os.environ['DYNAMO_ASSETS_TABLE_NAME'] = 'test'
os.environ['DYNAMO_INTEGRATION_POINTS_TABLE_NAME'] = 'integration_points'
os.environ['AWS_REGION'] = 'us-east-1'

from handler import handler, get_cache_ids
from association_converter import AssociationConverter

class MockSitewise(Mock):
    def __init__(self, *, aws_region=os.environ.get("AWS_REGION")):
        super(MockSitewise, self).__init__()

        self.asset_descriptions = {
            "root-urban.io": {
                "assetHierarchies": [
                    {"id": "111-abc-345", "name": "Operators"},
                ]
            },
            "operator-12313": {
                "assetHierarchies": [
                    {"id": "111-abc-345", "name": "Customers"},
                ]
            },
            "customer 4345345": {
                "assetHierarchies": [
                    {"id": "111-abc-345", "name": "Locations"},                ]
            },
            "location-979976": {
                "assetHierarchies": [
                    {"id": "111-abc-345", "name": "Category 1 Devices"},
                    {"id": "222-qwe-987", "name": "Category 2 Devices"},
                ]
            },
            "category-868665": {
                "assetHierarchies": [
                    {"id": "111-abc-345", "name": "Temperature sensors"},
                    {"id": "222-qwe-987", "name": "Pressure sensors"},
                    {"id": "333-zxc-567", "name": "Light sensors"}
                ]
            },
            "device-123456": {
                "assetHierarchies": []
            },
        }

    def describe(self, assetId):
        return self.asset_descriptions.get(assetId)

    def associate(self, asset_id, hierarchy_id, child_asset_id):
        return True

    def disassociate(self, asset_id, hierarchy_id, child_asset_id):
        return True


class MockSitewiseAssetsCache(Mock):
    def __init__(self, *, table_name=os.environ.get("DYNAMO_ASSETS_TABLE_NAME"), aws_region=os.environ.get("AWS_REGION")):
        super(MockSitewiseAssetsCache, self).__init__()

        self.asset_cache = {
            'operator-op_1': {
                "Id": "operator-op_1",
                "AssetId": "operator-12313",
                "ParentAssetId": None,
                "ParentHierarchyId": None,
                "HierarchyName": "Operators",
            },
            'customer-cust_1': {
                "Id": "customer-cust_1",
                "AssetId": "customer 4345345",
                "ParentAssetId": None,
                "ParentHierarchyId": None,
                "HierarchyName": "Customers"
            },
            'location-loc_1': {
                "Id": "location-loc_1",
                "AssetId": "location-979976",
                "ParentAssetId": None,
                "ParentHierarchyId": None,
                "HierarchyName": "Locations"
            },
            'category-cat_1-loc_1': {
                "Id": "category-cat_1-loc_1",
                "AssetId": "category-868665",
                "ParentAssetId": None,
                "ParentHierarchyId": None,
                "HierarchyName": "Category 1 Devices"
            },
            'device-dev_1': {
                "Id": "device-dev_1",
                "AssetId": "device-123456",
                "ParentAssetId": None,
                "ParentHierarchyId": None,
                "HierarchyName": "Temperature sensors"
            }
        }

    def get(self, record_id):
        return self.asset_cache.get(record_id)

    def save(self, item):
        self.asset_cache[item["Id"]] = item


class MockSitewiseIntegrationPoints(Mock):
    def __init__(self, *, table_name=os.environ.get("DYNAMO_INTEGRATION_POINTS_TABLE_NAME"), aws_region=os.environ.get("AWS_REGION")):
        super(MockSitewiseIntegrationPoints, self).__init__()
        self.integration_points = {
                'op_1': {
                    "Id": "op_1",
                    "Reading": {},
                    "Metadata": {}
                }
            }

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
mock_integration_points_cache = MockSitewiseIntegrationPoints()

mock_sitewise = MockSitewise()
mock_sqs = Mock()
mock_sqs.send_messages = Mock()

association_converter = AssociationConverter(mock_assets_cache, mock_sitewise)

@patch("handler.assets_cache", mock_assets_cache)
@patch("handler.sitewise", mock_sitewise)
@patch("handler.association_converter", association_converter)
@patch("handler.integration_points_cache", mock_integration_points_cache)
@patch("handler.sqs", mock_sqs)
def test_handler():

    event= {
        "Records": [
            {"body": "{\"type\":\"lifecycle\",\"reading\":{\"et\":\"operator_updated\",\"id\":\"op_1\"}}" },
            {"body": "{\"type\":\"lifecycle\",\"reading\":{\"et\":\"customer_updated\",\"id\":\"cust_1\"},\"metadata\":{\"ref\":{\"o\":[\"op_1\"]}}}" },
            {"body": "{\"type\":\"lifecycle\",\"reading\":{\"et\":\"location_updated\",\"id\":\"loc_1\"},\"metadata\":{\"ref\":{\"c\":\"cust_1\"}}}" },
            {"body": "{\"type\":\"lifecycle\",\"reading\":{\"et\":\"category_updated\",\"id\":\"cat_1-loc_1\"},\"metadata\":{\"ref\":{\"l\":\"loc_1\"}}}" },
            {"body": "{\"type\":\"lifecycle\",\"reading\":{\"et\":\"device_updated\",\"id\":\"dev_1\",\"device_category\":\"cat_1\"},\"metadata\":{\"ref\":{\"l\":\"loc_1\"}}}" },
        ]
    }

    handler(event, {})


def test_get_cache_ids():
    pass
