import os
import json
import datetime
from mock import patch, Mock, call
from freezegun import freeze_time

os.environ["DYNAMO_ASSETS_TABLE_NAME"] = "test"
os.environ["DYNAMO_EVENT_MAPPING_TABLE_NAME"] = "mappings"
os.environ["AWS_REGION"] = "us-east-1"

from handler import process_event

mock_assets_cache  = Mock()
mock_sitewise_asset = Mock()
mock_event_mappings = Mock()

with open('data/mappings.json') as json_file:
    mappings = json.load(json_file)

@freeze_time("2021-05-14")
@patch("handler.event_mappings", mock_event_mappings)
@patch("handler.assets_cache", mock_assets_cache)
@patch("handler.sitewise_asset", mock_sitewise_asset)
def test_process_new_asset():
    event = {
        "type": "lifecycle",
        "reading": {
            "et": "device_updated",
            "id": "abc123",
            "di": "100001",
            "name": "test device"
        }
    }

    asset_cache = {   
                'Id': "abc123",
                'AssetId': "asset_id",
                'AssetName': "test device",
                'AssetData': None
            }

    entries = [{
                'entryId': 'abc123-Name',
                'propertyAlias': "/urbanio/device/abc123/Name",
                'propertyValues': [
                    {
                        'value': {
                            'stringValue': 'test device',
                        },
                        'timestamp': {
                            'timeInSeconds': int(datetime.datetime.now().timestamp()),
                        }
                    }
                ]
            },
            {
                'entryId': 'abc123-ExternalID',
                'propertyAlias': "/urbanio/device/abc123/External ID",
                'propertyValues': [
                    {
                        'value': {
                            'stringValue': 'abc123',
                        },
                        'timestamp': {
                            'timeInSeconds': int(datetime.datetime.now().timestamp()),
                        }
                    }
                ]
            },
            {
                'entryId': 'abc123-SerialNumber',
                'propertyAlias': "/urbanio/device/abc123/Serial Number",
                'propertyValues': [
                    {
                        'value': {
                            'stringValue': '100001',
                        },
                        'timestamp': {
                            'timeInSeconds': int(datetime.datetime.now().timestamp()),
                        }
                    }
                ]
            },
            {
                'entryId': 'abc123-CommissioningStatus',
                'propertyAlias': "/urbanio/device/abc123/Commissioning Status",
                'propertyValues': [
                    {
                        'value': {
                            'stringValue': 'Commissioned',
                        },
                        'timestamp': {
                            'timeInSeconds': int(datetime.datetime.now().timestamp()),
                        }
                    }
                ]
            }
            ]

    mock_assets_cache.get = Mock(return_value=asset_cache)
    mock_sitewise_asset.update_values = Mock(return_value=True)

    process_event(event, mappings)

    mock_sitewise_asset.update_values.assert_called_once_with(entries=entries)

@freeze_time("2021-05-14")
@patch("handler.event_mappings", mock_event_mappings)
@patch("handler.assets_cache", mock_assets_cache)
@patch("handler.sitewise_asset", mock_sitewise_asset)
def test_process_updated_asset():
    event = {
        "type": "lifecycle",
        "reading": {
            "et": "device_updated",
            "id": "abc123",
            "di": "100002",
            "name": "test device 2"
        }
    }

    asset_cache = {   
                'Id': "abc123",
                'AssetId': "asset_id",
                'AssetName': "test device",
                'AssetData': {
                    "Name": "test device",
                    "Serial Number": "100001",
                    "External ID": "abc123",
                    "Commissioning Status": 'Commissioned'
                }
            }

    entries = [{
                'entryId': 'abc123-Name',
                'propertyAlias': "/urbanio/device/abc123/Name",
                'propertyValues': [
                    {
                        'value': {
                            'stringValue': 'test device 2',
                        },
                        'timestamp': {
                            'timeInSeconds': int(datetime.datetime.now().timestamp()),
                        }
                    }
                ]
            },
            {
                'entryId': 'abc123-SerialNumber',
                'propertyAlias': "/urbanio/device/abc123/Serial Number",
                'propertyValues': [
                    {
                        'value': {
                            'stringValue': '100002',
                        },
                        'timestamp': {
                            'timeInSeconds': int(datetime.datetime.now().timestamp()),
                        }
                    }
                ]
            }
            ]

    mock_assets_cache.get = Mock(return_value=asset_cache)
    mock_sitewise_asset.update_values = Mock(return_value=True)

    process_event(event, mappings)

    mock_sitewise_asset.update_values.assert_called_once_with(entries=entries)


@freeze_time("2021-05-14")
@patch("handler.event_mappings", mock_event_mappings)
@patch("handler.assets_cache", mock_assets_cache)
@patch("handler.sitewise_asset", mock_sitewise_asset)
def test_process_missing_event_value():
    event = {
        "type": "lifecycle",
        "reading": {
            "et": "device_updated",
            "id": "abc123",
            "di": "100002",
        }
    }

    asset_cache = {   
                'Id': "abc123",
                'AssetId': "asset_id",
                'AssetName': "test device",
                'AssetData': {
                    "Name": "test device",
                    "Serial Number": "100001",
                    "External ID": "abc123",
                    "Commissioning Status": 'Commissioned'
                }
            }

    entries = [{
                'entryId': 'abc123-SerialNumber',
                'propertyAlias': "/urbanio/device/abc123/Serial Number",
                'propertyValues': [
                    {
                        'value': {
                            'stringValue': '100002',
                        },
                        'timestamp': {
                            'timeInSeconds': int(datetime.datetime.now().timestamp()),
                        }
                    }
                ]
            }]

    mock_assets_cache.get = Mock(return_value=asset_cache)
    mock_sitewise_asset.update_values = Mock(return_value=True)

    process_event(event, mappings)

    mock_sitewise_asset.update_values.assert_called_once_with(entries=entries)