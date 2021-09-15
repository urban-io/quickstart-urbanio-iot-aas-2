import os
import boto3
import datetime
from mock import patch, Mock, call
from association_converter import AssociationConverter


def test_associate_asset():
    child_asset = {
        "Id": "device-123456",
        "AssetId": "device-123456",
        "ParentAssetId": None,
        "ParentHierarchyId": None,
        "HierarchyName": "Temperature sensors"
    }

    parent_asset = {
        "Id": "locaton-56789",
        "AssetId": "locaton-56789"
    }


    asset_description = {
        "assetHierarchies": [
            {"id": "111-abc-345", "name": "Temperature sensors"},
            {"id": "222-qwe-987", "name": "Pressure sensors"},
            {"id": "333-zxc-567", "name": "Light sensors"}
        ]
    }

    mock_assets_cache  = Mock()
    mock_sitewise_asset = Mock()
    mock_sitewise_asset.describe = Mock(return_value=asset_description)

    association_converter = AssociationConverter(mock_assets_cache, mock_sitewise_asset)
    association_converter.associate_asset(parent_asset, child_asset)

    mock_sitewise_asset.describe.assert_called_once_with("locaton-56789")
    assert mock_sitewise_asset.disassociate.call_count == 0
    mock_sitewise_asset.associate.assert_called_once_with("locaton-56789", "111-abc-345", "device-123456")
    mock_assets_cache.save.assert_called_once_with({
                                                        "Id": "device-123456",
                                                        "AssetId": "device-123456",
                                                        "ParentAssetId": "locaton-56789",
                                                        "ParentHierarchyId": "111-abc-345",
                                                        "HierarchyName": "Temperature sensors"
                                                    })


def test_reassociate_asset():
    child_asset = {
        "Id": "device-123456",
        "AssetId": "device-123456",
        "ParentAssetId": "locaton-123456",
        "ParentHierarchyId": "789-bnm-456",
        "HierarchyName": "Temperature sensors"
    }

    parent_asset = {
        "Id": "locaton-56789",
        "AssetId": "locaton-56789"
    }

    asset_description = {
        "assetHierarchies": [
            {"id": "111-abc-345", "name": "Temperature sensors"},
            {"id": "222-qwe-987", "name": "Pressure sensors"},
            {"id": "333-zxc-567", "name": "Light sensors"}
        ]
    }

    mock_assets_cache  = Mock()
    mock_sitewise_asset = Mock()
    mock_sitewise_asset.describe = Mock(return_value=asset_description)

    association_converter = AssociationConverter(mock_assets_cache, mock_sitewise_asset)
    association_converter.associate_asset(parent_asset, child_asset)

    mock_sitewise_asset.describe.assert_called_once_with("locaton-56789")
    mock_sitewise_asset.disassociate.assert_called_once_with("locaton-123456", "789-bnm-456", "device-123456")
    mock_sitewise_asset.associate.assert_called_once_with("locaton-56789", "111-abc-345", "device-123456")
    mock_assets_cache.save.assert_called_once_with({
                                                        "Id": "device-123456",
                                                        "AssetId": "device-123456",
                                                        "ParentAssetId": "locaton-56789",
                                                        "ParentHierarchyId": "111-abc-345",
                                                        "HierarchyName": "Temperature sensors"
                                                    })


def test_skip_association():
    child_asset = {
        "Id": "device-123456",
        "AssetId": "device-123456",
        "ParentAssetId": "locaton-123456",
        "ParentHierarchyId": "789-bnm-456",
        "HierarchyName": "Temperature sensors"
    }

    parent_asset = {
        "Id": "locaton-123456",
        "AssetId": "locaton-123456"
    }

    mock_assets_cache  = Mock()
    mock_sitewise_asset = Mock()
    association_converter = AssociationConverter(mock_assets_cache, mock_sitewise_asset)
    association_converter.associate_asset(parent_asset, child_asset)

    mock_sitewise_asset.describe.call_count == 0
    mock_sitewise_asset.disassociate.call_count == 0
    mock_sitewise_asset.associate.call_count == 0
    mock_assets_cache.save.call_count == 0


def test_no_parent():
    pass