import logging
import os

logger = logging.getLogger()
logger.setLevel(getattr(logging, os.environ.get('LOG_LEVEL', 'INFO').upper()))

class AssociationConverter(object):
    """Updates asset associations"""
    def __init__(self, assets_cache, sitewise_client):
        self.assets_cache = assets_cache
        self.sitewise_client = sitewise_client

    def associate_asset(self, asset_cache, child_asset_cache):
        if asset_cache is None:
            logger.error(f"No parent asset found for {child_asset_cache.get('Id')}")
            return None

        if asset_cache.get('AssetId') != child_asset_cache.get('ParentAssetId'):
            if child_asset_cache.get('ParentAssetId') is not None:
                self.sitewise_client.disassociate(child_asset_cache['ParentAssetId'], 
                                                    child_asset_cache['ParentHierarchyId'], 
                                                    child_asset_cache['AssetId'])

            asset_description = self.sitewise_client.describe(asset_cache['AssetId'])
            hierarchy_ids = [item['id'] for item in asset_description['assetHierarchies'] if item.get('name')==child_asset_cache['HierarchyName']]
            
            if len(hierarchy_ids) < 1:
                # TODO change to error when addin the rest of device types
                logger.error(f"No hierarchy found for {child_asset_cache['HierarchyName']}")
                return

            if len(hierarchy_ids) > 1:
                # TODO change to error when addin the rest of device types
                logger.error(f"More than one hierarchy found for {child_asset_cache['HierarchyName']}")
                return

            self.sitewise_client.associate(asset_cache['AssetId'], hierarchy_ids[0], child_asset_cache['AssetId'])
            
            child_asset_cache['ParentAssetId'] = asset_cache['AssetId']
            child_asset_cache['ParentHierarchyId'] = hierarchy_ids[0]
            self.assets_cache.save(child_asset_cache)

        return child_asset_cache