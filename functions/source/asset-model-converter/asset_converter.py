import logging
import os
from sitewise_assets_cache import SitewiseAssetsCache

logger = logging.getLogger()
logger.setLevel(getattr(logging, os.environ.get('LOG_LEVEL', 'INFO').upper()))

class AssetConverter:
    """
    Converts UrbanIO Thing to SiteWise Assets
    """
    def __init__(self, assets_cache, models_cache, sitewise_client):
        self.assets_cache = assets_cache
        self.models_cache = models_cache
        self.sitewise_client = sitewise_client

    def setup_asset(self, obj_type, external_id, name, model_name, hierarchy_name):
        record_id = self.__dynamo_record_id(obj_type, external_id)
        dynamo_record = self.assets_cache.get(record_id)

        if dynamo_record:
            if dynamo_record['AssetName'] != name:
                self.sitewise_client.update_asset(dynamo_record['AssetId'], name)
                dynamo_record['AssetName'] = name
                self.assets_cache.save(dynamo_record)

            asset_description = self.sitewise_client.describe_asset(dynamo_record['AssetId'])
            properties_to_update = self.__asset_properties(obj_type, external_id, asset_description)
            self.sitewise_client.update_asset_properties(asset_description['assetId'], properties_to_update)

            return dynamo_record

        model_id = self.__sitewise_model_id(model_name)
        if model_id is None:
            logger.info(f'No cached model {model_name} found')
            return None

        asset_description = self.sitewise_client.create_asset(name, model_id)

        if asset_description is None:
            return None

        properties_to_update = self.__asset_properties(obj_type, external_id, asset_description)
        self.sitewise_client.update_asset_properties(asset_description['assetId'], properties_to_update)
            
        dynamo_item = SitewiseAssetsCache.item(record_id, asset_description['assetId'], name, hierarchy_name)
        self.assets_cache.save(dynamo_item)

        return dynamo_item

    def __dynamo_record_id(self, obj_type, external_id):
        return f"{obj_type}-{external_id}"
    
    def __sitewise_model_id(self, model_name):
        return self.models_cache.get_model_id(model_name)

    def __asset_properties(self, obj_type, external_id, asset_description):
        properties = []

        for prop in asset_description['assetProperties']:
            properties.append({'id': prop['id'], 'alias': f"/urbanio/{obj_type}/{external_id}/{prop['name']}"})

        return properties