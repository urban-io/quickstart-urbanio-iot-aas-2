import os
import sys
import boto3
import logging
from sitewise_models_cache import SitewiseModelsCache

class ModelConverter:

    def __init__(self, models_cache, sitewise_client):
        self.models_dynamo = models_cache
        self.sitewise_client = sitewise_client

    def setup_model(self, model_description):
        models = {item['Name']:item for item in self.models_dynamo.get_all()}
        cached_model = models.get(model_description['assetModelName'])

        if cached_model is None:
            hierarchies = []
            if 'assetModelHierarchies' in model_description:
                for item in model_description['assetModelHierarchies']:
                    hierarchies.append({ 'name': item['name'], 
                                         'childAssetModelId': models.get(item['childAssetModel'])['AssetModelId'] 
                                        })

            asset_model = self.sitewise_client.create_asset_model( model_description['assetModelName'],
                                                                    model_description['assetModelDescription'], 
                                                                    model_description['assetModelProperties'], 
                                                                    hierarchies)
               
            dynamo_item = SitewiseModelsCache.item(model_description['assetModelName'], asset_model['assetModelId'], asset_model['assetModelHierarchies'], asset_model['assetModelProperties'])
            self.models_dynamo.save(dynamo_item)

            return dynamo_item
        else:
            hierarchies = []
            if 'assetModelHierarchies' in model_description:
                for hierarchy in model_description['assetModelHierarchies']:
                    cached_hierarchy_ids = [item['id'] for item in cached_model.get('AssetModelHierarchies',[]) if item.get('name')==hierarchy.get('name')]
                    
                    hierarchy_to_update = { 'name': hierarchy['name'], 'childAssetModelId': models.get(hierarchy['childAssetModel'])['AssetModelId']}
                    if len(cached_hierarchy_ids) == 1:
                        hierarchy_to_update['id'] = cached_hierarchy_ids[0] 

                    hierarchies.append(hierarchy_to_update)

            properties = []
            if 'assetModelProperties' in model_description:
                for prop in model_description['assetModelProperties']:
                    cached_property_ids = [item['id'] for item in cached_model.get('AssetModelProperties',[]) if item.get('name')==prop.get('name')]
                    if len(cached_property_ids) == 1:
                        prop['id'] = cached_property_ids[0]
                        
                    properties.append(prop)

            asset_model = self.sitewise_client.update_asset_model( cached_model['AssetModelId'],
                                                                    model_description['assetModelName'],
                                                                    model_description['assetModelDescription'], 
                                                                    properties, 
                                                                    hierarchies)

            cached_model['AssetModelHierarchies'] = asset_model['assetModelHierarchies']
            cached_model['AssetModelProperties'] = asset_model['assetModelProperties']
            self.models_dynamo.save(cached_model)

            return cached_model