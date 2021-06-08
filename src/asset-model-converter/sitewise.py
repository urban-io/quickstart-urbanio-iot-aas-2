import os
import time
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper()))


class Sitewise:
    """
    Interactions with sitewise assets and models - creation, updating, and deletion.
    """

    def __init__(self, aws_region=os.environ.get("AWS_REGION")):
        self.aws_region = aws_region
        self.sitewise = boto3.client('iotsitewise', region_name=self.aws_region)
        self.pollWaitTime = 0.5

    def create_asset(self, assetName, assetModelId):
        logger.info(f"Creating asset {assetName}")
        try:
            asset = self.sitewise.create_asset(
                assetName=assetName,
                assetModelId=assetModelId
            )
        except ClientError as e:
            logger.error(e)
            return None

        assetDescription = self.sitewise.describe_asset(
            assetId=asset['assetId']
        )

        while assetDescription['assetStatus']['state'] != 'ACTIVE':
            time.sleep(self.pollWaitTime)
            assetDescription = self.sitewise.describe_asset(
                assetId=asset['assetId']
            )

        return assetDescription

    def describe_asset(self, assetId):
        return  self.sitewise.describe_asset(assetId=assetId)

    def update_asset(self, assetId, assetName):
        self.sitewise.update_asset(
            assetId=assetId,
            assetName=assetName
        )

    def update_asset_properties(self, assetId, assetProperties):
        logger.info(f"Updating asset properties {assetProperties}")
        for propRef in assetProperties:
            self.sitewise.update_asset_property(
                assetId=assetId,
                propertyId=propRef['id'],
                propertyNotificationState='DISABLED',
                propertyAlias=propRef['alias']
            )


    def waitForActiveAssetModel(self, model_id):
        model_description = self.sitewise.describe_asset_model(
            assetModelId=model_id
        )
        while model_description['assetModelStatus']['state'] != 'ACTIVE':
            time.sleep(self.pollWaitTime)
            model_description = self.sitewise.describe_asset_model(
                assetModelId=model_id
            )

        return model_description

    def describe_asset_model(self, model_id):
        return self.sitewise.describe_asset_model(assetModelId=model_id)

    def create_asset_model(self, model_name, model_description, model_properties=[], model_hierarchies=[]):
        logger.info('Creating model {}'.format(model_name))

        model = self.sitewise.create_asset_model(
            assetModelName=model_name,
            assetModelDescription=model_description,
            assetModelProperties=model_properties,
            assetModelHierarchies=model_hierarchies,
        )

        return self.waitForActiveAssetModel(model_id=model['assetModelId'])


    def update_asset_model(self, model_id , model_name, model_description, model_properties=[], model_hierarchies=[]):
        logger.info('Updating model {}'.format(model_name))

        model = self.sitewise.update_asset_model(
            assetModelId=model_id,
            assetModelName=model_name,
            assetModelDescription=model_description,
            assetModelProperties=model_properties,
            assetModelHierarchies=model_hierarchies,
        )

        return self.waitForActiveAssetModel(model_id=model_id)
