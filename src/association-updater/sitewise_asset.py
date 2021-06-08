import os
import time
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper()))


class SitewiseAsset:
    """
    Interactions with sitewise asset - updates attributes values
    """

    def __init__(self, aws_region=os.environ.get("AWS_REGION")):
        self.aws_region = aws_region
        self.sitewise = boto3.client('iotsitewise', region_name=self.aws_region)
        self.pollWaitTime = 0.5

    def describe(self, assetId):
        return  self.sitewise.describe_asset(assetId=assetId)

    def associate(self, asset_id, hierarchy_id, child_asset_id):
        logger.info(f'Associating child_asset_id {child_asset_id} to asset {asset_id}')
        self.sitewise.associate_assets(assetId=asset_id, hierarchyId=hierarchy_id, childAssetId=child_asset_id)

    def disassociate(self, asset_id, hierarchy_id, child_asset_id):
        logger.info(f'Disassociating child_asset_id {child_asset_id} to asset {asset_id}')
        self.sitewise.disassociate_assets(assetId=asset_id, hierarchyId=hierarchy_id, childAssetId=child_asset_id)