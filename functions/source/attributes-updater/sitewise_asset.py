import os
import time
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper()))


class SitewiseAsset:
    """
    Interactions with sitewise asset - creation, updating, and deletion.
    """

    def __init__(self, aws_region=os.environ.get("AWS_REGION")):
        self.aws_region = aws_region
        self.sitewise = boto3.client('iotsitewise', region_name=self.aws_region)
        self.pollWaitTime = 0.5

    def update_values(self, entries):
        logger.info(f'Updating attributes values {entries}')
        response = self.sitewise.batch_put_asset_property_value(entries=entries)
        logger.info(f'Response: {response}')
        return  len(response.get('errorEntries',[])) == 0