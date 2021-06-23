import boto3
import os
import json
import logging
from sqs import Sqs
from sitewise_asset import SitewiseAsset
from sitewise_assets_cache import SitewiseAssetsCache
from association_converter import AssociationConverter
from sitewise_integration_points import SitewiseIntegrationPoints

logger = logging.getLogger()
logger.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper()))

assets_cache = SitewiseAssetsCache(os.environ['DYNAMO_ASSETS_TABLE_NAME'], os.environ['AWS_REGION'])
integration_points_cache = SitewiseIntegrationPoints(os.environ['DYNAMO_INTEGRATION_POINTS_TABLE_NAME'], os.environ['AWS_REGION'])

sitewise = SitewiseAsset()
sqs = Sqs(os.environ['ASSETS_TO_UPDATE_QUEUE_URL'], int(os.environ.get("BATCH_SIZE", 10)), os.environ["AWS_REGION"])

association_converter = AssociationConverter(assets_cache, sitewise)

def get_cache_ids(event, integration_points):
    if not event['type'] == 'lifecycle':
        return

    if event['reading']['et'] == 'operator_updated':
        return {   
            'child': f"operator-{event['reading']['id']}", 
            'parent': 'root-urban.io'
        }
    elif event['reading']['et'] == 'customer_updated':
        parent_operators = [integration for integration in integration_points if integration['Id'] in event['metadata']['ref']['o']]
        if len(parent_operators) > 0 : 
            return { 
                'child': f"customer-{event['reading']['id']}", 
                'parent': f"operator-{parent_operators[0]['Id']}"
            }
    elif event['reading']['et'] == 'location_updated':
        return { 
            'child': f"location-{event['reading']['id']}", 
            'parent': f"customer-{event['metadata']['ref']['c']}"
        }
    elif event['reading']['et'] == 'category_updated':
        return {
            'child': f"category-{event['reading']['id']}", 
            'parent': f"location-{event['metadata']['ref']['l']}"
        }
    elif event['reading']['et'] == 'device_updated':
        return {
            'child': f"device-{event['reading']['id']}", 
            'parent': f"category-{event['reading']['device_category']}-{event['metadata']['ref']['l']}"
        }


def process_event(event, integration_points):
    cache_ids = get_cache_ids(event, integration_points)
    if cache_ids and cache_ids.get('child') and cache_ids.get('parent'):
        child_asset = assets_cache.get(cache_ids.get('child'))
        parent_asset = assets_cache.get(cache_ids.get('parent'))

        if child_asset is None:
            logger.warn(f"Asset with id={cache_ids.get('child')} isn't found.")
            return

        if parent_asset is None:
            logger.error(f"No parent asset with id={cache_ids.get('parent')} found for {cache_ids.get('child')}")
            return

        association_converter.associate_asset(parent_asset, child_asset)
    
def handler(event, context):
    logger.debug('event is {}'.format(event))

    assets_to_update = []
    integration_points = integration_points_cache.get_all()

    try:
        for record in event['Records']:
            # Batch by 10
            lifecycle_event = json.loads(record["body"])
            logger.info(f"Message: {lifecycle_event}")
            process_event(lifecycle_event, integration_points)
            assets_to_update.append(lifecycle_event)

        sqs.send_messages(assets_to_update)

    except Exception as e:
        # Send some context about this error to Lambda Logs
        logger.error(e)
        # throw exception, do not handle. Lambda will make message visible again.
        raise e
    