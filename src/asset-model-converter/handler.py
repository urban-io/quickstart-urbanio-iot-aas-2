import boto3
import os
import sys
import json
import logging
from sqs import Sqs
from sitewise import Sitewise
from sitewise_assets_cache import SitewiseAssetsCache
from sitewise_models_cache import SitewiseModelsCache
from sitewise_integration_points import SitewiseIntegrationPoints
from asset_converter import AssetConverter
from model_converter import ModelConverter

logger = logging.getLogger()
logger.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper()))

sitewise = Sitewise(os.environ['AWS_REGION'])
assets_cache = SitewiseAssetsCache(os.environ['DYNAMO_ASSETS_TABLE_NAME'], os.environ['AWS_REGION'])
models_cache = SitewiseModelsCache(os.environ['DYNAMO_MODELS_TABLE_NAME'], os.environ['AWS_REGION'])
integration_points_cache = SitewiseIntegrationPoints(os.environ['DYNAMO_INTEGRATION_POINTS_TABLE_NAME'], os.environ['AWS_REGION'])

sqs = Sqs(os.environ['ASSETS_TO_ASSOCIATE_QUEUE_URL'], int(os.environ.get("BATCH_SIZE", 10)), os.environ["AWS_REGION"])

asset_converter = AssetConverter(assets_cache, models_cache, sitewise)
model_converter = ModelConverter(models_cache, sitewise)


def process_event(event, integration_points):
    if not event['type'] == 'lifecycle':
        return None

    asset = None
    integration_point_ids = [integration['Id'] for integration in integration_points]

    if event['reading']['et'] == 'model_updated':
        model_converter.setup_model(event['reading']['asset_model'])

    elif event['reading']['et'] == 'root_updated':
        asset = asset_converter.setup_asset( 
            obj_type="root",
            external_id=event['reading']['id'], 
            name=event['reading']['name'], 
            model_name="UrbanIO Root", 
            hierarchy_name=None
        )

    elif event['reading']['et'] == 'operator_updated' and event['reading']['id'] in integration_point_ids:
        # Remove superoperator name from ancestors
        ancestor_names = event['reading']['ancestor_names']
        ancestor_names.pop(0)
        asset = asset_converter.setup_asset( 
            obj_type="operator",
            external_id=event['reading']['id'],
            name=' / '.join(ancestor_names), 
            model_name="UrbanIO Operator", 
            hierarchy_name='Operators'
        )

    elif event['reading']['et'] == 'customer_updated':
        parent_operators = [integration for integration in integration_points if integration['Id'] in event['metadata']['ref']['o']]
        parent_operator_names = parent_operators[0]['Reading']['ancestor_names'] if len(parent_operators) > 0 else []
        sub_operator_names = [x for x in event['reading']['ancestor_names'] if x not in parent_operator_names]
        customer_name = f"{'/'.join(sub_operator_names)}/{event['reading']['name']}" if len(sub_operator_names) > 0 else event['reading']['name']
        asset = asset_converter.setup_asset( 
            obj_type="customer",
            external_id=event['reading']['id'], 
            name= customer_name, 
            model_name="UrbanIO Customer", 
            hierarchy_name='Customers'
        )

    elif event['reading']['et'] == 'location_updated':
        asset = asset_converter.setup_asset( 
            obj_type="location",
            external_id=event['reading']['id'], 
            name= f"{event['reading']['name']} ({event['reading']['id']})", 
            model_name="UrbanIO Location", 
            hierarchy_name='Locations'
        )

    elif event['reading']['et'] == 'category_updated':
        asset = asset_converter.setup_asset( 
            obj_type="category",
            external_id=event['reading']['id'], 
            name= f"{event['reading']['name']} ({event['metadata']['ref']['l']})", 
            model_name=f"UrbanIO {event['reading']['name'].capitalize()} Sensors", 
            hierarchy_name=f"{event['reading']['name'].capitalize()} Devices"
        )       

    elif event['reading']['et'] == 'device_updated':
        asset = asset_converter.setup_asset( 
            obj_type="device",
            external_id=event['reading']['id'], 
            name= f"{event['reading']['name']} ({event['reading']['id']})", 
            model_name=f"UrbanIO {event['reading']['device_type']} Device", 
            hierarchy_name=f"{event['reading']['device_type']} Devices"
        )

    return asset    
            

def handler(event, context):
    logger.debug('event is {}'.format(event))

    assets_to_associate = []
    integration_points = integration_points_cache.get_all()

    try:
        for record in event['Records']:
            lifecycle_event = json.loads(record["body"])
            logger.info(f"lifecycle event: {lifecycle_event}")
            asset = process_event(lifecycle_event, integration_points)
            if asset:
                assets_to_associate.append(lifecycle_event)

        sqs.send_messages(assets_to_associate)

    except Exception as e:
        # Send some context about this error to Lambda Logs
        logger.error(e)
        # throw exception, do not handle. Lambda will make message visible again.
        raise e
    