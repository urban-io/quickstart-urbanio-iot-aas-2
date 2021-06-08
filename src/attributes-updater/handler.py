import boto3
import os
import json
import logging
import datetime
from jsonpath_ng import jsonpath, parse
from event_mappings import EventMappings
from sitewise_asset import SitewiseAsset
from sitewise_assets_cache import SitewiseAssetsCache

logger = logging.getLogger()
logger.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper()))

assets_cache = SitewiseAssetsCache(os.environ['DYNAMO_ASSETS_TABLE_NAME'], os.environ['AWS_REGION'])
sitewise_asset = SitewiseAsset()
event_mappings = EventMappings(os.environ['DYNAMO_EVENT_MAPPING_TABLE_NAME'], os.environ['AWS_REGION'])


def get_mapping(event, mappings):
    if not event['type'] == 'lifecycle':
        return None

    mapping = [x for x in mappings if x['Type'] == event['reading']['et']]
    return mapping[0] if len(mapping) else None


def process_event(event, mappings):
    mapping = get_mapping(event, mappings)

    logger.info(f"Mapping: {mapping}")

    if mapping is None:
        return None

    asset = assets_cache.get(f"{mapping['Model']}-{event['reading']['id']}")

    logger.info(f"Asset {asset}")
    
    if asset is None:
        return None

    if asset.get('AssetData') is None:
        asset["AssetData"] = {}

    entries = []
    
    for prop in mapping['Mappings']:
        value = None

        if 'value' in prop:
            value = prop['value']
        elif 'path' in prop:
            jsonpath_expression = parse(prop['path'])
            match = jsonpath_expression.find(event)
            if len(match) == 0:
                continue
            else:
                value = match[0].value

        if prop['type'] == 'integer':
            value = int(value)
        elif prop['type'] == 'double':
            value = float(value)
        elif prop['type'] == 'boolean':
            value = (value in ['True', 'true'])
        else:
            value = value

        if asset.get('AssetData').get(prop['name']) == value:
            continue

        asset['AssetData'][prop['name']] = value

        entries.append(
            {
                'entryId': f"{event['reading']['id']}-{prop['name']}".replace(" ", ""),
                'propertyAlias': f"/urbanio/{mapping['Model']}/{event['reading']['id']}/{prop['name']}",
                'propertyValues': [
                    {
                        'value': {
                            f"{prop['type']}Value": value,
                        },
                        'timestamp': {
                            'timeInSeconds': int(datetime.datetime.now().timestamp()),
                        }
                    }
                ]
            }
        )

    if len(entries) > 0:
        if sitewise_asset.update_values(entries=entries):
            assets_cache.save(asset)
    

def handler(event, context):
    logger.debug('event is {}'.format(event))

    mappings = event_mappings.get_all()

    logger.info(f'Event Mappings: {mappings}')

    try:
        for record in event['Records']:
            # Batch by 10
            body = json.loads(record["body"])
            logger.info(f"Message: {body}")
            process_event(body, mappings)

    except Exception as e:
        # Send some context about this error to Lambda Logs
        logger.error(e)
        # throw exception, do not handle. Lambda will make message visible again.
        raise e