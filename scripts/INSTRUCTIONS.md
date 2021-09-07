# ricochet-site-wise-assets 
Integration to AWS Site Wise 

&nbsp;

![alt text](SitewiseIntegration.png) 

&nbsp;
## Asset Model Converter

[asset-model-converter](src/asset-model-converter)creates UrbanIo models based on a description uploaded in S3 and converts UrbanIO items into AWS Site Wise assets.
It's intended to be deployed as a lambda triggered by SQS message Recieved events will have following structure:

&nbsp;

```json
{
    "type": "lifecycle",
    "reading": {
        "et": "location_updated",
        "id": "djpKTyygQWGFIWCebwxb7g",
        "name": "Test location"
    },
    "metadata": {
        "ref": {
            "l": "djpKTyygQWGFIWCebwxb7g",
            "c": "d9-bcsHFS7OwhJe2WQaOWg",
            "o": [
              "EI73X8OtQ_2lkSJc23JGIw ",
              "Gg6N0SBTTNa5PhuQcUL0Ug",
              "1xDODYrrRoKlLbTlOUsACQ"
            ]
        }
    }
}

{
  "type": "lifecycle",
  "reading": {
    "et": "device_updated",
    "id": "fdOyRr42dJu7vA4QcCH81Q",
    "di": "34563567",
    "name": "42634563546",
    "device_type": "(G4) Light"
  },
  "metadata": {
    "ref": {
      "d": "fdOyRr42dJu7vA4QcCH81Q",
      "l": "vujW2rdqFBoo6C8opXsh_g",
      "c": "DpCKLTButMkYDgUkuU1jeA",
      "o": [
        "TD016x-tBKEpZ_PUmyA29Q",
        "ieNIRulGlPWajnsP6vCd3g"
      ]
    }
  }
}
```
&nbsp;

When setted up it needs a list of environment variables. Example:

&nbsp;
```sh
export AWS_REGION=<aws-region>
export DYNAMO_ASSETS_TABLE_NAME=<dynamodb-to-store-created-assets>
export DYNAMO_MODELS_TABLE_NAME=<dynamodb-to-store-created-models>
```

&nbsp;

```
cd src/asset-model-converter
mkvirtualenv -p /usr/bin/python3 asset-model-converter
pipenv install --dev
pytest test_*
```
&nbsp;
### Site Wise Models

- UrbanIO Root Model
- [UrbanIO Operator Model](docs/urbanio_operator_model.json)
- [UrbanIO Customer Model](docs/urbanio_customer_model.json)
- [UrbanIO Location Model](docs/urbanio_location_model.json)
- [UrbanIO (G4) Temperature Device Model](docs/urbanio_gen4_temperature_device_model.json)
- [UrbanIO (G4) Activity Device Model](docs/urbanio_gen4_activity_device_model.json)
- ... more device models to be added

&nbsp;
## Association Updater

[Association updater](src/association-updater) updates the associations between assets.

&nbsp;
## Attributes Updater

[Attributes updater](src/attributes-updater) updates the attribute values of assets

&nbsp;
## S3 to Lifecycle Events Converter

It is triggered by both AWS IOT Core rule and S3 file upload.

```
cd src/s3-to-lifecycle-events-converter
export LIFECICLE_EVENTS_QUEUE_URL=ap-southeast-2
mkvirtualenv -p /usr/bin/python3 s3-to-sqs
pipenv install --dev
pytest test_*
```

&nbsp;
## IoT Core to SQS
It's triggered by Iot Core Rule and sends the messages to FIFO SQS.

```
cd src/iot-core-to-sqs
mkvirtualenv -p /usr/bin/python3 iot-to-sqs
pipenv install --dev
pytest test_*
```

&nbsp;
## Lifecycle Events to Model Mapping Updater
Caches the mapping between lifecycle events and SiteWise assets

Example mappings:
```
[
    {   
        "Model": "operator", 
        "Mappings": [
            { "name": "Name", "value": "reading.name", "type": "string"},
            { "name": "External ID", "value": "reading.id", "type": "string"}
        ]
    },
    { 
        "Model": "customer", 
        "Mappings": [
            { "name": "Name", "value": "reading.name", "type": "string"},
            { "name": "External ID", "value": "reading.id", "type": "string"}
        ]
    },
    { 
        "Model": "location", 
        "Mappings": [
            { "name": "Name", "value": "reading.name", "type": "string"},
            { "name": "External ID", "value": "reading.id", "type": "string"},
            { "name": "Longitude", "value": "reading.longitude", "type": "double"},
            { "name": "Latitude", "value": "reading.latitude", "type": "double"}
        ]
    },
    {
        "Model": "device", 
        "Mappings": [
            { "name": "Name", "value": "reading.name", "type": "string"},
            { "name": "External ID", "value": "reading.id", "type": "string"},
            { "name": "Serial Number", "value": "reading.di", "type": "string"}
        ]
    }
]
```

&nbsp;

Test lambda:

&nbsp;


```
cd src/lifecycle-events-mapping-updater
mkvirtualenv -p /usr/bin/python3 events-mapping-updater
pipenv install --dev
pytest test_*
```

&nbsp;

## Cloudformation Deploy

&nbsp;
### Prerequisites

- s3 bucket to upload the builded code (this bucket should be in the same region where the cloudformation stack will be deployed)
- aws cli
- python 3.8

&nbsp;
### The followig resources will be created:

- s3 bucket
- s3 BucketPolicy for cross account access
- two dynamodb tables
  - sitewiseModels
  - sitewiseAssets
- two Lambda functions
  - UrbanioSitewiseAssetsConverter
  - UrbanioSitewiseModelsConverter

&nbsp;
### Deploy Stack

&nbsp;
#### 1. Build Lambdas zip packages

  ```sh
  ./bin/build
  ```

&nbsp;
#### 2. Create cloudformation package. This will upload the zips in the specifued s3 bucket

&nbsp;
  ```sh
  aws cloudformation package --template deploy-stack.yaml --s3-bucket <BUCKETNAME> --output yaml --output-template-file packaged-deploy-stack.yaml
  ```

&nbsp;
#### 3. Create cloudformation stack

&nbsp;
- Go to: <https://ap-southeast-2.console.aws.amazon.com/cloudformation/home>

- Click Create stack
- Upload packaged.yaml
- Fill the parameters:
  - StackName (e.g UrbanioSitewise)
  - Bucket name (this bucket will store the assets and models)
  - OtherAccountNumber (this account will have access to the bucket that will be  created)
  - Read and Write capacity units for the dynamodb tables
