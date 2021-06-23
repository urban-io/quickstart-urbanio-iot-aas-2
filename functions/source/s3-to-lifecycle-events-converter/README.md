## S3 to Lifecycle Events Converter

It is triggered by both AWS IOT Core rule and S3 file upload.

```
cd src/s3-to-lifecycle-events-converter
export LIFECICLE_EVENTS_QUEUE_URL=ap-southeast-2
mkvirtualenv -p /usr/bin/python3 s3-to-sqs
pipenv install --dev
pytest test_*
```
