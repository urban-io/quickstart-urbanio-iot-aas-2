# IoT Core to SQS
It's triggered by Iot Core Rule and sends the messages to FIFO SQS.

```
cd src/iot-core-to-sqs
mkvirtualenv -p /usr/bin/python3 iot-to-sqs
pipenv install --dev
pytest test_*
```
