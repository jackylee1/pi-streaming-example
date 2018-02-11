import json
from uuid import uuid4
from os.path import join
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

creds_base_path = '.creds'
root_ca = join(creds_base_path, 'ROOT_CA')
private_key = join(creds_base_path, 'PKEY')
cert_path = join(creds_base_path, 'CERT')
host = 'HOST.iot.eu-west-1.amazonaws.com'
topic = 'TOPIC'

myAWSIoTMQTTClient = AWSIoTMQTTClient(uuid4())
myAWSIoTMQTTClient.configureEndpoint(host, 8883)
myAWSIoTMQTTClient.configureCredentials(CAFilePath=root_ca, KeyPath=private_key, CertificatePath=cert_path)
myAWSIoTMQTTClient.connect()
message = {}
message['message'] = 'Hello World!'
messageJson = json.dumps(message)
myAWSIoTMQTTClient.publish(topic, messageJson, 1)