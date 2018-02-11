#!/usr/bin/env python

import io
import picamera
import boto3
import traceback
import argparse
import logging
import time
from logging.config import fileConfig

fileConfig('logging.ini')
logger = logging.getLogger('pi_demo')

# Gather input
parser = argparse.ArgumentParser(
    description='Take a snap on your Pi and upload it to AWS S3', usage='python record.py -resolution 480'
)
parser.add_argument('-r', dest='resolution', type=str, choices=['360', '480', '720', '1080'],
                    help='the resolution for your snap', default='480', )

args = parser.parse_args()
resolutions = {
    '360': (480, 360),
    '480': (640, 480),
    '720': (1280, 720),
    '1080': (1920, 1080),
}
resolution = resolutions[args.resolution]

# Setup AWS
s3 = boto3.client('s3')
bucket = 'pi-demo-raw'
key = 'jpg/test.jpg'
loop_length = 10


def upload_to_s3(stream):
    logger.info("Uploading to S3...")
    stream.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=stream.read())
    logger.info("Upload complete")


def countdown(t, message_on_finish):
    while t:
        logger.info("%s..." % t)
        time.sleep(1)
        t -= 1
    logger.info(message_on_finish)


stream = io.BytesIO()
# Init the camera
with picamera.PiCamera() as camera:
    try:
        camera.resolution = resolution
        camera.start_preview()
        countdown(3, "Snap!")
        camera.capture(stream, 'jpeg')
        upload_to_s3(stream)
    except Exception:
        logger.error("Snap Failed")
        logger.error(traceback.format_exc())
    finally:
        logger.info("Exiting. Have a nice day!")
