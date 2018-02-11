#!/usr/bin/env python
import argparse
import io
import logging
import traceback
from logging.config import fileConfig

import boto3
import picamera

from utils import check_arg_in_range, get_resolution, countdown

fileConfig('logging.ini')
logger = logging.getLogger('pi_demo')

# Gather input
parser = argparse.ArgumentParser(
    description='Take a camera snap on your Pi and upload it to AWS S3',
    usage='python snap.py -o my_file_name -resolution 480')
parser.add_argument('-o', dest='outfile', type=str, required=True,
                    help='the name (without format suffix) for the resulting jpg image', )
parser.add_argument('-r', dest='resolution', type=str, choices=['360', '480', '720', '1080'],
                    help='the resolution for your snap', default='480', )
parser.add_argument('-s', dest='storage_handler', type=str, required=False, default='s3', choices=['s3'],
                    help='the storage destination for the snap', )
parser.add_argument('-q', dest='quality', type=int, default=85,
                    help='the output quality where 100 is highest and 1 is lowest', )
parser.add_argument('-t', dest='delay', type=int, default=3,
                    help='the delay before taking the snap. max is 10', )

args = parser.parse_args()
quality = args.quality
delay = args.delay
check_arg_in_range(args.quality)
check_arg_in_range(args.delay, min=0, max=10)
resolution = get_resolution(args.resolution)
file_name = args.outfile

# Setup AWS
s3 = boto3.client('s3')
""":type: pyboto3.s3"""
bucket = 'pi-demo-raw'
key = 'jpg/{FileName}.jpg'.format(FileName=file_name)


def upload_to_s3(stream):
    logger.info("Uploading to S3...")
    stream.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=stream.read())
    logger.info("Upload complete")


storage_handler = {
    's3': upload_to_s3
}[args.storage_handler]
stream = io.BytesIO()
# Init the camera
with picamera.PiCamera() as camera:
    try:
        camera.resolution = resolution
        camera.start_preview()
        countdown(delay, "Snap!")
        camera.capture(stream, 'jpeg', quality=quality)
        storage_handler(stream)
    except Exception:
        logger.error("Snap Failed")
        logger.error(traceback.format_exc())
    finally:
        logger.info("Exiting. Have a nice day!")
