#!/usr/bin/env python
import argparse
import logging
import traceback
from logging.config import fileConfig

import boto3
import picamera
from botocore.exceptions import ClientError

from utils import check_arg_in_range, get_resolution, bytes_to_mb

fileConfig('logging.ini')
logger = logging.getLogger('pi_demo')

# Setup Program
parser = argparse.ArgumentParser(
    description='Start a recording on your Pi and upload it to AWS S3',
    usage='python record.py -o my_file_name -resolution 480')
parser.add_argument('-o', dest='outfile', type=str, required=True,
                    help='the name (without format suffix) for the resulting h.264 recording file', )
parser.add_argument('-r', dest='resolution', type=str, choices=['360', '480', '720', '1080'],
                    help='the resolution for your recording', default='480', )
parser.add_argument('-s', dest='storage_handler', type=str, required=False, default='s3', choices=['s3'],
                    help='the storage destination for the snap', )
parser.add_argument('-q', dest='quality', type=int, default=25,
                    help='the output quality where 40 is lowest and 1 is highest', )
parser.add_argument('-l', dest='loop_length', type=int, default=10,
                    help='the loop length to keep in memory. max is 60', )
parser.add_argument('-br', dest='bitrate', type=int, default=17000000,
                    help='the bitrate limit. max is 25000000', )
parser.add_argument('-f', dest='framerate', type=int, default=24,
                    help='the framerate for the recording', )
# Gather input
args = parser.parse_args()
bitrate = args.bitrate
quality = args.quality
framerate = args.framerate
loop_length = args.loop_length
resolution = get_resolution(args.resolution)
file_name = args.outfile

# Validate input
check_arg_in_range(framerate, max=None, min=1)
check_arg_in_range(bitrate, max=25000000, min=1)
check_arg_in_range(quality, max=40, min=1)
check_arg_in_range(loop_length, max=60)

# Configure AWS
s3 = boto3.client('s3')
""":type: pyboto3.s3"""
bucket = 'pi-demo-raw'
key = 'h264/{FileName}.h264'.format(FileName=file_name)
min_part_size = 5


def find_first_header_frame(stream):
    for frame in stream.frames:
        if frame.frame_type == picamera.PiVideoFrameType.sps_header:
            return frame.position


def upload_to_s3(stream):
    # Find the first header frame in the video
    first_frame = find_first_header_frame(stream)
    stream.seek(first_frame)
    data = stream.read()
    stream_size = bytes_to_mb(len(data))
    # Write the stream to S3
    if stream_size > min_part_size:
        # Reset the stream to the first header
        stream.seek(first_frame)
        upload_as_multipart(stream)
    else:
        logger.info("Uploading to S3...")
        s3.put_object(Bucket=bucket, Key=key, Body=data)
    logger.info("Upload complete")


def upload_as_multipart(stream):
    parts = []
    multipart_upload = s3.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = multipart_upload["UploadId"]
    logger.info("Uploading video as multipart upload. S3 Multipart upload ID: {Id}".format(Id=upload_id))
    part_number = 0
    total_size = 0
    part_buffer = ''
    while True:
        buf = stream.read1()
        if not buf:
            break
        part_buffer += buf
        if bytes_to_mb(len(part_buffer)) >= min_part_size:
            # Exceeded the threshold for a part so flush it S3
            # TODO: Threading
            part_number += 1
            part_size = len(part_buffer)
            logger.debug("Uploading part #{Part} - {Bytes} bytes".format(Part=part_number, Bytes=part_size))
            total_size += part_size
            part_res = s3.upload_part(
                Bucket=bucket, Key=key, PartNumber=part_number, UploadId=upload_id, Body=part_buffer)
            parts.append({
                'ETag': part_res["ETag"],
                'PartNumber': part_number
            })
            part_buffer = ''

    try:
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={
            'Parts': parts
        })
        logger.info("Upload complete. Total parts: {Length}. {Bytes} bytes uploaded".format(Length=len(parts),
                                                                                            Bytes=total_size))
    except ClientError as e:
        # Abort the mpu if it fails
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        logger.error("Upload failed")
        raise e


storage_handler = {
    's3': upload_to_s3
}[args.storage_handler]
# Init the camera
with picamera.PiCamera() as camera:
    # Create a stream that will be recycled
    with picamera.PiCameraCircularIO(camera, seconds=loop_length) as stream:
        try:
            # Start Recording
            camera.resolution = resolution
            camera.framerate = framerate
            camera.start_recording(stream, format='h264', quality=quality, bitrate=bitrate)
            logger.info("Stream initiated. Use Ctrl + C to end stream")
            while True:
                camera.wait_recording(loop_length)
        except KeyboardInterrupt:
            logger.info("Stream stopped by user")
            # Finish recording
            camera.stop_recording()
            storage_handler(stream)
        except Exception as e:
            logger.error(traceback.format_exc())
        finally:
            logger.info("Exiting. Have a nice day!")
