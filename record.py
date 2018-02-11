from multiprocessing import Pool

import botocore
import picamera
import boto3
import traceback
import argparse
import logging
from logging.config import fileConfig
from botocore.exceptions import ClientError

fileConfig('logging.ini')
logger = logging.getLogger('pi_demo')

# Gather input
parser = argparse.ArgumentParser(
    description='Start a recording on your Pi and upload it to AWS S3',
    usage='python record.py -o my_file_name -resolution 480'
)
parser.add_argument('-o', dest='outfile', type=str, required=True,
                    help='the name for the resulting h.264 recording file',)
parser.add_argument('-r', dest='resolution', type=str, choices=['360', '480','720','1080'],
                    help='the resolution for your recording', default='480',)

args = parser.parse_args()
resolutions = {
    '360': (480, 360),
    '480': (640, 480),
    '720': (1280, 720),
    '1080': (1920, 1080),
}
resolution = resolutions[args.resolution]
file_name = args.outfile

# Setup AWS
s3 = boto3.client('s3')
bucket = 'pi-demo-raw'
key = 'h264/{FileName}.h264'.format(FileName=file_name)
loop_length = 10
min_part_size = 5

def bytes_to_mb(b):
    return b / (1024 ** 2)

def find_first_header_frame(stream):
    for frame in stream.frames:
        if frame.frame_type == picamera.PiVideoFrameType.sps_header:
            return frame.position

def upload_stream_to_s3(stream):
    # Find the first header frame in the video
    first_frame = find_first_header_frame(stream)
    stream.seek(first_frame)
    data = stream.read()
    stream_size = bytes_to_mb(len(data))
    # Write the stream to S3
    if stream_size > min_part_size:
        # Reset the stream to the first header
        stream.seek(first_frame)
        upload_stream_to_s3_as_multipart(stream)
    else:
        logger.info("Uploading to S3...")
        s3.put_object(Bucket=bucket, Key=key, Body=data)
    logger.info("Upload complete")

def upload_stream_to_s3_as_multipart(stream):
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
        logger.info("Upload complete. Total parts: {Length}. {Bytes} bytes uploaded".format(Length=len(parts), Bytes=total_size))
    except ClientError as e:
        # Abort the mpu if it fails
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        logger.error("Upload failed")
        raise e


# Init the camera
with picamera.PiCamera() as camera:
    # Create a stream that will be recycled every 30 secs
    with picamera.PiCameraCircularIO(camera, seconds=loop_length) as stream:
        try:
            # Start Recording
            camera.resolution = resolution
            camera.framerate = 24
            camera.start_recording(stream, format='h264', quality=25, bitrate=750000)
            logger.info("Stream initiated. Use Ctrl + C to end stream")
            while True:
                camera.wait_recording(loop_length)
        except KeyboardInterrupt:
            logger.info("Stream stopped by user")
            # Finish recording
            camera.stop_recording()
            upload_stream_to_s3(stream)
        except Exception as e:
            logger.error(traceback.format_exc())
        finally:
            logger.info("Exiting. Have a nice day!")
