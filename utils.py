import argparse
import logging
import time

logger = logging.getLogger('pi_demo')


def check_arg_in_range(value, min=1, max=100):
    ivalue = int(value)
    if (min is not None and ivalue < min) or (max is not None and ivalue > max):
        raise argparse.ArgumentTypeError("%s is an invalid value" % value)
    return ivalue


def get_resolution(res_name):
    return {
        '360': (480, 360),
        '480': (640, 480),
        '720': (1280, 720),
        '1080': (1920, 1080),
    }[res_name]


def countdown(t, message_on_finish):
    while t:
        logger.info("%s..." % t)
        time.sleep(1)
        t -= 1
    logger.info(message_on_finish)


def bytes_to_mb(b):
    return b / (1024 ** 2)
