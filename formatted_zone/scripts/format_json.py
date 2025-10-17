import sys
import os
import boto3
import logging
from botocore.exceptions import ClientError

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../global_scripts'))
sys.path.append(parent_dir)
from utils import *
from consts import *

setup_logging("format_json.log")


def main():
    pass


if __name__ == '__main__':
    main()