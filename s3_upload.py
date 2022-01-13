#!/usr/bin/env python3

# File name: s3_upload.py
# Description: Exchange-currency-API
# Author: Viktor Vodnev
# Date: 07-01-2022

import boto3
import hashlib
import argparse
import logging
import re
from urllib.parse import urlparse
from botocore.exceptions import NoCredentialsError, ClientError


# arguments parsing
def arguments():
    logging.info('Start parsing arguments')
    try:
        parser = argparse.ArgumentParser(description='Start parsing arguments')
        parser.add_argument('--s3url', type=str, required=True,
                            help='Provide a link to the s3 storage')
        parser.add_argument('--s3key', type=str, required=True,
                            help='KEY for s3 Storage')
        parser.add_argument('--s3secret', type=str, required=True,
                            help='SECRET for s3 Storage')
        parser.add_argument('-f', type=str, required=True,
                            help='Path to file to upload')
        parser.add_argument('--s3path', type=str, required=True,
                            help='Path to s3 object')
        args = parser.parse_args()
    except:
        logging.exception("Error reading arguments ")
    finally:
        logging.debug("Argument reading completed successfully")
    return args


def upload(file_path, s3url, s3_path, S3KEY, S3SECRET):
    # s3 credentials
    client_kwargs = {
        'aws_access_key_id': S3KEY,
        'aws_secret_access_key':  S3SECRET,
        'endpoint_url': s3url,
        'verify': False,
        'use_ssl': True
    }
    s3_client = boto3.client('s3', **client_kwargs)

    # parsing path with regex
    if bool(re.match('^s3:\/\/(.+?)\/*', s3_path)):
        path_parse = urlparse(s3_path)
        bucket_name = path_parse.netloc
        remote_path = path_parse.path
    elif bool(re.match('^\/s3\/(.+?)\/*', s3_path)):
        bucket_name = s3_path.split('/', 3)[2]
        remote_path = '/' + s3_path.split('/', 3)[3]

    # upload fiel to s3
    try:
        s3_client.upload_file(file_path, bucket_name, remote_path)
        logging.info("Successful upload of a file")
        md5_s3 = s3_client.head_object(
                Bucket=bucket_name,
                Key=remote_path
            )['ETag'][1:-1]
        logging.info("Successful upload of the md5 checksum")
    except FileNotFoundError:
        logging.error("Local file was not found")
    except NoCredentialsError:
        logging.error("Wrong or lack of credentials")
    return md5_s3


# getting md5 hash of a local file
def md5_verification(file_path):
    try:
        with open(file_path, 'rb') as file_to_verif:
            bytes = file_to_verif.read()
            md5_local = hashlib.md5(bytes).hexdigest()
    except NoCredentialsError as err:
        md5_local = None
        logging.error(str(err))
        pass
    return md5_local


def main():
    # logging configuration
    logging.basicConfig(level=logging.DEBUG, filename='s3_upload.log',
                        format='%(asctime)s %(levelname)s:%(message)s')
    logging.info("Running the main program.")

    # compare md5 hash
    try:
        args = arguments()
        if upload(
            args.f, args.s3url, args.s3path, args.s3key, args.s3secret
                ) == md5_verification(args.f):
            logging.info("MD5 is valid.")
        else:
            logging.error("MD5 verification failed!.")
    except Exception as err:
        logging.error(str(err))

if __name__ == '__main__':
    main()
