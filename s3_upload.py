#!/usr/bin/env python3

# File name: s3_upload.py
# Description: Exchange-currency-API
# Author: Viktor Vodnev
# Date: 07-01-2021

import boto3
import pathlib
import hashlib
import argparse

def arguments():
    parser = argparse.ArgumentParser(description='Start parsing arguments')
    parser.add_argument('--s3url', type=str, help='Provide a link to the s3 storage', required=True)
    parser.add_argument('--s3key', type=str, help='KEY for s3 Storage', required=True)
    parser.add_argument('--s3secret', type=str, help='SECRET for s3 Storage', required=True)
    parser.add_argument('-f', type=str, help='Path to file to upload', required=True)
    parser.add_argument('--s3path', type=str, help='Path to s3 object', required=True)
    args = parser.parse_args()
    return args

def upload(file_path, s3url, s3_path, S3KEY, S3SECRET):
    
    client_kwargs = {
        'aws_access_key_id': S3KEY,
        'aws_secret_access_key':  S3SECRET,
        'endpoint_url': s3url,
        'verify': False
        #'use_ssl': True
    }
    path_split = pathlib.Path(s3_path)
    
    s3_client = boto3.client('s3', **client_kwargs)
    bucket_name = path_split.parts[0]
    remote_path = str(pathlib.Path(*path_split.parts[1:]))

    s3_client.upload_file(file_path, bucket_name, remote_path)

    md5_s3 = s3_client.head_object(
            Bucket=bucket_name,
            Key=remote_path
        )['ETag'][1:-1]
    return md5_s3

def md5_verification(file_path):
    with open(file_path, 'rb') as file_to_verif:
        bytes = file_to_verif.read()
        md5_local = hashlib.md5(bytes).hexdigest()
    return md5_local

def main():
    args = arguments();
    if upload(
        args.f, args.s3url, args.s3path, args.s3key, args.s3secret
        ) == md5_verification(args.f):
        print("MD5 verified.")
    else:
        print("MD5 verification failed!.")

if __name__ == '__main__':
    main()
