#!/usr/bin/env python3

# File name: s3_upload.py
# Description: Exchange-currency-API
# Author: Viktor Vodnev
# Date: 07-01-2021

import boto3
import pathlib

def upload(file_path, s3url, s3_path, S3KEY, S3SECRET):
    
    client_kwargs = {
        'aws_access_key_id': S3KEY,
        'aws_secret_access_key':  S3SECRET,
        'endpoint_url': s3url,
        'verify': False
    }
    path_split = pathlib.Path(s3_path)
    
    s3 = boto3.client('s3', **client_kwargs)
    remote_path = str(pathlib.Path(*path_split.parts[1:]))
    s3.upload_file(file_path, path_split.parts[0], remote_path)

def main():
    upload("file.txt", "http://192.168.169.12:9000", "test/test2/file.txt", "miniokey", "miniosecret")

if __name__ == '__main__':
    main()
