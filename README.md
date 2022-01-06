**File upload on S3 bucket**

Python script that allows uploading file to S3 bucket with MD5 hash checking. The script works on every system (UNIX, Windows, Mac).

Run the script using command: 
`python s3_upload.py --s3url https://s3-storage:9000 --s3key ${S3KEY} --s3secret ${S3SECRET} -f "/path/to/file/to/upload" --s3path "/s3/object/path"`