import s3fs
import sys

from utils.constants import AWS_ACCESS_KEY, AWS_ACCESS_KEY_ID

def connect_to_s3():
    try:
        s3 = s3fs.S3FileSystem(
            anon=False,
            key=AWS_ACCESS_KEY_ID,
            secret=AWS_ACCESS_KEY
        )
        return s3
    except Exception as e:
        print('connect fail')
        sys.exit(1)

def create_bucket_if_not_exists(s3:s3fs.S3FileSystem, bucket:str):
    try:
        if not s3.exists(bucket):
            s3.mkdir(bucket)
            print('Bucket created')
        else:
            print('Bucket already exists')
    except Exception as e:
        print('something went wrong in create bucket')
        sys.exit(1)

def upload_to_s3(s3:s3fs.S3FileSystem, file_path:str, bucket:str, s3_file_name:str):
    try:
        s3.put(file_path, bucket+'/raw/'+s3_file_name)
        print('File upload to s3')
    except FileNotFoundError as fnf:
        print('file is not found')