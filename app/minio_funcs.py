from minio import Minio
import os
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,BucketAlreadyExists)

MINIO_URL = os.environ.get("MINIO_URL", "minionas.uvadcos.io")
MINIO_SECRET = os.environ.get("MINIO_SECRET")
MINIO_KEY = os.environ.get("MINIO_KEY")

def remove_file(bucket,location):
    minioClient = Minio(MINIO_URL,
                    access_key=MINIO_KEY,
                    secret_key=MINIO_SECRET,
                    secure=False)

    try:
        minioClient.remove_object(bucket,location)

    except:
        return False, 'Object does not exist'

    return True, None


def bucket_exists(bucketName):

    minioClient = Minio(MINIO_URL,
                    access_key=MINIO_KEY,
                    secret_key=MINIO_SECRET,
                    secure=False)

    try:
        result = minioClient.bucket_exists(bucketName)

    except:
        return False

    return result


def make_bucket(bucketName):
    minioClient = Minio(MINIO_URL,
                    access_key=MINIO_KEY,
                    secret_key=MINIO_SECRET,
                    secure=False)
    try:
        minioClient.make_bucket(bucketName)

    except Exception as err:
        return False, str(err)

    return True, None


def delete_bucket(bucketName):
    minioClient = Minio(MINIO_URL,
                    access_key=MINIO_KEY,
                    secret_key=MINIO_SECRET,
                    secure=False)

    if bucketName == 'prevent' or bucketName == 'breakfast' or bucketName == 'puglia':
        return "Can't delete that bucket"

    try:
        minioClient.remove_bucket(bucketName)

    except Exception as err:
        return False, str(err)

    return True, None


def download_script(bucket,location):

    minioClient = Minio(MINIO_URL,
                    access_key= MINIO_KEY,
                    secret_key= MINIO_SECRET,
                    secure=False)
    data = minioClient.get_object(bucket, location)
    file_name = location.split('/')[-1]

    with open(ROOT_DIR + '/app/' + file_name, 'wb') as file_data:
            for d in data.stream(32*1024):
                file_data.write(d)

    return './' + file_name


def upload(f,name,bucket,folder = ''):
    #filename = get_filename(file)
    minioClient = Minio(MINIO_URL,
                    access_key= MINIO_KEY,
                    secret_key= MINIO_SECRET,
                    secure=False)
    f.seek(0, os.SEEK_END)
    size = f.tell()
    f.seek(0)
    if size == 0:
        return {'upload':False,'error':"Empty File"}
    # try:
    minioClient.put_object(bucket, folder + name, f, size)
    # except ResponseError as err:
    #
    #     return {'upload':False}
    #f.save(secure_filename(f.filename))
    return {'upload':True,'location':'breakfast/' + folder + name}


def get_obj_hash(name,folder = ''):

    minioClient = Minio(MINIO_URL,
                    access_key= MINIO_KEY,
                    secret_key= MINIO_SECRET,
                    secure=False)

    result = minioClient.stat_object('breakfast', folder + name)

    return result.etag
