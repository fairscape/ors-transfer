#© 2020 By The Rector And Visitors Of The University Of Virginia

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
from metadata import *
import boto3
from botocore.client import Config
import os
import hashlib


MINIO_URL = os.environ.get("MINIO_URL", "minionas.uvadcos.io")
MINIO_SECRET = os.environ.get("MINIO_SECRET")
MINIO_KEY = os.environ.get("MINIO_KEY")
OS_URL = os.environ.get("OS_URL")

class Distribution:
    def __init__(self,metadata):

        if metadata.get('@type') == 'DataDownload':
            data_url = metadata['contentUrl']
            self.bucket = data_url.split('/')[1]
            self.file_location = '/'.join(data_url.split('/')[2:])
            self.version = 1
        elif metadata.get('@type') == 'Download':
            data_url = metadata['name']
            self.version = metadata.get('version',1.0)
            self.bucket = data_url.split('/')[0]
            self.file_location = '/'.join(data_url.split('/')[1:])

        else:
            self.bucket = ''
            self.file_location = ''
            self.version = ''

class Download:

    def __init__(self,metadata,token):

        self.token = token
        self.valid = True

        if metadata.get('@type') == 'Download' or metadata.get('@type') == 'DataDownload':
            dist = Distribution(metadata)
        else:

            if 'distribution' in metadata.keys():

                if isinstance(metadata['distribution'],dict):
                    dist_metadata = retrieve_metadata(metadata['distribution']['@id'],
                                                        self.token)
                    dist = Distribution(dist_metadata)
                elif isinstance(metadata['distribution'],list):
                    dist_metadata = retrieve_metadata(metadata['distribution'][-1]['@id'],
                                                        self.token)
                    dist = Distribution(dist_metadata)
            else:
                raise Exception

        self.bucket, self.location = dist.bucket, dist.file_location

        if self.bucket == '' or self.location == '':
            self.valid = False
            self.error = 'Distribution missing or improperly formatted.'
            return



    def download(self):
        download_location = download_file(self.bucket,self.location)
        return download_location

def download_file(bucket,location):
    file_name = location.split('/')[-1]
    minioClient = boto3.client('s3',
                        endpoint_url=MINIO_URL,
                        aws_access_key_id=MINIO_KEY,
                        aws_secret_access_key=MINIO_SECRET,
                        region_name='us-east-1')
    minioClient.download_file(bucket, location, '/' + file_name)
    return '/' + file_name
