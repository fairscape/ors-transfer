#© 2020 By The Rector And Visitors Of The University Of Virginia

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
import requests,json, os, jwt
from metadata import *
from utils import *

ORS_URL = os.environ.get("ORS_URL", "http://localhost:80/")
OS_URL = os.environ.get("OS_URL", "http://localhost:80/")
AUTH_SERVICE = os.environ.get("AUTH_SERVICE", "http://clarklab.uvarc.io/auth")
KEY = os.environ.get('AUTH_KEY')

class Distribution:
    def __init__(self,metadata):


        if metadata.get('@type') == 'DataDownload':
            data_url = metadata['contentUrl']
            self.bucket = data_url.split('/')[1]
            self.file_location = '/'.join(data_url.split('/')[2:])
            self.version = 1
        elif metadata.get('@type') == 'Download':
            data_url = metadata['name']
            self.version = metadata['version']
            self.bucket = data_url.split('/')[0]
            self.file_location = '/'.join(data_url.split('/')[1:])

        else:
            self.bucket = ''
            self.file_location = ''
            self.version = ''


class File:
    def __init__(self, meta,file_data,hash,token):
        '''
        Upload Class has file and metadata about where file
        will be stored.
        '''

        if 'bucket' in meta.keys():
            self.bucket = meta['bucket']
            del meta['bucket']
        else:
            self.bucket = 'breakfast'
        if 'folder' in meta.keys():
            self.folder = meta['folder']
            del meta['folder']
        else:
            self.folder = ''
        if 'group' in meta.keys():
            self.group = meta['group']
            del meta['folder']
        else:
            self.group = []
        if 'version' in meta.keys():
            try:
                self.version = float(meta['version'])
            except:
                self.version = None
            del meta['version']
        else:
            self.version = None
        if 'namespace' in meta.keys():
            self.ns = meta['namespace']
            if not valid_namespace(self.ns):
                self.ns = '99999'
        else:
            self.ns = '99999'
        self.qualifier = False
        if 'qualifier' in meta.keys():
            self.qualifier = meta['qualifier']

        self.meta = meta
        self.sha256 = hash
        self.file_name = file_data.filename.split('/')[-1]
        self.file_data = file_data
        if self.folder != '':
            self.file_location = self.folder + '/' + self.file_name
        else:
            self.file_location = self.file_name
        self.token = token
        self.object_id = None

    def mint_object_id(self):
        self.object_id = mint_identifier(self.meta,self.ns,
                                            token = self.token)

    def get_object_version(self):

        if self.version is not None:
            return

        main_meta = retrieve_metadata(self.object_id,self.token)
        self.main_meta = main_meta

        if 'distribution' in main_meta.keys():

            if isinstance(main_meta['distribution'],dict):
                dist_metadata = retrieve_metadata(main_meta['distribution']['@id'],
                                                    self.token)
                dist = Distribution(dist_metadata)
            elif isinstance(main_meta['distribution'],list):
                dist_metadata = retrieve_metadata(main_meta['distribution'][-1]['@id'],
                                                    self.token)
                dist = Distribution(dist_metadata)
            else:
                self.version = 1.0
                return
            if dist.version == '':
                self.version = 1.0
                return
            self.version = float(dist.version) + .1
            return
        else:
            self.version = 1.0
            return

    def create_resource(self):
        if self.token is None:
            return

        json_token = jwt.decode(self.token, KEY, algorithms='HS256',audience = 'https://fairscape.org')

        resource_meta = {'@id':self.dist_id.split('/')[-1],'Owner':json_token.get('sub'),'@type':'Resource'}
        if self.group != []:
            resource_meta['group'] = self.group
        r = requests.post(AUTH_SERVICE + '/resource',data = json.dumps(resource_meta))

        resource_meta = {'@id':self.object_id.split('/')[-1],'Owner':json_token.get('sub'),'@type':'Resource'}
        if self.group != []:
            resource_meta['group'] = self.group
        r = requests.post(AUTH_SERVICE + '/resource',data = json.dumps(resource_meta))

        return

    def upload(self):
        full_location = self.object_id.split('/')[-1] + '/' + 'V' + str(self.version) + '/' +  self.file_location
        object_service_meta = {
            'file_location': full_location,
            'bucket':self.bucket,
            'file_name':self.file_name,
            'namespace':self.ns,
            'sha256':self.sha256,
            'version':self.version
        }
        resp = requests.post(
            OS_URL + 'data/' + self.object_id,
                files = {
                    'files':self.file_data,
                    'metadata':json.dumps(object_service_meta),
                },
                headers = {"Authorization": self.token}
        ).json()

        print(resp)

        self.dist_id = resp['distribution_id']

        return
    def delete_object_id(self):
        resp = requests.delete(
            ORS_URL + self.object_id,
            headers = {"Authorization": self.token}
        ).json()

        return

    def update_id(self):

        main_meta = self.main_meta
        if 'distribution' in main_meta.keys():
            if isinstance(main_meta['distribution'],list):
                dist = main_meta['distribution']
                dist.append({'@id':self.dist_id})
            else:
                dist = [main_meta['distribution']]
                dist.append({'@id':self.dist_id})
        else:
            dist = [{'@id':self.dist_id}]

        update = {'distribution':dist}

        requests.put(ORS_URL + self.object_id,data = json.dumps(update))

        return
