#© 2020 By The Rector And Visitors Of The University Of Virginia

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
import flask, requests, time, json, os, warnings, re,logging,jwt
from datetime import datetime
from flask import request
from flask import send_file
from auth import *
from file_class import *
from download_class import *
from metadata import *
from utils import *
import copy


app = flask.Flask(__name__)

ROOT_DIR = os.environ.get("ROOT_DIR", "")
ORS_URL = os.environ.get("ORS_URL", "http://localhost:80/")
OS_URL = os.environ.get("OS_URL", "http://localhost:80/")
TESTING = os.environ.get("NO_AUTH",False)
EVI_PREFIX = 'evi:'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app.url_map.converters['everything'] = EverythingConverter


@app.route('/data',methods = ['POST'])
@user_level_permission
def just_upload():

    logger.info('Transfer Service handling request %s', request)

    error, valid_inputs = correct_inputs(request)
    if not valid_inputs:
        return flask.jsonify({'uploaded':False,"Error":error}),400

    try:
        meta = json.loads(request.files['metadata'].read())
    except:
        return flask.jsonify({'uploaded':False,
                        'error':'Metadata must be json file describing object.'}),400

    token = request.headers.get("Authorization",None)
    files = request.files.getlist('files')
    try:
        sha256 = request.files['sha256'].read().decode("utf-8")
    except:
        return flask.jsonify({'uploaded':False,
                        'error':'Must upload hash of object with tag sha256.'}),400

    print(sha256)
    file_to_upload = File(meta,files[0],sha256,token)
    try:
        file_to_upload.mint_object_id()
    except:
        logger.error('Failed to mint identifier.',exc_info=True)
        return flask.jsonify({'uploaded':False,
                        'error':'Minting identifers failed.'}),503

    file_to_upload.get_object_version()
    try:
        file_to_upload.upload()
    except:
        file_to_upload.delete_object_id()
        return flask.jsonify({'uploaded':False,
                        'error':'Failed to upload file.'}),503

    meta_updated = file_to_upload.update_id()
    if not meta_updated:
        logger.error('Failed to add distribution id to %s', file_to_upload.object_id)

    file_to_upload.create_resource()
    return flask.jsonify({'uploaded':True,
                    'Minted Identifiers':[file_to_upload.object_id]}),200


@app.route('/data/<everything:ark>',methods = ['POST','PUT','GET','DELETE'])
@group_get_owner_else
def rest(ark):
    if flask.request.method == 'DELETE':

        if not valid_ark(ark):
            return flask.jsonify({"error":"Improperly formatted Identifier"}), 400

        token = request.headers.get("Authorization")
        obj_req = requests.delete(OS_URL + 'data/' + ark,headers = {"Authorization": token})
        try:
            obj_resp = obj_req.json()
        except:
            return flask.jsonify({'deleted':False}), 503
        return flask.jsonify(obj_resp), obj_req.status_code


    if flask.request.method == 'POST':
        error, valid_inputs = correct_inputs(request)
        if not valid_inputs:
            return flask.jsonify({'uploaded':False,"Error":error}),400

        try:
            meta = json.loads(request.files['metadata'].read())
        except:
            return flask.jsonify({'uploaded':False,
                            'error':'Metadata must be json file describing object.'}),400

        token = request.headers.get("Authorization")
        files = request.files.getlist('files')
        try:
            sha256 = request.files['sha-256']
        except:
            sha256 = 0

        file_to_upload = File(meta,files[0],sha256,token)
        try:
            file_to_upload.mint_object_id()
        except:
            logger.error('Failed to mint identifier.',exc_info=True)
            return flask.jsonify({'uploaded':False,
                            'error':'Minting identifers failed.'}),503

        file_to_upload.get_object_version()
        try:
            file_to_upload.upload()
        except:
            file_to_upload.delete_object_id()
            return flask.jsonify({'uploaded':False,
                            'error':'Failed to upload file.'}),503

        file_to_upload.update_id()
        return flask.jsonify({'uploaded':True,
                        'Minted Identifiers':[file_to_upload.object_id]}),200
    if flask.request.method == 'GET':

        if not valid_ark(ark):
            return flask.jsonify({"error":"Improperly formatted Identifier"}), 400

        token = request.headers.get("Authorization")
        metadata = retrieve_metadata(ark,token)

        if not valid_meta(metadata):
            return flask.jsonify({'error':'Given Ark missing distribution or is not a distribution.'}),400

        try:
            download = Download(metadata,token)
        except:
            return flask.jsonify({'error':"Given ark is not a download or is missing distribution"}),400
        if not download.valid:
            return flask.jsonify({'error':download.error})

        download_location = download.download()

        result = send_file(download_location)
        os.remove(download_location)

        return result

    if flask.request.method == 'PUT':
        logger.info('Transfer Service handling request %s', request)

        if not valid_ark(ark):
            return flask.jsonify({"error":"Improperly formatted Identifier"}), 400

        error, valid_inputs = correct_inputs(request,'PUT')
        if not valid_inputs:
            return flask.jsonify({'uploaded':False,"Error":error}),400

        try:
            if 'metadata' in request.files.keys():
                meta = json.loads(request.files['metadata'].read())
            else:
                meta = {}
        except:
            return flask.jsonify({'uploaded':False,
                            'error':'Metadata must be json file describing object.'}),400

        token = request.headers.get("Authorization")
        files = request.files.getlist('files')
        try:
            sha256 = request.files['sha256']
        except:
            return flask.jsonify({'uploaded':False,
                            'error':'Must upload hash of object with tag sha256.'}),400

        file_to_upload = File(meta,files[0],sha256,token)
        file_to_upload.object_id = ark

        file_to_upload.get_object_version()

        try:
            file_to_upload.upload()
        except:
            file_to_upload.delete_object_id()
            return flask.jsonify({'updated':False,
                            'error':'Failed to upload file.'}),503

        file_to_upload.update_id()
        return flask.jsonify({'updated':True,
                        'Minted Identifiers':[file_to_upload.object_id]}),200

if __name__ == "__main__":
    if TESTING:
        app.config['TESTING'] = True
    app.run(host='0.0.0.0',port=5002)
