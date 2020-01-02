import flask
import requests
from datetime import datetime
import pandas as pd
import time
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,BucketAlreadyExists)
import json
from flask import send_file
import os
import warnings
import stardog
import flask
import re
from werkzeug.routing import PathConverter


app = flask.Flask(__name__)

ROOT_DIR = os.environ.get("ROOT_DIR", "")
ORS_MDS = os.environ.get("ORS_MDS", "http://ors.uvadcos.io/")
MINIO_SECRET = os.environ.get("MINIO_SECRET")
MINIO_KEY = os.environ.get("MINIO_KEY")

class EverythingConverter(PathConverter):
    regex = '.*?'

app.url_map.converters['everything'] = EverythingConverter


@app.route('/')
@token_redirect
def homepage():
    return flask.render_template('upload_boot.html')


@app.route('/bucket/<bucketName>',methods = ['POST', 'DELETE'])
@token_required
def bucket(bucketName):

    if len(bucketName) < 3:
        return flask.jsonify({'created':False,
                        'error':"Bucket does not exist"}),400


    # get auth token from header
    user_token = request.headers.get("Authorization").strip("Bearer ")

    # check the ability of a user to delete the bucket
    resource = "transfer:bucket:" + bucketName

    if flask.request.method == 'POST':

        # check the ability of a user to create a bucket in the transfer service
        # by looking up permissions in the auth service
        allowed = check_permission(user_token, "transfer", "transfer:createBucket")

        if not allowed:
            return flask.Response(
                response = json.dumps({"error": "User is not allowed to create a Bucket"}),
                status_code = 403
                )


        if bucket_exists(bucketName):
            return flask.Response(
                response = json.dumps({
                    "@id": bucketName,
                    'error':"Bucket Already Exists"
                }),
                status_code = 400
                )

        success, error = make_bucket(bucketName)

        if not success:
            error_response = { "@id": resource_name, "message": "Failed to create bucket", "error": error}
            return flask.Response(json.dumps(error_response), status_code = 500)

        resource_registration = register_resource(resource, user_token)

        if resource_registration.status_code != 200:
            return flask.jsonify({"@id": resource, error": "Failed to register resource with auth server"}), 500

        return jsonify({'created':True}),200

    if flask.request.method == 'DELETE':

        allowed = check_permission(user_token, resource, "transfer:deleteBucket")

        if not allowed:
            return flask.jsonify({"error": "User lacks permission to delete resource"}), 403

        success, error = delete_bucket(bucketName)

        if not success:
            return flask.Response(
                    response = json.dumps({
                        "@id": bucketName,
                        "message": "Failed to Delete Bucket",
                        "error": error
                    }),
                    status_code = 400
                    )

         resource_response = delete_resource(user_token, resource_name)

        return flask.Response(response=json.{'deleted':True})


@app.route('/data/<everything:ark>',methods = ['POST','GET','DELETE','PUT'])
@token_required
def all(ark):

    if flask.request.method == 'GET':

        accept = gather_accepted(request.headers.getlist('accept'))

        ark = request.args.get('ark')

        if not valid_ark(ark):
            return flask.jsonify({"error":"Improperly formatted Identifier"}), 400

        r = requests.get(ORS_MDS + ark)

        metareturned = r.json()

        if 'error' in metareturned.keys():
            return flask.jsonify({'error':"Identifier does not exist"}), 400

        location = get_file(metareturned['distribution'])

        filename = location.split('/')[-1]

        result = send_file(ROOT_DIR + '/app/' + filename)

        os.remove(ROOT_DIR + '/app/' + filename)

        return result

    if flask.request.method == 'POST':
        accept = request.headers.getlist('accept')

        if len(accept) > 0:
            accept = accept[0]


        if 'metadata' not in request.files.keys():

            error = "Missing Metadata File. Must pass in file with name metadata"

            return flask.jsonify({'uploaded':False,"Error":error}),400

        if 'files' not in request.files.keys() and 'data-file' not in request.files.keys():

            error = "Missing Data Files. Must pass in at least one file with name files"

            return flask.jsonify({'uploaded':False,"Error":error}),400

        files, meta, folder = getUserInputs(request.files,request.form)

        if 'bucket' in meta.keys():
            bucket = meta['bucket']
        else:
            bucket = 'breakfast'

        valid, error = validate_inputs(files,meta)

        if not valid:

            return flask.jsonify({'uploaded':False,"Error":error}),400


        upload_failures = []
        minted_ids = []
        failed_to_mint = []

        least_one = False
        full_upload = False

        for file in files:

            start_time = datetime.fromtimestamp(time.time()).strftime("%A, %B %d, %Y %I:%M:%S")

            file_name = file.filename.split('/')[-1]

            current_id = mint_identifier(meta)

            file_data = file

            file_name = current_id.split('/')[1]

            result = upload(file,file_name,bucket,folder)

            success = result['upload']


            if success:

                obj_hash = get_obj_hash(file_name,folder)

                end_time = datetime.fromtimestamp(time.time()).strftime("%A, %B %d, %Y %I:%M:%S")

                location = result['location']

                activity_meta = {
                    "@type":"eg:Activity",
                    "dateStarted":start_time,
                    "dateEnded":end_time,
                    "eg:usedSoftware":"Transfer Service",
                    'eg:usedDataset':file_name,
                    "identifier":[{"@type": "PropertyValue", "name": "md5", "value": obj_hash}]
                }

                act_id = mint_identifier(activity_meta)

                file_meta = meta

                file_meta['eg:generatedBy'] = act_id

                file_meta['distribution'] = []

                f = file_data
                f.seek(0, os.SEEK_END)
                size = f.tell()

                file_meta['distribution'].append({
                    "@type":"DataDownload",
                    "name":file_name,
                    "fileFormat":file_name.split('.')[-1],
                    "contentSize":size,
                    "contentUrl":'minionas.uvadcos.io/' + location
                })

                download_id = mint_identifier(file_meta['distribution'][0])

                file_meta['distribution'][0]['@id'] = download_id

                #Base meta taken from user

                if current_id != 'error':

                    least_one = True

                    minted_id = current_id

                    file_meta['@id'] = minted_id

                    minted_ids.append(minted_id)

                    create_named_graph(file_meta,minted_id)

                    eg = make_eg(minted_id)

                    r = requests.put('http://ors.uvadcos.io/' + minted_id,
                                    data=json.dumps({'eg:evidenceGraph':eg,
                                    'eg:generatedBy':activity_meta,
                                    'distribution':file_meta['distribution']}))

                else:

                    failed_to_mint.append(file.filename)

            else:

                upload_failures.append(file.filename)

        if len(upload_failures) == 0:

            full_upload = True

        if least_one:

            if len(minted_ids) > 0:

                minted_id = current_id

                if 'text/html' in accept:

                    return render_template('success.html',id = minted_id)

                return flask.jsonify({'All files uploaded':full_upload,
                                'failed to upload':upload_failures,
                                'Minted Identifiers':minted_ids,
                                'Failed to mint Id for':failed_to_mint
                                }),200

            else:

                return flask.jsonify({'All files uploaded':full_upload,
                                'failed to upload':upload_failures,
                                'Minted Identifiers':minted_ids,
                                'Failed to mint Id for':failed_to_mint
                                }),200
        if 'text/html' in accept:

            return flask.render_template('failure.html')

        return flask.jsonify({'error':'Files failed to upload.'}),400


    if flask.request.method == 'PUT':

        accept = gather_accepted(request.headers.getlist('accept'))

        if not valid_ark(ark):

            return jsonify({"error":"Improperly formatted Identifier"}),400

        r = requests.get('http://ors.uvadcos.io/' + ark)

        meta = r.json()


        if 'files' not in request.files.keys() and 'data-file' not in request.files.keys():

            error = "Missing Data Files. Must pass in at least one file with name files"

            return jsonify({'uploaded':False,"Error":error}),400

        files = request.files.getlist('files')

        if 'bucket' in meta.keys():
            bucket = meta['bucket']
        else:
            bucket = 'breakfast'
        if 'folder' in meta.keys():
            folder = meta['folder']
        else:
            folder = ''

        valid, error = validate_inputs(files,meta)

        if not valid:

            return jsonify({'uploaded':False,"Error":error}),400

        if 'version' in meta.keys():
            meta['version'] = meta['version'] + 1
        else:
            meta['version'] = 2.0

        meta['isBasedOn'] = ark
        if 'eg:evidenceGraph' in meta.keys():
            del meta['eg:evidenceGraph']
        upload_failures = []
        minted_ids = []
        failed_to_mint = []

        least_one = False
        full_upload = False

        for file in files:

            start_time = datetime.fromtimestamp(time.time()).strftime("%A, %B %d, %Y %I:%M:%S")

            file_name = file.filename.split('/')[-1]

            current_id = mint_identifier(meta)

            file_data = file

            file_name = current_id.split('/')[1]

            result = upload(file,file_name,bucket,folder)

            success = result['upload']


            if success:

                obj_hash = get_obj_hash(file_name,folder)

                end_time = datetime.fromtimestamp(time.time()).strftime("%A, %B %d, %Y %I:%M:%S")

                location = result['location']

                activity_meta = {
                    "@type":"eg:Activity",
                    "dateStarted":start_time,
                    "dateEnded":end_time,
                    "eg:usedSoftware":"Transfer Service",
                    'eg:usedDataset':file_name,
                    "identifier":[{"@type": "PropertyValue", "name": "md5", "value": obj_hash}]
                }

                act_id = mint_identifier(activity_meta)

                file_meta = meta

                file_meta['eg:generatedBy'] = act_id

                file_meta['distribution'] = []

                f = file_data
                f.seek(0, os.SEEK_END)
                size = f.tell()

                file_meta['distribution'].append({
                    "@type":"DataDownload",
                    "name":file_name,
                    "fileFormat":file_name.split('.')[-1],
                    "contentSize":size,
                    "contentUrl":'minionas.uvadcos.io/' + location
                })

                download_id = mint_identifier(file_meta['distribution'][0])

                file_meta['distribution'][0]['@id'] = download_id

                #Base meta taken from user

                if current_id != 'error':

                    least_one = True

                    minted_id = current_id

                    file_meta['@id'] = minted_id

                    minted_ids.append(minted_id)

                    create_named_graph(file_meta,minted_id)

                    eg = make_eg(minted_id)

                    r = requests.put('http://ors.uvadcos.io/' + minted_id,
                                    data=json.dumps({'eg:evidenceGraph':eg,
                                    'eg:generatedBy':activity_meta,
                                    'distribution':file_meta['distribution']}))

                else:

                    failed_to_mint.append(file.filename)

            else:

                upload_failures.append(file.filename)

        if len(upload_failures) == 0:

            full_upload = True

        if least_one:

            if len(minted_ids) > 0:

                minted_id = current_id

                if 'text/html' in accept:

                    return render_template('success.html',id = minted_id)

                return jsonify({'All files uploaded':full_upload,
                                'failed to upload':upload_failures,
                                'Minted Identifiers':minted_ids,
                                'Failed to mint Id for':failed_to_mint
                                }),200

            else:

                return jsonify({'All files uploaded':full_upload,
                                'failed to upload':upload_failures,
                                'Minted Identifiers':minted_ids,
                                'Failed to mint Id for':failed_to_mint
                                }),200
        if 'text/html' in accept:

            return render_template('failure.html')

        return jsonify({'error':'Files failed to upload.'}),400

    if flask.request.method == 'DELETE':

        if valid_ark(ark):

            req = requests.get("http://ors.uvadcos.io/" + ark)

            if regestiredID(req.json()):

                meta = req.json()

            else:

                return flask.jsonify({"error":"Given Identifier not regesited"}),400

        else:
            return flask.jsonify({"error":"Improperly formatted Identifier"}),400

        if 'distribution' in meta.keys():
            if isinstance(meta['distribution'],list):
                if 'contentUrl' in meta['distribution'][0].keys():

                    minioLocation = meta['distribution'][0]['contentUrl']

                else:
                    return flask.jsonify({'deleted':False,'error':"Metadata distribution Improperly formatted"}),400
            else:
                return flask.jsonify({'deleted':False,'error':"Metadata distribution Improperly formatted"}),400
        else:
            return flask.jsonify({'deleted':False,'error':"Metadata distribution Improperly formatted"}),400

        bucket = minioLocation.split('/')[1]

        location = '/'.join(minioLocation.split('/')[2:])

        success, error = remove_file(bucket,location)

        if success:

            return flask.jsonify({'deleted':True}),200

        else:

            return flask.jsonify({'deleted':False,'error':error}),400


@app.route('/download/',methods = ['GET'])
@token_required
def download_html():
    return flask.render_template('download_homepage.html')


def gather_accepted(accepted_list):
    if len(accepted_list) > 0:
        full_accepted = []
        for value in accepted_list:
            items = value.split(',')
            for item in items:
                full_accepted.append(item)
        return full_accepted
    return []


def registeredID(result):
    if 'error' in result.keys():
        return False
    return True


def valid_ark(ark):
    pattern = re.compile("ark:\d+/[\d,\w,-]+")
    if pattern.match(ark):
        return True
    return False


def remove_file(bucket,location):
    minioClient = Minio('minionas.uvadcos.io',
                    access_key=MINIO_KEY,
                    secret_key=MINIO_SECRET,
                    secure=False)

    try:
        minioClient.remove_object(bucket,location)

    except:
        return False, 'Object does not exist'

    return True, None


def bucket_exists(bucketName):

    minioClient = Minio('minionas.uvadcos.io',
                    access_key=MINIO_KEY,
                    secret_key=MINIO_SECRET,
                    secure=False)

    try:
        result = minioClient.bucket_exists(bucketName)

    except:
        return False

    return result


def make_bucket(bucketName):
    minioClient = Minio('minionas.uvadcos.io',
                    access_key=MINIO_KEY,
                    secret_key=MINIO_SECRET,
                    secure=False)
    try:
        minioClient.make_bucket(bucketName)

    except Exception as err:
        return False, str(err)

    return True, None


def delete_bucket(bucketName):
    minioClient = Minio('minionas.uvadcos.io',
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


def get_file(dist, which_file = '', gave = False):
    for file in dist:

        if 'contentUrl' not in file.keys():
            continue

        py_url = file['contentUrl']

        if 'minionas' not in py_url:
            continue

        if which_file not in py_url and gave:
            continue

        py_bucket = py_url.split('/')[1]
        py_location = '/'.join(py_url.split('/')[2:])
        py_full = '/'.join(py_url.split('/')[1:])

        download_script(py_bucket,py_location)

        download = True

        return py_location


def getUserInputs(requestFiles,requestForm):

    files = requestFiles

    if files['metadata'].filename != '':

        try:

            meta = json.loads(files['metadata'].read())

        except:

            meta = {'usererror in upload':'not able to make json'}

        if 'folder' in meta.keys():

            folder = meta['folder'] + '/'

        else:

            folder = ''
    else:

        meta = requestForm
        meta = meta.to_dict(flat=True)
        folder = meta['folder'] + '/'

    if 'files' in requestFiles.keys():

        files = requestFiles.getlist('files')


    elif 'data-file' in files.keys():

        files = requestFiles.getlist("data-file")

        folder_data = requestForm
        folder = folder_data['folder'] + '/'

        if folder == '/':
            folder = ''

    return files, meta, folder


def validate_inputs(files,meta):

    if meta == {}:

        return False, "Missing Metadata"
    elif 'usererror in upload' in meta.keys():

        return False, "Metadata not json"

    if len(files) == 0:

        return False, "Submit at least one file"

    return True,''


def mint_identifier(meta):

    url = 'http://ors.uvadcos.io/shoulder/ark:99999'

    #Create Identifier for each file uploaded
    r = requests.post(url, data=json.dumps(meta))

    if 'created' in r.json().keys():
        id = r.json()['created']
        return id
    else:

        return 'error'


def download_script(bucket,location):

    minioClient = Minio('minionas.uvadcos.io',
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

    minioClient = Minio('minionas.uvadcos.io',
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

    minioClient = Minio('minionas.uvadcos.io',
                    access_key= MINIO_KEY,
                    secret_key= MINIO_SECRET,
                    secure=False)

    result = minioClient.stat_object('breakfast', folder + name)

    return result.etag


def get_filename(full_path):
    return(full_path.split('/')[len(full_path.split('/')) -1 ])


def build_evidence_graph(data,clean = True):
    eg = {}
    context = {
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#':'@',
          'http://schema.org/':'',
           'http://example.org/':'eg:',
           "https://wf4ever.github.io/ro/2016-01-28/wfdesc/":'wfdesc:'
          }

    trail = []

    for index, row in data.iterrows():
        if pd.isna(row['x']):
            trail = []
            continue
        if clean:
            for key in context:

                if key in row['p']:
                    row['p'] = row['p'].replace(key,context[key])
                if key in row['y']:
                    row['y'] = row['y'].replace(key,context[key])

        if '@id' not in eg.keys():
            eg['@id'] = row['x']

        if trail == []:
            if row['p'] not in eg.keys():
                eg[row['p']] = row['y']
            else:
                trail.append(row['p'])
                if not isinstance(eg[row['p']],dict):
                    eg[row['p']] = {'@id':row['y']}

            continue
        current = eg
        for t in trail:
            current = current[t]
        if row['p'] not in current.keys():
                current[row['p']] = row['y']
        else:
            trail.append(row['p'])
            if not isinstance(current[row['p']],dict):
                current[row['p']] = {'@id':row['y']}
    return eg


def stardog_eg_csv(ark):
    conn_details = {
        'endpoint': 'http://stardog.uvadcos.io',
        'username': 'admin',
        'password': 'admin'
    }
    with stardog.Connection('db', **conn_details) as conn:
        conn.begin()
    #results = conn.select('select * { ?a ?p ?o }')
        results = conn.paths("PATHS START ?x=<"+ ark + "> END ?y VIA ?p",content_type='text/csv')
    with open(ROOT_DIR + '/star/test.csv','wb') as f:
        f.write(results)

    return


def make_eg(ark):
    stardog_eg_csv(ark)
    data = pd.read_csv(ROOT_DIR + '/star/test.csv')
    eg = build_evidence_graph(data)
    clean_up()
    return eg


def create_named_graph(meta,id):
    with open(ROOT_DIR + '/star/meta.json','w') as f:
        json.dump(meta, f)
    conn_details = {
        'endpoint': 'http://stardog.uvadcos.io',
        'username': 'admin',
        'password': 'admin'
    }
    with stardog.Connection('db', **conn_details) as conn:
        conn.begin()
        conn.add(stardog.content.File(ROOT_DIR + "/star/meta.json"),graph_uri='http://ors.uvadcos/'+id)
        conn.commit()
    # cmd = 'stardog data add --named-graph http://ors.uvadcos.io/' + id + ' -f JSONLD test "/star/meta.json"'
    # test = os.system(cmd)
    # warnings.warn('Creating named graph returned: ' + str(test))
    return


def clean_up():
    os.system('rm ' + ROOT_DIR + '/star/*')


if __name__ == "__main__":
    app.run(host='0.0.0.0',debug = True)
