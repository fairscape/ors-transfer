from flask import Flask, render_template, request, redirect,jsonify
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

app = Flask(__name__)

root_dir = ''

root_dir = '/Users/justinniestroy-admin/Documents'

secret_key = os.environ['MINIO_SECRET_KEY']

access_key = os.environ['MINIO_ACCESS_KEY']


@app.route('/')
def homepage():
    return render_template('upload_boot.html')

@app.route('/run-job',methods = ['GET','POST'])
def run_job():

    if flask.request.method == 'GET':
        return render_template('job_runner.html')
    if request.data == b'':
        return(jsonify({'error':"Please POST json with keys, Dataset Identifier, Job Identifier, and Main Function",'valid':False}))

    try:
        inputs = json.loads(request.data.decode('utf-8'))
    except:
        return(jsonify({'error':"Please POST JSON file",'valid':False}))

    r = requests.post(url = 'http://clarklabspark-api-test.marathon.l4lb.thisdcos.directory:5000/run-job',data = json.dumps(inputs))


    return r.content.decode()

@app.route('/delete-bucket/<bucketName>',methods = ['DELETE'])
def delete_bucket(bucketName):
    #################
    #
    #if auth
    #
    ##################

    if len(bucketName) < 3:
        return jsonify({'created':False,
                        'error':"Bucket name must be at least 3 characters"}),400


    if not bucket_exists(bucketName):
        return jsonify({'created':False,
                        'error':"Bucket does not exist"}),400

    error = delete_bucket(bucketName)

    if error != "":
        return jsonify({'created':False,
                        'error':error}),400

    return jsonify({'deleted':True})

@app.route('/create-bucket/<bucketName>',methods = ['GET'])
def create_bucket(bucketName):
    #################
    #
    #if auth
    #
    ##################

    if len(bucketName) < 3:
        return jsonify({'created':False,
                        'error':"Bucket name must be at least 3 characters"}),400

    if bucket_exists(bucketName):
        return jsonify({'created':False,
                        'error':"Bucket with that name already exists"}),400

    error = make_bucket(bucketName)

    if error != "":
        return jsonify({'created':False,
                        'error':"Probably cant connect"}),500

    return jsonify({'created':True}),200

@app.route('/download',methods = ['GET'])
def download_file_html():
    if flask.request.method == 'GET':
        return render_template('download_homepage.html')


from werkzeug.routing import PathConverter

class EverythingConverter(PathConverter):
    regex = '.*?'

app.url_map.converters['everything'] = EverythingConverter

@app.route('/download-files/<everything:download_id>',methods = ['GET','POST'])
def download_file(download_id):
    cont_type = request.content_type

    inputs = {}

    if flask.request.method == 'POST':
        if request.data == b'':
            return(jsonify({'error':"Please POST json with keys, Dataset Identifier, Job Identifier, and Main Function",'valid':False}))

        try:
            inputs = json.loads(request.data.decode('utf-8'))
        except:
            return jsonify({'error':"Please POST JSON file",'valid':False}),400
        if 'Download Identifier' in inputs.keys():
            download_id = inputs['Download Identifier']
        else:
            return jsonify({'error':"Missing required key: Download Identifier"}),400


    gave = False

    if 'distribution'in inputs:

        which_file = inputs['distribution']
        gave = True

    r = requests.get('http://ors.uvadcos.io/' + download_id)
    print(r.content.decode())
    try:

        metareturned = r.json()

    except:

        return jsonify({"error":"Improperly formatted Identifier"}),400

    if 'error' in metareturned.keys():

        return(jsonify({'error':"Identifier does not exist"})),400

    if gave:
        py_location = get_file(metareturned['distribution'],which_file,True)

    else:
        py_location = get_file(metareturned['distribution'])


    filename = py_location.split('/')[-1]
    result = send_file(root_dir + '/app/' + filename)
    os.remove(root_dir + '/app/' + filename)

    return result

@app.route('/upload-files',methods = ['GET','POST'])
def upload_files():

    accept = request.headers.getlist('accept')

    if len(accept) > 0:
        accept = accept[0]

    if flask.request.method== 'GET':

        return render_template('upload_boot.html')

    if 'metadata' not in request.files.keys():

        error = "Missing Metadata File. Must pass in file with name metadata"

        return jsonify({'uploaded':False,"Error":error}),400

    if 'files' not in request.files.keys() and 'data-file' not in request.files.keys():

        error = "Missing Data Files. Must pass in at least one file with name files"

        return jsonify({'uploaded':False,"Error":error}),400

    files, meta, folder = getUserInputs(request.files,request.form)

    valid, error = validate_inputs(files,meta)

    if not valid:

        return jsonify({'uploaded':False,"Error":error}),400


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

        result = upload(file,file_name,folder)

        success = result['upload']

        print(success)
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
            #metareturned,file_meta = mintIdentifier(meta,start_time,end_time,file_name,location,obj_hash,file_data)

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

                failed_to_mint.append(file)

        else:

            upload_failures.append(file)

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

@app.route('/delte-file/<everything:ark>',methods = ['DELETE'])
def delete_files(ark):

    if valid_ark(ark):

        if regestiredID(ark):

            meta = getObjectMetadata(ark)

        else:

            return jsonify({"error":"Given Identifier not regesited"}),400

    else:
        return jsonify({"error":"Improperly formatted Identifier"}),400

    minioLocation = meta['distribution'][0]['contentUrl']

    bucket = minioLocation.split('/')[1]

    location = '/'.join(minioLocation.split('/')[2:])

    full = '/'.join(minioLocation.split('/')[1:])

    success, error = remove_file(bucket,location)

def remove_file(bucket,location):
    minioClient = Minio('minionas.uvadcos.io',
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=False)

def bucket_exists(bucketName):

    minioClient = Minio('minionas.uvadcos.io',
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=False)

    try:

        result = minioClient.bucket_exists(bucketName)

    except:

        return False

    return result

def make_bucket(bucketName):
    minioClient = Minio('minionas.uvadcos.io',
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=False)
    try:
        minioClient.make_bucket(bucketName)

    except:

        return "Error: Probably Connection"

    return ""

def get_file(dist,which_file = '',gave = False):
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

def delete_bucket(bucketName):
    minioClient = Minio('minionas.uvadcos.io',
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=False)
    if bucketName == 'prevent' or bucketName == 'breakfast' or bucketName == 'puglia':
        return "Can't delete that bucket"

    try:

        minioClient.remove_bucket(bucketName)

    except:

        return "Minio Error Try Again"

    return ""

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

def mintIdentifier(meta,start_time,end_time,file_name,location,obj_hash,file_data):
        file_meta = meta

        #add in prov about upload process
        file_meta['eg:generatedBy'] = {
            "@type":"eg:Activity",
            "dateStarted":start_time,
            "dateEnded":end_time,
            "eg:usedSoftware":"Transfer Service",
            'eg:usedDataset':file_name,
            "identifier":[{"@type": "PropertyValue", "name": "md5", "value": obj_hash}]
        }

        url = 'http://ors.uvadcos.io/shoulder/ark:99999'

        #Create Identifier for each file uploaded
        r = requests.post(url, data=json.dumps(file_meta))

        file_meta['eg:generatedBy']['@id'] = r.json()['created']

        least_one = True

        file_meta['distribution'] = []

        if 'distribution' not in file_meta.keys():
            file_meta['distribution'] = []

        elif not isinstance(file_meta['distribution'],list):
            file_meta['distribution'] = [file_meta['distribution']]

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

        url = 'http://ors.uvadcos.io/shoulder/ark:99999'

        r = requests.post(url, data=json.dumps(file_meta['distribution'][0]))

        download_id = r.json()['created']

        file_meta['distribution'][0]['@id'] = download_id

        r = requests.post(url, data=json.dumps(file_meta))

        metareturned = r.json()

        return metareturned,file_meta

def download_script(bucket,location):

    minioClient = Minio('minionas.uvadcos.io',
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=False)

    data = minioClient.get_object(bucket, location)
    file_name = location.split('/')[-1]

    with open(root_dir + '/app/' + file_name, 'wb') as file_data:
            for d in data.stream(32*1024):
                file_data.write(d)

    return './' + file_name

def upload(f,name,folder = ''):

    #filename = get_filename(file)

    minioClient = Minio('minionas.uvadcos.io',
                    access_key='breakfast',
                    secret_key='breakfast',
                    secure=False)

    f.seek(0, os.SEEK_END)
    size = f.tell()
    f.seek(0)
    if size == 0:
        return {'upload':False,'error':"Empty File"}
    # try:

    minioClient.put_object('breakfast', folder + name, f, size)

    # except ResponseError as err:
    #
    #     return {'upload':False}

    #f.save(secure_filename(f.filename))
    return {'upload':True,'location':'breakfast/' + folder + name}

def get_obj_hash(name,folder = ''):

    minioClient = Minio('minionas.uvadcos.io',
                    access_key=access_key,
                    secret_key=secret_key,
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
    with open(root_dir + '/star/test.csv','wb') as f:
        f.write(results)

    return

def make_eg(ark):
    stardog_eg_csv(ark)
    data = pd.read_csv(root_dir + '/star/test.csv')
    eg = build_evidence_graph(data)
    clean_up()
    return eg

def create_named_graph(meta,id):
    with open(root_dir + '/star/meta.json','w') as f:
        json.dump(meta, f)
    conn_details = {
        'endpoint': 'http://stardog.uvadcos.io',
        'username': 'admin',
        'password': 'admin'
    }
    with stardog.Connection('db', **conn_details) as conn:
        conn.begin()
        conn.add(stardog.content.File(root_dir + "/star/meta.json"),graph_uri='http://ors.uvadcos/'+id)
        conn.commit()
    # cmd = 'stardog data add --named-graph http://ors.uvadcos.io/' + id + ' -f JSONLD test "/star/meta.json"'
    # test = os.system(cmd)
    # warnings.warn('Creating named graph returned: ' + str(test))
    return

def clean_up():
    os.system('rm ' + root_dir + '/star/*')

if __name__ == "__main__":
    app.run(host='0.0.0.0',debug = True)
