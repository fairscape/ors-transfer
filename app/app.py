from flask import Flask, render_template, request, redirect,jsonify
import requests
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,BucketAlreadyExists)
import json
import os

app = Flask(__name__)

@app.route('/')
def homepage():

    #if more metadata is required add to metadata.html in templates
    return 'hi'

@app.route('/upload-files',methods = ['POST'])
def post_wf():


    files = request.files

    meta = json.loads(files['metadata'].read())
    upload_failures = []
    least_one = False
    full_upload = False
    print(meta)

    for file in files:

        if file == 'metadata':
            continue
        success, location = upload(files[file],file,'')

        if success:
            least_one = True

            if 'distribution' not in meta.keys():
                meta['distribution'] = []

            elif not isinstance(meta['distribution'],list):
                meta['distribution'] = [meta['distribution']]

            f = files[file]
            f.seek(0, os.SEEK_END)
            size = f.tell()

            meta['distribution'].append({
                "@type":"DataDownload",
                "name":file,
                "fileFormat":file.split('.')[-1],
                "contentSize":size,
                "contentUrl":location
            })

        else:
            upload_failures.append(file)

    if len(upload_failures) == 0:
        full_upload = True

    if least_one:

        url = 'http://ors.uvadcos.io/shoulder/ark:99999'
        r = requests.post(url, data=json.dumps(meta))
        metareturned = r.json()
        # metareturned = {'created':'1232'}

        if 'created' in metareturned:

            minted_id = metareturned['created']

            return jsonify({'All files uploaded':full_upload,
                            'failed to upload':upload_failures,
                            'Minted Identifier':minted_id
                            })

        else:

            return jsonify({'All files uploaded':full_upload,
                            'failed to upload':upload_failures,
                            'error':'Failed to mint identifier'
                            })

    return jsonify({'error':'Files failed to upload'})

def upload(f,name,folder):

    #filename = get_filename(file)

    minioClient = Minio('minionas.int.uvadcos.io',
                    access_key='breakfast',
                    secret_key='breakfast',
                    secure=False)
    # minioClient = Minio(minio_name,
    #         access_key=minio_key,
    #         secret_key=minio_secret,
    #         secure=False)
    # minioClient = Minio('127.0.0.1:9000',
    #     access_key='92WUKA7ZAP4M3UOS0TNG',
    #     secret_key='uIgJzgatEyop9ZKWfRDSlgkAhDtOzJdF+Jw+N9FE',
    #     secure=False)

    f.seek(0, os.SEEK_END)
    size = f.tell()
    f.seek(0)

    try:
           minioClient.put_object('breakfast', folder +name, f,size)

    except ResponseError as err:
           return False

    #f.save(secure_filename(f.filename))
    return {'upload':True,'location':'breakfast' + folder + '/'+ name}

def get_filename(full_path):
    return(full_path.split('/')[len(full_path.split('/')) -1 ])

if __name__ == "__main__":
    app.run(host='0.0.0.0')
