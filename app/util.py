#© 2020 By The Rector And Visitors Of The University Of Virginia

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
import os
import json
from werkzeug.routing import PathConverter
import re
from minio_funcs import *

class EverythingConverter(PathConverter):
    regex = '.*?'


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


def get_filename(full_path):
    return(full_path.split('/')[len(full_path.split('/')) -1 ])


def get_file(dist, which_file = '', gave = False):
    for file in dist:

        if 'contentUrl' not in file.keys():

            continue

        py_url = file['contentUrl']

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


def clean_up():
    os.system('rm ' + ROOT_DIR + '/star/*')
