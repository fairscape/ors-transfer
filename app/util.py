import os
from werkzeug.routing import PathConverter


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


def clean_up():
    os.system('rm ' + ROOT_DIR + '/star/*')
