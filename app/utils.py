#© 2020 By The Rector And Visitors Of The University Of Virginia

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
from werkzeug.routing import PathConverter
import re
class EverythingConverter(PathConverter):
    regex = '.*?'

def correct_inputs(request,req_type = 'POST'):
    if 'metadata' not in request.files.keys() and req_type == 'POST':

        error = "Missing Metadata File. Must pass in json containing object metadata."

        return error, False

    if 'files' not in request.files.keys():

        error = "Missing Data File. Must pass in at least one file with name files"

        return error, False


    return '', True

def valid_meta(metadata):

    if metadata.get('@type') != 'Download' and metadata.get('@type') != 'DataDownload':
        if 'distribution' not in metadata.keys():
            return False
        if not isinstance(metadata['distribution'],list) and not isinstance(metadata['distribution'],dict):
            return False

    return True

def valid_ark(ark):
    pattern = re.compile("ark:\d+/[\d,\w,-]+")
    if pattern.match(ark):
        return True
    return False

def valid_namespace(ns):
    return True
