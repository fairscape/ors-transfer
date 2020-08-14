#© 2020 By The Rector And Visitors Of The University Of Virginia

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
import requests,stardog,json
import pandas as pd
import os

ORS_URL = os.environ.get("ORS_URL", "http://ors.uvadcos.io/")

def valid_namespace(ns):
    return True

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

import random
import string



def mint_identifier(meta,ARK_NS,qualifier = False,auth_header=None):

    if qualifier:
        url = ORS_URL + 'ark:' + ARK_NS + '/' + qualifier + '/' + random_alphanumeric_string(30)
    else:
        url = ORS_URL + 'shoulder/ark:' + ARK_NS

    #Create Identifier for each file uploaded
    r = requests.post(
            url,
            data=json.dumps(meta),
            headers={"Authorization": auth_header}
            )

    if 'created' in r.json().keys():
        id = r.json()['created']
        return id

    else:
        print(r.json())
        return 'error'


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
    '''
    Method to create a named graph in stardog
    '''

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

    return
