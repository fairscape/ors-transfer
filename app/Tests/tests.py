import unittest
import sys
import requests
sys.path.append(".")
import app as up
import pandas as pd
import json
import io

class test_eg(unittest.TestCase):

    def test_eg_builder(self):

        data = pd.read_csv('./Tests/test.csv')

        eg = up.build_evidence_graph(data)

        with open('./Tests/result.json','r') as f:
            should_be = json.load(f)

        self.assertEqual(eg,should_be)

class test_upload(unittest.TestCase):

    def test_up(self):

        f = open('./Tests/test.csv','rb')

        result = up.upload(f,'test')

        self.assertEqual(result,{'upload':True,'location':'breakfast/test'})

class test_app(unittest.TestCase):

    def setUp(self):
        self.app = up.app.test_client()


    def test_upload_good(self):


        metadata = {
                    "@context":{
                        "@vocab":"http://schema.org/"
                    },
                    "@type":"Dataset",
                    "folder":"Test",
                    "name":"Test Dataset",
                    "description":"Spark Test Data Set",
                    "author":[
                        {
                            "name":"Justin Niestroy",
                            "@id": "https://orcid.org/0000-0002-1103-3882",
                            "affiliation":"University of Virginia"
                        }
                    ],
                }

        data = {}
        data['files'] = (io.BytesIO(b"abcdef"), 'test.jpg')
        data['metadata'] = (io.BytesIO(json.dumps(metadata).encode('utf-8')),'name')
        req = self.app.post('/upload-files',data = data,content_type='multipart/form-data')

        self.assertEqual(req.status_code,200)

    def test_upload_missing_meta(self):


        metadata = {
                    "@context":{
                        "@vocab":"http://schema.org/"
                    },
                    "@type":"Dataset",
                    "folder":"Test",
                    "name":"Test Dataset",
                    "description":"Spark Test Data Set",
                    "author":[
                        {
                            "name":"Justin Niestroy",
                            "@id": "https://orcid.org/0000-0002-1103-3882",
                            "affiliation":"University of Virginia"
                        }
                    ],
                }

        data = {}
        data['files'] = (io.BytesIO(b"abcdef"), 'test.jpg')
        #data['metadata'] = (io.BytesIO(json.dumps(metadata).encode('utf-8')),'name')
        req = self.app.post('/upload-files',data = data,content_type='multipart/form-data')

        self.assertEqual(req.status_code,400)

    def test_upload_invalid_meta(self):


        metadata = {
                    "@context":{
                        "@vocab":"http://schema.org/"
                    },
                    "@type":"Dataset",
                    "folder":"Test",
                    "name":"Test Dataset",
                    "description":"Spark Test Data Set",
                    "author":[
                        {
                            "name":"Justin Niestroy",
                            "@id": "https://orcid.org/0000-0002-1103-3882",
                            "affiliation":"University of Virginia"
                        }
                    ],
                }

        data = {}
        data['files'] = (io.BytesIO(b"abcdef"), 'test.jpg')
        data['metadata'] = (io.BytesIO(b"abcdef"), 'test.jpg')
        req = self.app.post('/upload-files',data = data,content_type='multipart/form-data')

        self.assertEqual(req.status_code,400)

    def test_upload_missing_files(self):


        metadata = {
                    "@context":{
                        "@vocab":"http://schema.org/"
                    },
                    "@type":"Dataset",
                    "folder":"Test",
                    "name":"Test Dataset",
                    "description":"Spark Test Data Set",
                    "author":[
                        {
                            "name":"Justin Niestroy",
                            "@id": "https://orcid.org/0000-0002-1103-3882",
                            "affiliation":"University of Virginia"
                        }
                    ],
                }

        data = {}
        #data['files'] = (io.BytesIO(b"abcdef"), 'test.jpg')
        data['metadata'] = (io.BytesIO(json.dumps(metadata).encode('utf-8')),'name')
        req = self.app.post('/upload-files',data = data,content_type='multipart/form-data')

        self.assertEqual(req.status_code,400)

    def test_download(self):

        i = 'ark:99999/f453fd15-f39b-43d4-bdab-6fd8cdb7c6d3'

        req = self.app.get('/download-files/' + i)

        self.assertEqual(req.status_code,200)

    def test_download_improper(self):

        i = 'ark:/99999/f453fd15-f39b-43d4-bdab-6fd8cdb7c6d3'

        req = self.app.get('/download-files/' + i)

        self.assertEqual(req.status_code,400)

    def test_download_not_real_id(self):

        i = 'ark:99999/f453fd15-f39b-43d4-bdab-6fd8cdb7c'

        req = self.app.get('/download-files/' + i)

        self.assertEqual(req.status_code,400)

    def test_create_bucket(self):

        bucket_name = 'abcdefghi'

        if not up.bucket_exists(bucket_name):

            req = self.app.get('create-bucket/' + bucket_name)

            self.assertTrue(up.bucket_exists(bucket_name))

    def test_create_bucket_too_short(self):

        bucket_name = 'ab'
        req = self.app.get('create-bucket/' + bucket_name)
        self.assertEqual(req.status_code,400)


    def test_bucket_exists(self):

        bucket = 'prevent'

        r = up.bucket_exists(bucket)

        self.assertTrue(r)

    def test_bucket_doesnt_exists(self):

        bucket = 'randombucketname'

        r = up.bucket_exists(bucket)

        self.assertFalse(r)

if __name__ == '__main__':
    unittest.main()

#
# class test_recongize_class(unittest.TestCase):
#
#     def test_all_classes(self):
#         val = validate.RDFSValidator({"@type":"Dataset"})
#         check = True
#         for clas in val.schema_properties.keys():
#             if not val.recongized_class(clas):
#                 check = False
#         self.assertTrue(check)
#
#     def test_non_schema_class(self):
#         val = validate.RDFSValidator({"@type":"Dataset"})
#         check = True
#         if val.recongized_class("MadeUpClass"):
#             check = False
#         self.assertTrue(check)
