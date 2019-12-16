import unittest
import sys
import requests
sys.path.append(".")
import uploader as up
import pandas as pd
import json

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
class test_download(unittest.TestCase):
    def setUp(self):
        self.app = up.app.test_client()

    def test_download(self):
        i = 'ark:99999/f453fd15-f39b-43d4-bdab-6fd8cdb7c6d3'
        req = self.app.post('/download-files',data = json.dumps({'Download Identifier':i}))
        fb = open('./Tests/UVA_6738_vitals.mat','rb')
        print(req.status_code)
        self.assertEqual(req.status_code,200)

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
