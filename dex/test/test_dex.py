################################################################################
#
# Copyright (c) 2012 ObjectLabs Corporation
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
################################################################################

import unittest
import pymongo
import yaml
import sys
from dex import dex
import os

TEST_URI = "mongodb://localhost:27017"
TEST_DBNAME = "dex_test"
TEST_COLLECTION = "test_collection"
TEST_LOGFILE = os.path.dirname(__file__) + "/whitebox.log"

class test_dex(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        try:
            cls._connection = pymongo.Connection()
            cls._connection.drop_database(TEST_DBNAME)
            db = cls._connection[TEST_DBNAME]
            collection = db[TEST_COLLECTION]
                
            collection.create_index("simpleIndexedField")
            collection.create_index([("complexIndexedFieldOne",
                                      pymongo.DESCENDING),
                                     ("complexIndexedFieldTwo",
                                      pymongo.DESCENDING)])
            collection.create_index([("complexIndexedFieldTen",
                                      pymongo.DESCENDING),
                                     ("complexIndexedFieldNine",
                                      pymongo.DESCENDING)])
            collection.create_index([("complexIndexedFieldOne",
                                      pymongo.DESCENDING),
                                     ("complexIndexedFieldTwo",
                                      pymongo.DESCENDING),
                                     ("complexIndexedFieldThree",
                                      pymongo.DESCENDING)])
            collection.create_index([("geoIndexedFieldOne",
                                      pymongo.GEO2D)])
        except:
            raise unittest.SkipTest('You must have a database at ' + TEST_URI + ' to run this test case. Do not run this mongod in --auth mode.')
        else:
            
            pass
    
    @classmethod
    def tearDownClass(cls):
        cls._connection.drop_database(TEST_DBNAME)
        pass
   
    def test_analyze_query(self):
        test_dex = dex.Dex(TEST_URI, False, [])
        
        test_query = "{ query: { simpleUnindexedField: null }, ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation']['index'], "{'simpleUnindexedField': 1}")
                
        test_query = "{ query: { simpleIndexedField: null }, ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation'], None, dex.pretty_json(result))
                
        test_query = "{ query: { simpleUnindexedField: {$lt: 4}}, ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation']['index'], "{'simpleUnindexedField': 1}")
                
        test_query = "{ query: { simpleIndexedField:  { $lt: 4 } }, ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation'], None)
                
        test_query = "{ query: {}, orderby: { simpleUnindexedField }, ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation']['index'], "{'simpleUnindexedField': 1}")
                
        test_query = "{ query: {}, orderby: { simpleIndexedField: 1 }, ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation'], None)
                
        test_query = "{ query: {complexUnindexedFieldOne: null, complexUnindexedFieldTwo: null }, ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation']['index'], "{'complexUnindexedFieldOne': 1, 'complexUnindexedFieldTwo': 1}")
        
        test_query = "{ query: { complexIndexedFieldOne: null, complexIndexedFieldTwo: null }, ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation'], None)
        self.assertEqual(result['indexAnalysis']['fullIndexes'][0]['index']['key'], [('complexIndexedFieldOne', -1), ('complexIndexedFieldTwo', -1), ('complexIndexedFieldThree', -1)])
                
        test_query = "{ query: { complexUnindexedFieldOne: null, complexUnindexedFieldTwo: { $lt: 4 }}, ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation']['index'], "{'complexUnindexedFieldOne': 1, 'complexUnindexedFieldTwo': 1}")
        
        test_query = "{ query: { complexIndexedFieldOne: null, complexIndexedFieldTwo: { $lt: 4 }  }, ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation'], None, dex.pretty_json(result))
                
        test_query = "{ query: { complexIndexedFieldNine: null, complexIndexedFieldTen: { $lt: 4 }  }, ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation']['index'], "{'complexIndexedFieldNine': 1, 'complexIndexedFieldTen': 1}")
        
        test_query = "{ query: {complexUnindexedFieldOne: null}, orderby: { complexUnindexedFieldTwo: 1 } , ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation']['index'], "{'complexUnindexedFieldOne': 1, 'complexUnindexedFieldTwo': 1}")
                
        test_query = "{ query: {complexIndexedFieldOne: null}, orderby: { complexIndexedFieldTwo: 1 } , ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation'], None)
        
        test_query = "{ query: {complexIndexedFieldTen: {$lt: 4}}, orderby: { complexIndexedFieldNine: 1 } , ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation']['index'], "{'complexIndexedFieldNine': 1, 'complexIndexedFieldTen': 1}")
                
        test_query = "{ query: {complexIndexedFieldThree: null, complexIndexedFieldTwo: {$lt: 4}}, orderby: { complexIndexedFieldOne: 1 } , ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation']['index'], "{'complexIndexedFieldThree': 1, 'complexIndexedFieldOne': 1, 'complexIndexedFieldTwo': 1}")
        
        test_query = "{ query: {complexIndexedFieldOne: null, complexIndexedFieldThree: {$lt: 4}}, orderby: { complexIndexedFieldTwo: 1 } , ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation'], None)
                
        test_query = "{ query: { $or: [ { orFieldOne: { $lt: 4 } }, {orFieldTwo: { $gt: 5 } }], complexUnindexedFieldOne: 'A'}, orderby: { _id: 1 }, ns: 'dex_test.test_collection' }"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation'], None)
        
        test_query = "{ query: { geoIndexedFieldOne: { $near: [50, 50] } } , ns: 'dex_test.test_collection'}"
        result = test_dex.analyze_query(TEST_URI,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)        
        self.assertEqual(result['recommendation'], None)
                
    def test_generate_query_analysis(self):
        analyzer = dex.Dex(TEST_URI, False, [])._query_analyzer

        analysis = analyzer._generate_query_analysis(yaml.load('{query: { a: null }}'), 'db', 'collection')
        self.assertEqual(analysis['fieldCount'], 1)
        self.assertTrue(analysis['supported'])
        self.assertEqual(analysis['analyzedFields'][0]['fieldName'], 'a')
        self.assertEqual(analysis['analyzedFields'][0]['fieldType'], 'EQUIV')
        analysis = analyzer._generate_query_analysis(yaml.load('{query: { a: null , b: { $lt: 4 }}}'), 'db', 'collection')
        self.assertEqual(analysis['fieldCount'], 2)
        self.assertTrue(analysis['supported'])
        self.assertEqual(analysis['analyzedFields'][0]['fieldName'], 'a')
        self.assertEqual(analysis['analyzedFields'][0]['fieldType'], 'EQUIV')
        self.assertEqual(analysis['analyzedFields'][1]['fieldName'], 'b')
        self.assertEqual(analysis['analyzedFields'][1]['fieldType'], 'RANGE')
        analysis = analyzer._generate_query_analysis(yaml.load('{query: { a: null , b: { $lt: 4 }}, orderby: {c: 1}}'), 'db', 'collection')
        self.assertEqual(analysis['fieldCount'], 3)
        self.assertTrue(analysis['supported'])
        self.assertEqual(analysis['analyzedFields'][0]['fieldName'], 'c')
        self.assertEqual(analysis['analyzedFields'][0]['fieldType'], 'SORT')
        self.assertEqual(analysis['analyzedFields'][1]['fieldName'], 'a')
        self.assertEqual(analysis['analyzedFields'][1]['fieldType'], 'EQUIV')
        self.assertEqual(analysis['analyzedFields'][2]['fieldName'], 'b')
        self.assertEqual(analysis['analyzedFields'][2]['fieldType'], 'RANGE')
        analysis = analyzer._generate_query_analysis(yaml.load('{query: { a: null , b: { $lt: 4 }, d: {$near: [50, 50]}}, orderby: {c: 1}}'), 'db', 'collection')
        self.assertEqual(analysis['fieldCount'], 4)
        self.assertFalse(analysis['supported'])
        self.assertEqual(analysis['analyzedFields'][0]['fieldName'], 'c')
        self.assertEqual(analysis['analyzedFields'][0]['fieldType'], 'SORT')
        self.assertEqual(analysis['analyzedFields'][1]['fieldName'], 'a')
        self.assertEqual(analysis['analyzedFields'][1]['fieldType'], 'EQUIV')
        self.assertEqual(analysis['analyzedFields'][2]['fieldName'], 'b')
        self.assertEqual(analysis['analyzedFields'][2]['fieldType'], 'RANGE')
        self.assertEqual(analysis['analyzedFields'][3]['fieldName'], 'd')
        self.assertEqual(analysis['analyzedFields'][3]['fieldType'], 'UNSUPPORTED')
                
    def test_generate_index_report(self):
        analyzer = dex.Dex(TEST_URI, False, [])._query_analyzer

        index = {"key": [ ("complexIndexedFieldOne", -1), ("complexIndexedFieldTwo", -1)],
                 "v": 1}
        analysis = {'supported': True,
                    'analyzedFields': [{'fieldName': 'simpleUnindexedField', 'fieldType': 'EQUIV'}],
                    'fieldCount': 1}
        report = analyzer._generate_index_report(index, analysis)
        self.assertEqual(report['queryFieldsCovered'], 0)
        self.assertEqual(report['index'], index)
        self.assertEqual(report['coverage'], 'none')

        index = {"key": [ ("complexIndexedFieldOne", -1), ("complexIndexedFieldTwo", -1)],
                 "v": 1}
        analysis = {'supported': True,
                    'analyzedFields': [{'fieldName': 'complexIndexedFieldTwo', 'fieldType': 'EQUIV'}],
                    'fieldCount': 1}
        report = analyzer._generate_index_report(index, analysis)
        self.assertEqual(report['queryFieldsCovered'], 0)
        self.assertEqual(report['index'], index)
        self.assertEqual(report['coverage'], 'none')
        
        index = {"key": [ ("complexIndexedFieldOne", -1), ("complexIndexedFieldTwo", -1)],
                 "v": 1}
        analysis = {'supported': True,
                    'analyzedFields': [{'fieldName': 'complexIndexedFieldOne', 'fieldType': 'EQUIV'}],
                    'fieldCount': 1}
        report = analyzer._generate_index_report(index, analysis)
        self.assertEqual(report['queryFieldsCovered'], 1)
        self.assertEqual(report['index'], index)
        self.assertEqual(report['coverage'], 'full')

        index = {"key": [ ("complexIndexedFieldOne", -1), ("complexIndexedFieldTwo", -1)],
                 "v": 1}
        analysis = {'supported': True,
                    'analyzedFields': [{'fieldName': 'complexIndexedFieldTwo', 'fieldType': 'EQUIV'},
                                       {'fieldName': 'complexIndexedFieldOne', 'fieldType': 'RANGE'}],
                    'fieldCount': 2}
        report = analyzer._generate_index_report(index, analysis)
        self.assertEqual(report['queryFieldsCovered'], 2)
        self.assertEqual(report['index'], index)
        self.assertEqual(report['coverage'], 'full')
        self.assertFalse(report['idealOrder'])

        index = {"key": [ ("complexIndexedFieldOne", -1), ("complexIndexedFieldTwo", -1)],
                 "v": 1}
        analysis = {'supported': True,
                    'analyzedFields': [{'fieldName': 'complexIndexedFieldTwo', 'fieldType': 'RANGE'},
                                       {'fieldName': 'complexIndexedFieldOne', 'fieldType': 'EQUIV'}],
                    'fieldCount': 2}
        report = analyzer._generate_index_report(index, analysis)
        self.assertEqual(report['queryFieldsCovered'], 2)
        self.assertEqual(report['index'], index)
        self.assertEqual(report['coverage'], 'full')
        self.assertTrue(report['idealOrder'])

        index = {"key": [ ("complexIndexedFieldOne", -1), ("complexIndexedFieldTwo", -1)],
                 "v": 1}
        analysis = {'supported': True,
                    'analyzedFields': [{'fieldName': 'complexIndexedFieldTwo', 'fieldType': 'RANGE'},
                                       {'fieldName': 'complexIndexedFieldOne', 'fieldType': 'SORT'}],
                    'fieldCount': 2}
        report = analyzer._generate_index_report(index, analysis)
        self.assertEqual(report['queryFieldsCovered'], 2)
        self.assertEqual(report['index'], index)
        self.assertEqual(report['coverage'], 'full')
        self.assertTrue(report['idealOrder'])

    def test_report_aggregation(self):
        test_dex = dex.Dex(TEST_URI, True, [])
        report = test_dex._full_report._reports

        test_query = "{ query: { simpleUnindexedField: null }, " \
                     "ns: 'dex_test.test_collection', 'millis': 150}"
        result = test_dex.analyze_query(None,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)
        test_dex._full_report.add_report(result)
        self.assertEqual(len(report), 1)
        self.assertEqual(report[0]['queryCount'], 1)
        self.assertEqual(len(report[0]['queriesCovered']), 1)

        test_query = "{ query: {}, orderby: { simpleUnindexedField: null }," \
                     "ns: 'dex_test.test_collection', 'millis': 50}"
        result = test_dex.analyze_query(None,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)
        test_dex._full_report.add_report(result)

        self.assertEqual(len(report), 1)
        self.assertEqual(report[0]['queryCount'], 2)
        self.assertEqual(len(report[0]['queriesCovered']), 2)
        totalAvg = report[0]['avgTimeMillis']
        self.assertEqual(totalAvg, 100)

        test_query = "{ query: { anotherUnindexedField: null }, "\
                     "ns: 'dex_test.test_collection', 'millis': 100}"
        result = test_dex.analyze_query(None,
                                        yaml.load(test_query),
                                        TEST_DBNAME,
                                        TEST_COLLECTION)
        #adding twice for a double query
        test_dex._full_report.add_report(result)
        test_dex._full_report.add_report(result)

        self.assertEqual(len(report), 2)
        self.assertEqual(report[0]['queryCount'], 2)
        self.assertEqual(len(report[0]['queriesCovered']), 2)
        self.assertEqual(report[1]['queryCount'], 2)
        self.assertEqual(len(report[1]['queriesCovered']), 1)
        totalAvg = report[0]['avgTimeMillis']
        self.assertEqual(totalAvg, 100)

    if __name__ == '__main__':
        unittest.main()
