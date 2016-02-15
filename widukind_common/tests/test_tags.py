# -*- coding: utf-8 -*-

import unittest
from pprint import pprint
import pymongo 

from widukind_common import tags as tags_utils
from widukind_common import constants

from widukind_common.tests.base import BaseTestCase, BaseDBTestCase

class TagsUtilsTestCase(BaseTestCase):

    # nosetests -s -v widukind_common.tests.test_tags:TagsUtilsTestCase
    
    def test_tags_map(self):
        
        query = "The a France Quaterly"
        #result = sorted(tags_utils.tags_map(query))
        result = sorted(tags_utils._translate(query))
        self.assertEqual(result, ["a", "france", "quaterly", "the"])
        
        result = tags_utils._translate(None)
        #result = tags_utils.tags_map(None)
        self.assertIsNone(result)
        
    def test_str_to_tags(self):
        
        self.assertEqual(tags_utils.str_to_tags("Bank's of France"), ['bank', 'france'])        
        
        self.assertEqual(tags_utils.str_to_tags("Bank's of & France"), ['bank', 'france'])
        
        self.assertEqual(tags_utils.str_to_tags("France"), ['france'])
        
        self.assertEqual(tags_utils.str_to_tags("Bank's"), ['bank'])

        self.assertEqual(tags_utils.str_to_tags("The a France Quaterly"), ["france", "quaterly"])        

        self.assertEqual(tags_utils.str_to_tags(None), [])        
        

class GenerateTagsTestCase(BaseDBTestCase):

    # nosetests -s -v widukind_common.tests.test_tags:GenerateTagsTestCase
    
    def setUp(self):
        super().setUp()
        self.doc_provider = {
            "enable": True,
            "name": "p1",
            "long_name": "Provider Test",
            "region": "Mars",
            "slug": "p1"
        }
    
    def test_generate_tags_datasets(self):

        # nosetests -s -v widukind_common.tests.test_tags:GenerateTagsTestCase.test_generate_tags_datasets
        
        dataset = {
            "enable": True,
            "provider_name": self.doc_provider["name"],
            "dataset_code": "d1",
            "name": "dataset 1",
            "slug": "%s-d1" % self.doc_provider["slug"],
            "concepts": {
                "FREQ": "Frequency",
                "OBS_STATUS": "Observation Status",
            },
            "codelists": {
                "FREQ": {
                    "D": "Daily"
                },
                "OBS_STATUS": {
                    "E": "Estimate"
                }
            },
            "dimension_keys": ["FREQ"],
            "attribute_keys": ["OBS_STATUS"],
        } 
        
        tags = tags_utils.generate_tags_dataset(self.db, dataset, self.doc_provider)
        self.assertEqual(tags, ['d1', 'daily', 'dataset', 'estimate', 'frequency', 'mars', 'observation', 'p1', 'provider', 'status', 'test'])
        
    def test_generate_tags_series(self):

        # nosetests -s -v widukind_common.tests.test_tags:GenerateTagsTestCase.test_generate_tags_series
        
        dataset = {
            "enable": True,
            "provider_name": self.doc_provider["name"],
            "dataset_code": "d1",
            "name": "dataset 1",
            "slug": "%s-d1" % self.doc_provider["slug"],
            "concepts": {
                "FREQ": "Frequency",
                "OBS_STATUS": "Observation Status",
                "COUNTRY": "Country"
            },
            "codelists": {
                "FREQ": {
                    "D": "Daily",
                    "M": "Monthly"
                },
                "OBS_STATUS": {
                    "E": "Estimate",
                },
                "COUNTRY": {
                    "FRA": "France"
                }
            },
            "dimension_keys": ["FREQ", "COUNTRY"],
            "attribute_keys": ["OBS_STATUS"],
        }
        
        series = {
            "provider_name": self.doc_provider["name"],
            "dataset_code": "d1",
            "key": "x1",
            "name": "series 1",
            "slug": "%s-d1-x1" % self.doc_provider["slug"],
            "frequency": "M",
            "dimensions": {
                "COUNTRY": "FRA"                
            },
            "attributes": {
                "OBS_STATUS": "E"
            },
        }
        
        tags = tags_utils.generate_tags_series(self.db, series, 
                                               self.doc_provider, 
                                               dataset)
        self.assertEqual(tags, ['country', 'd1', 'estimate', 'france', 'mars', 'monthly', 'observation', 'p1', 'provider', 'series', 'status', 'test', 'x1'])

class UpdateTagsTestCase(BaseDBTestCase):
    
    # nosetests -s -v widukind_common.tests.test_tags:UpdateTagsTestCase
    
    def setUp(self):
        super().setUp()
        self.doc_provider = {
            "enable": True,
            "name": "p1",
            "long_name": "Provider Test",
            "region": "Mars",
            "slug": "p1"
        }
        
    def test_update_tags_datasets(self):

        # nosetests -s -v widukind_common.tests.test_tags:UpdateTagsTestCase.test_update_tags_datasets
        
        self.db[constants.COL_PROVIDERS].insert(self.doc_provider)
        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 1)

        dataset = {
            "enable": True,
            "provider_name": self.doc_provider["name"],
            "dataset_code": "d1",
            "name": "dataset 1",
            "slug": "%s-d1" % self.doc_provider["slug"],
            "concepts": {
                "FREQ": "Frequency",
                "OBS_STATUS": "Observation Status",
            },
            "codelists": {
                "FREQ": {
                    "D": "Daily"
                },
                "OBS_STATUS": {
                    "E": "Estimate"
                }
            },
            "dimension_keys": ["FREQ"],
            "attribute_keys": ["OBS_STATUS"],
        } 

        self.db[constants.COL_DATASETS].insert(dataset)
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 1)

        result = tags_utils.update_tags_datasets(self.db, 
                                                 self.doc_provider["name"], 
                                                 dataset["dataset_code"])
        self.assertEqual(len(result["writeErrors"]), 0)
        self.assertEqual(result["nMatched"], 1)
        self.assertEqual(result["nModified"], 1)
        
        result = tags_utils.update_tags_datasets(self.db, 
                                                 self.doc_provider["name"], 
                                                 dataset["dataset_code"],
                                                 update_only=True)
        self.assertEqual(result["nMatched"], 0)
        self.assertEqual(result["nModified"], 0)

        query = {"provider_name": dataset["provider_name"],
                 "dataset_code": dataset["dataset_code"]}
        dataset_doc = self.db[constants.COL_DATASETS].find_one(query)
        self.assertIsNotNone(dataset_doc)
        self.assertEqual(dataset_doc["tags"], ['d1', 'daily', 'dataset', 'estimate', 'frequency', 'mars', 'observation', 'p1', 'provider', 'status', 'test'])
    
    def test_update_tags_series(self):

        # nosetests -s -v widukind_common.tests.test_tags:UpdateTagsTestCase.test_update_tags_series
        
        self.db[constants.COL_PROVIDERS].insert(self.doc_provider)
        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 1)

        dataset = {
            "enable": True,
            "provider_name": self.doc_provider["name"],
            "dataset_code": "d1",
            "name": "dataset 1",
            "slug": "%s-d1" % self.doc_provider["slug"],
            "concepts": {
                "FREQ": "Frequency",
                "OBS_STATUS": "Observation Status",
                "COUNTRY": "Country"
            },
            "codelists": {
                "FREQ": {
                    "D": "Daily",
                    "M": "Monthly"
                },
                "OBS_STATUS": {
                    "E": "Estimate",
                },
                "COUNTRY": {
                    "FRA": "France"
                }
            },
            "dimension_keys": ["FREQ", "COUNTRY"],
            "attribute_keys": ["OBS_STATUS"],
        }

        self.db[constants.COL_DATASETS].insert(dataset)
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 1)
        
        series = {
            "provider_name": self.doc_provider["name"],
            "dataset_code": dataset["dataset_code"],
            "key": "x1",
            "name": "series 1",
            "slug": "%s-d1-x1" % self.doc_provider["slug"],
            "frequency": "M",
            "dimensions": {
                "COUNTRY": "FRA"                
            },
            "attributes": {
                "OBS_STATUS": "E"
            },
        }

        self.db[constants.COL_SERIES].insert(series)
        self.assertEqual(self.db[constants.COL_SERIES].count(), 1)

        result = tags_utils.update_tags_series(self.db, 
                                               self.doc_provider["name"], 
                                               dataset["dataset_code"])

        self.assertEqual(result["nMatched"], 1)
        self.assertEqual(result["nModified"], 1)

        result = tags_utils.update_tags_series(self.db, 
                                               self.doc_provider["name"], 
                                               dataset["dataset_code"],
                                               update_only=True)
        self.assertEqual(result["nMatched"], 0)
        self.assertEqual(result["nModified"], 0)

        query = {"provider_name": series["provider_name"],
                 "dataset_code": series["dataset_code"],
                 "key": series["key"]}
        series_doc = self.db[constants.COL_SERIES].find_one(query)
        self.assertIsNotNone(series_doc)
        self.assertEqual(series_doc["tags"], ['country', 'd1', 'estimate', 'france', 'mars', 'monthly', 'observation', 'p1', 'provider', 'series', 'status', 'test', 'x1'])
        
@unittest.skipIf(True, "TODO")    
class SearchTagsTestCase(BaseDBTestCase):
    
    # nosetests -s -v widukind_common.tests.test_tags:SearchTagsTestCase
    
    def setUp(self):
        super().setUp()

        self.doc_provider = {
            "enable": True,
            "name": "p1",
            "long_name": "Provider Test",
            "region": "Mars",
            "slug": "p1"
        }
        
        self.doc_dataset = {
            "enable": True,
            "provider_name": self.doc_provider["name"],
            "dataset_code": "d1",
            "name": "dataset 1",
            "slug": "%s-d1" % self.doc_provider["slug"],
            "concepts": {
                "FREQ": "Frequency",
                "OBS_STATUS": "Observation Status",
                "COUNTRY": "Country"
            },
            "codelists": {
                "FREQ": {
                    "D": "Daily",
                    "M": "Monthly"
                },
                "OBS_STATUS": {
                    "E": "Estimate",
                },
                "COUNTRY": {
                    "FRA": "France"
                }
            },
            "dimension_keys": ["FREQ", "COUNTRY"],
            "attribute_keys": ["OBS_STATUS"],
        } 

        self.doc_series = {
            "provider_name": self.doc_provider["name"],
            "dataset_code": self.doc_dataset["dataset_code"],
            "key": "x1",
            "name": "series 1",
            "slug": "%s-d1-x1" % self.doc_provider["slug"],
            "frequency": "M",
            "dimensions": {
                "COUNTRY": "FRA"                
            },
            "attributes": {
                "OBS_STATUS": "E"
            },
        }

        

    def test_search_datasets_tags(self):

        # nosetests -s -v widukind_common.tests.test_tags:SearchTagsTestCase.test_search_datasets_tags
        
        self.db[constants.COL_PROVIDERS].insert(self.doc_provider)
        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 1)

        self.db[constants.COL_DATASETS].insert(self.doc_dataset)
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 1)

        tags_utils.update_tags_datasets(self.db, self.doc_provider["name"], self.doc_dataset["dataset_code"])
        
        query = {"provider_name": self.doc_dataset["provider_name"],
                 "dataset_code": self.doc_dataset["dataset_code"]}
        dataset_doc = self.db[constants.COL_DATASETS].find_one(query)
        self.assertEqual(dataset_doc["tags"], ['country', 'd1', 'daily', 'dataset', 'estimate', 'france', 'frequency', 'mars', 'monthly', 'observation', 'p1', 'provider', 'status', 'test'])        


        cursor, query = tags_utils.search_datasets_tags(self.db, 
                                                        search_tags="France MARS Daily")
        self.assertEqual(cursor.count(), 1)
        
        cursor, query = tags_utils.search_datasets_tags(self.db, 
                                                        search_tags="France MARS Daily",
                                                        provider_name="UNKNOW")
        
        self.assertEqual(cursor.count(), 0)
        
        """
        provider_name=None, dataset_code=None, 
        frequency=None,
        projection=None, 
        search_tags=None, search_type=None,
        start_date=None, end_date=None,
        sort=None, sort_desc=False,                        
        skip=None, limit=None        
        """
        
    
    def test_search_series_tags(self):

        # nosetests -s -v widukind_common.tests.test_tags:SearchTagsTestCase.test_search_series_tags
        
        self.db[constants.COL_PROVIDERS].insert(self.doc_provider)
        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 1)

        self.db[constants.COL_DATASETS].insert(self.doc_dataset)
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 1)
        tags_utils.update_tags_datasets(self.db, self.doc_provider["name"], self.doc_dataset["dataset_code"])
        
        self.db[constants.COL_SERIES].insert(self.doc_series)
        self.assertEqual(self.db[constants.COL_SERIES].count(), 1)
        tags_utils.update_tags_series(self.db, self.doc_provider["name"], self.doc_dataset["dataset_code"])


