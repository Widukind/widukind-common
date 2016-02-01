# -*- coding: utf-8 -*-

import unittest
from pprint import pprint
import pymongo 

from widukind_common import tags as tags_utils
from widukind_common import constants

from widukind_common.tests.base import BaseTestCase, BaseDBTestCase

class TagsUtilsTestCase(BaseTestCase):

    # nosetests -s -v widukind_common.tests.test_tags:TagsUtilsTestCase
    
    def test_tags_filter(self):

        tags = ["the", "a", "france", "-", "quaterly"]
        result = sorted([a for a in filter(tags_utils.tags_filter, tags)])
        self.assertEqual(result, ["france", "quaterly"])

        result = sorted([a for a in filter(tags_utils.tags_filter, [None])])
        self.assertEqual(result, [])
        
    def test_tags_map(self):
        
        query = "The a France Quaterly"
        result = sorted(tags_utils.tags_map(query))
        self.assertEqual(result, ["a", "france", "quaterly", "the"])
        
        result = tags_utils.tags_map(None)
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
        
    def test_get_categories_tags_for_dataset(self):

        # nosetests -s -v widukind_common.tests.test_tags:GenerateTagsTestCase.test_get_categories_tags_for_dataset

        self._collections_is_empty()

        category = {
            "enable": True,
            "provider_name": self.doc_provider["name"],
            "category_code": "c1",
            "name": "Category 1",
            "parent": None,
            "all_parents": None,
            "slug": "%s-c1" % self.doc_provider["slug"],
            "tags": ['c1', 'category', 'mars', 'p1', 'provider', 'test'],
            "datasets": [{
                "dataset_code": "ds1"
            }],
        }
        docs = [category]
        try:
            self.db[constants.COL_CATEGORIES].insert_many(docs)
            self.assertEqual(self.db[constants.COL_CATEGORIES].count(), len(docs))
        except pymongo.errors.BulkWriteError as err:
            pprint(err.details)
            self.fail(str(err))
            
        tags = tags_utils.get_categories_tags_for_dataset(self.db, 
                                                          self.doc_provider["name"], 
                                                          "ds1")
        
        self.assertEqual(tags, ['c1', 'category', 'mars', 'p1', 'provider', 'test'])
            
    
    def test_generate_tags_categories(self):

        # nosetests -s -v widukind_common.tests.test_tags:GenerateTagsTestCase.test_generate_tags_categories

        self._collections_is_empty()
        
        category = {
            "enable": True,
            "provider_name": self.doc_provider["name"],
            "category_code": "c1",
            "name": "Category 1",
            "parent": None,
            "all_parents": None,
            "slug": "%s-c1" % self.doc_provider["slug"],
        }

        tags = tags_utils.generate_tags_categories(self.db, category, self.doc_provider)
        self.assertEqual(tags, ['c1', 'category', 'mars', 'p1', 'provider', 'test'])
        
        parent_category = {
            "enable": True,
            "provider_name": self.doc_provider["name"],
            "category_code": "c1parent",
            "name": "ParentCategory",
            "parent": None,
            "all_parents": None,
            "slug": "%s-c1parent" % self.doc_provider["slug"],
        }
        
        '''disable category'''
        sub_parent_category = {
            "enable": False,
            "provider_name": self.doc_provider["name"],
            "category_code": "c1subparent",
            "name": "SubParentCategory",
            "parent": parent_category["category_code"],
            "all_parents": [parent_category["category_code"]],
            "slug": "%s-c1subparent" % self.doc_provider["slug"],
        }
        
        category["all_parents"] = [parent_category["category_code"], sub_parent_category["category_code"]]
        
        docs = [category, parent_category, sub_parent_category]

        try:
            self.db[constants.COL_CATEGORIES].insert_many(docs)
            self.assertEqual(self.db[constants.COL_CATEGORIES].count(), len(docs))
        except pymongo.errors.BulkWriteError as err:
            pprint(err.details)
            self.fail(str(err))
        
        tags = tags_utils.generate_tags_categories(self.db, category, self.doc_provider)
        self.assertEqual(tags, ['c1', 'c1parent', 'category', 'mars', 'p1', 'parentcategory', 'provider', 'test'])

    def test_generate_tags_datasets(self):

        # nosetests -s -v widukind_common.tests.test_tags:GenerateTagsTestCase.test_generate_tags_datasets
        
        self._collections_is_empty()
        
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
        
        category = {
            "enable": True,
            "provider_name": self.doc_provider["name"],
            "category_code": "c1",
            "name": "Category 1",
            "parent": None,
            "all_parents": None,
            "slug": "%s-c1" % self.doc_provider["slug"],
            "tags": ['c1', 'category', 'mars', 'p1', 'provider', 'test'],
            "datasets": [{
                "dataset_code": dataset["dataset_code"]
            }],
        }
        docs = [category]
        try:
            self.db[constants.COL_CATEGORIES].insert_many(docs)
            self.assertEqual(self.db[constants.COL_CATEGORIES].count(), len(docs))
        except pymongo.errors.BulkWriteError as err:
            pprint(err.details)
            self.fail(str(err))

        tags = tags_utils.generate_tags_dataset(self.db, dataset, self.doc_provider, categories_tags=category["tags"])
        self.assertEqual(tags, ['c1', 'category', 'd1', 'daily', 'dataset', 'estimate', 'frequency', 'mars', 'observation', 'p1', 'provider', 'status', 'test'])

    def test_generate_tags_series(self):

        # nosetests -s -v widukind_common.tests.test_tags:GenerateTagsTestCase.test_generate_tags_series
        
        self._collections_is_empty()
        
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


        category = {
            "enable": True,
            "provider_name": self.doc_provider["name"],
            "category_code": "c1",
            "name": "Category 1",
            "parent": None,
            "all_parents": None,
            "slug": "%s-c1" % self.doc_provider["slug"],
            "tags": ['c1', 'category', 'mars', 'p1', 'provider', 'test'],
            "datasets": [{
                "dataset_code": dataset["dataset_code"]
            }],
        }
        docs = [category]
        try:
            self.db[constants.COL_CATEGORIES].insert_many(docs)
            self.assertEqual(self.db[constants.COL_CATEGORIES].count(), len(docs))
        except pymongo.errors.BulkWriteError as err:
            pprint(err.details)
            self.fail(str(err))

        tags = tags_utils.generate_tags_series(self.db, series, 
                                               self.doc_provider, 
                                               dataset,
                                               categories_tags=category["tags"])
        self.assertEqual(tags, ['c1', 'category', 'country', 'd1', 'estimate', 'france', 'mars', 'monthly', 'observation', 'p1', 'provider', 'series', 'status', 'test', 'x1'])


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
    
    def test_update_tags_categories(self):

        # nosetests -s -v widukind_common.tests.test_tags:UpdateTagsTestCase.test_update_tags_categories
        
        self._collections_is_empty()
        
        self.db[constants.COL_PROVIDERS].insert(self.doc_provider)
        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 1)
        
        category = {
            "enable": True,
            "provider_name": self.doc_provider["name"],
            "category_code": "c1",
            "name": "Category 1",
            "parent": None,
            "all_parents": None,
            "slug": "%s-c1" % self.doc_provider["slug"],
        }
        self.db[constants.COL_CATEGORIES].insert(category)
        self.assertEqual(self.db[constants.COL_CATEGORIES].count(), 1)
        
        result = tags_utils.update_tags_categories(self.db, self.doc_provider["name"])
        
        self.assertEqual(len(result["writeErrors"]), 0)
        self.assertEqual(result["nMatched"], 1)
        self.assertEqual(result["nModified"], 1)

        result = tags_utils.update_tags_categories(self.db, self.doc_provider["name"],
                                                   update_only=True)
        self.assertEqual(result["nMatched"], 0)
        self.assertEqual(result["nModified"], 0)

        query = {"provider_name": category["provider_name"]}
        category_doc = self.db[constants.COL_CATEGORIES].find_one(query)
        self.assertIsNotNone(category_doc)
        self.assertEqual(category_doc["tags"], ['c1', 'category', 'mars', 'p1', 'provider', 'test'])
        
    def test_update_tags_datasets(self):

        # nosetests -s -v widukind_common.tests.test_tags:UpdateTagsTestCase.test_update_tags_datasets
        
        self._collections_is_empty()
        
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
        
        self._collections_is_empty()
        
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

        self.assertEqual(len(result["writeErrors"]), 0)
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
    

        """    
        result = self.db[constants.COL_SERIES].update_one({"provider_name": self.doc_provider["name"], 
                                                  "dataset_code": dataset["dataset_code"]}, 
                                                 {"$set": {"tags": []}})
        
        pprint(result)
        result = tags_utils.update_tags_series(self.db, 
                                               self.doc_provider["name"], 
                                               dataset["dataset_code"])

        self.assertEqual(len(result["writeErrors"]), 0)
        self.assertEqual(result["nMatched"], 1)
        self.assertEqual(result["nModified"], 1)
        """
    
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
        
        self._collections_is_empty()
        
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
        
        self._collections_is_empty()

        self.db[constants.COL_PROVIDERS].insert(self.doc_provider)
        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 1)

        self.db[constants.COL_DATASETS].insert(self.doc_dataset)
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 1)
        tags_utils.update_tags_datasets(self.db, self.doc_provider["name"], self.doc_dataset["dataset_code"])
        
        self.db[constants.COL_SERIES].insert(self.doc_series)
        self.assertEqual(self.db[constants.COL_SERIES].count(), 1)
        tags_utils.update_tags_series(self.db, self.doc_provider["name"], self.doc_dataset["dataset_code"])


