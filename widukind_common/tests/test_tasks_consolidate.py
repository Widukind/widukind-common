# -*- coding: utf-8 -*-

from widukind_common.tasks import consolidate

from widukind_common import constants
from widukind_common.tests.base import BaseDBTestCase

class ConsolidateTasksTestCase(BaseDBTestCase):
    
    # nosetests -s -v widukind_common.tests.test_tasks_consolidate:ConsolidateTasksTestCase
    
    def setUp(self):
        BaseDBTestCase.setUp(self)

        self.dataset = {
            "enable": True,
            "provider_name": "p1",
            "dataset_code": "d1",
            "name": "dataset 1",
            "slug": "p1-d1",
            "concepts": {
                "FREQ": "Frequency",
                "OBS_STATUS": "Observation Status",
                "CURRENCY": "Currency",
                "COUNTRY": "Country"
            },
            "codelists": {
                "FREQ": {
                    "D": "Daily",
                    "M": "Monthly"
                },
                "OBS_STATUS": {
                    "E": "Estimate",
                    "T": "Terminate",
                },
                "CURRENCY": {
                    "E": "Euros",
                    "D": "Dollars",
                },
                "COUNTRY": {
                    "FRA": "France",
                    "AUS": "Autralie",
                }
            },
            "dimension_keys": ["FREQ", "COUNTRY"],
            "attribute_keys": ["CURRENCY", "OBS_STATUS"],
        }
        
        self.series = {
            "provider_name": "p1",
            "dataset_code": "d1",
            "key": "x1",
            "name": "series 1",
            "slug": "p1-d1-x1",
            "frequency": "M",
            "dimensions": {
                "COUNTRY": "FRA"                
            },
            "attributes": {
                "CURRENCY": "D"
            },
            'values': [
                {
                    'attributes': {
                        'OBS_STATUS': 'E'
                    },
                },
                {
                    'attributes': None
                }
            ],
        }

        self.datas_after = {
            "concepts": {
                "OBS_STATUS": "Observation Status",
                "CURRENCY": "Currency",
                "COUNTRY": "Country"
            },
            "codelists": {
                "OBS_STATUS": {
                    "E": "Estimate",
                },
                "CURRENCY": {
                    "D": "Dollars",
                },
                "COUNTRY": {
                    "FRA": "France",
                }
            },
            "dimension_keys": ["COUNTRY"],
            "attribute_keys": ["CURRENCY", "OBS_STATUS"],
        }
        
    def test_consolidate_dataset(self):
        
        self.db[constants.COL_DATASETS].insert(self.dataset)
        self.db[constants.COL_SERIES].insert(self.series)
        
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 1)
        self.assertEqual(self.db[constants.COL_SERIES].count(), 1)
    
        modified = consolidate.consolidate_dataset(provider_name=self.dataset["provider_name"], 
                                                   dataset_code=self.dataset["dataset_code"], 
                                                   db=self.db, 
                                                   execute=True)
        
        self.assertEqual(modified, 1)
        
        
        dataset = self.db[constants.COL_DATASETS].find_one({"slug": self.dataset["slug"]})
        self.assertIsNotNone(dataset)

        self.assertEqual(dataset["concepts"], self.datas_after["concepts"])
        self.assertEqual(dataset["codelists"], self.datas_after["codelists"])
        self.assertEqual(dataset["dimension_keys"], self.datas_after["dimension_keys"])
        self.assertEqual(dataset["attribute_keys"], self.datas_after["attribute_keys"])
        

        modified = consolidate.consolidate_dataset(provider_name=self.dataset["provider_name"], 
                                                   dataset_code=self.dataset["dataset_code"], 
                                                   db=self.db, 
                                                   execute=True)
        
        self.assertIsNone(modified)
        
        
    def test_consolidate_all_dataset(self):
    
        self.db[constants.COL_DATASETS].insert(self.dataset)
        self.db[constants.COL_SERIES].insert(self.series)
        
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 1)
        self.assertEqual(self.db[constants.COL_SERIES].count(), 1)
        
        result = consolidate.consolidate_all_dataset(provider_name=self.dataset["provider_name"], 
                                                     db=self.db)
        
        self.assertEqual(result, 
                         {"matched_count": 1, "modified_count": 1})
        
        dataset = self.db[constants.COL_DATASETS].find_one({"slug": self.dataset["slug"]})
        self.assertIsNotNone(dataset)

        self.assertEqual(dataset["concepts"], self.datas_after["concepts"])
        self.assertEqual(dataset["codelists"], self.datas_after["codelists"])
        self.assertEqual(dataset["dimension_keys"], self.datas_after["dimension_keys"])
        self.assertEqual(dataset["attribute_keys"], self.datas_after["attribute_keys"])
        

        result = consolidate.consolidate_all_dataset(provider_name=self.dataset["provider_name"], 
                                                     db=self.db)
        
        self.assertEqual(result, 
                         {"matched_count": 0, "modified_count": 0})
    