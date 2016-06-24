# -*- coding: utf-8 -*-

import os
import unittest
from io import StringIO

import mongomock

from widukind_common.utils import get_mongo_client, create_or_update_indexes
from widukind_common import tests_tools as utils

from widukind_common import constants

class BaseTestCase(unittest.TestCase):
    
    def setUp(self):
        super().setUp()
        self.log = StringIO()        

    def assertInLog(self, msg):
        self.assertTrue(msg in self.log.getvalue())
        
    def assertNotInLog(self, msg):
        self.assertFalse(msg in self.log.getvalue())

class BaseDBTestCase(BaseTestCase):
    """Tests with MongoDB
    """
    CLEAN_DB = True
    INDEXES = True

    def setUp(self):
        super().setUp()
        
        self.mongo_client = None
        self.init_mongo_client()
        
        self.db = self.get_mongo_db()
        
        if self.CLEAN_DB:
            self.clean_db()

        self._collections_is_empty()
        
        if self.INDEXES:
            self.set_indexes()
    
    def init_mongo_client(self):
        if 'USE_MONGO_SERVER' in os.environ:
            self.mongo_client = get_mongo_client()
            self.addCleanup(self.mongo_client.close)
        else:
            self.mongo_client = mongomock.MongoClient(connect=False)
    
    def get_mongo_db(self):
        return self.mongo_client["widukind_test"]
                
    def set_indexes(self):
        create_or_update_indexes(self.db, force_mode=True, background=False)
        
    def clean_db(self):
        utils.clean_mongodb(self.db)
        
    def _collections_is_empty(self):
        for col in constants.COL_ALL:
            self.assertEqual(self.db[col].count(), 0)

    def get_plan_stage(self, root, stage):
        """
        Usage:

            explain = db.test.find({"x": 6, "a": 1}).explain()
            stage = self.get_plan_stage(explain['queryPlanner']['winningPlan'], 'IXSCAN')
            self.assertEqual("x_1", stage.get('indexName'))
            self.assertTrue(stage.get('isPartial'))
        
        """
        if root.get('stage') == stage:
            return root
        elif "inputStage" in root:
            return self.get_plan_stage(root['inputStage'], stage)
        elif "inputStages" in root:
            for i in root['inputStages']:
                stage = self.get_plan_stage(i, stage)
                if stage:
                    return stage
        elif "shards" in root:
            for i in root['shards']:
                stage = self.get_plan_stage(i['winningPlan'], stage)
                if stage:
                    return stage
        return {}
