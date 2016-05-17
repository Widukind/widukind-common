# -*- coding: utf-8 -*-

import os
import unittest

import mongomock

from widukind_common.utils import get_mongo_client, create_or_update_indexes
from widukind_common import tests_tools as utils

from widukind_common import constants

class BaseTestCase(unittest.TestCase):
    
    def setUp(self):
        super().setUp()

class BaseDBTestCase(BaseTestCase):
    """Tests with MongoDB
    """

    def setUp(self):
        super().setUp()
        
        if 'USE_MONGO_SERVER' in os.environ:
            self.client = get_mongo_client()
            self.addCleanup(self.client.close)
        else:
            self.client = mongomock.MongoClient(connect=False)
        
        self.db = self.client["widukind_test"]
        
        utils.clean_mongodb(self.db)

        self._collections_is_empty()
                
        create_or_update_indexes(self.db, force_mode=True, background=False)
        
    def _collections_is_empty(self):
        for col in constants.COL_ALL:
            self.assertEqual(self.db[col].count(), 0)
