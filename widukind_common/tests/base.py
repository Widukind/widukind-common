# -*- coding: utf-8 -*-

import unittest

from widukind_common.utils import get_mongo_db, create_or_update_indexes
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
        
        db = get_mongo_db()
        self.db = db.client["widukind_test"] 
        
        self.assertEqual(self.db.name, "widukind_test")

        utils.clean_mongodb(self.db)

        self._collections_is_empty()
                
        create_or_update_indexes(self.db, force_mode=True)

    def _collections_is_empty(self):
        self.assertEqual(self.db[constants.COL_PROVIDERS].count(), 0)
        self.assertEqual(self.db[constants.COL_CATEGORIES].count(), 0)
        self.assertEqual(self.db[constants.COL_DATASETS].count(), 0)
        self.assertEqual(self.db[constants.COL_SERIES].count(), 0)
        self.assertEqual(self.db[constants.COL_TAGS_DATASETS].count(), 0)
        self.assertEqual(self.db[constants.COL_TAGS_SERIES].count(), 0)
        
