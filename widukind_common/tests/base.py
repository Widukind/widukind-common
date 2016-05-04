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
                
        create_or_update_indexes(self.db, force_mode=True, background=False)

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        try:
            self.db.client.close()
        except:
            pass

    def _collections_is_empty(self):
        for col in constants.COL_ALL:
            self.assertEqual(self.db[col].count(), 0)
