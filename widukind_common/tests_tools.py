# -*- coding: utf-8 -*-

from widukind_common.utils import get_mongo_db
from widukind_common import constants

def drop_gridfs(db):
    collections = db.collection_names()
    if 'fs.files' in collections:
        db.drop_collection('fs.files')
        db.drop_collection('fs.chunks')

def clean_mongodb(db=None):
    """Drop all collections used by dlstats
    """
    db = db or get_mongo_db()
    for col in constants.COL_ALL:
        try:
            db.drop_collection(col)
        except:
            pass
    drop_gridfs(db)
