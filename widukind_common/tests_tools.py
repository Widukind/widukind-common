# -*- coding: utf-8 -*-

from widukind_common.utils import get_mongo_db
from widukind_common import constants

def drop_gridfs(db):
    collections = db.collection_names()
    if 'fs.files' in collections:
        db.drop_collection('fs.files')
        db.drop_collection('fs.chunks')

def clean_mongodb(collection_list=None, db=None):
    """Drop all collections used by dlstats
    """
    db = db or get_mongo_db()
    collection_list = collection_list or constants.COL_ALL
    for col in collection_list:
        try:
            db.drop_collection(col)
        except:
            pass
    drop_gridfs(db)
    
    for col in collection_list:
        try:
            db.create_collection(col)
        except:
            pass
