# -*- coding: utf-8 -*-

from datetime import datetime
import uuid
import time
import logging
from pprint import pprint

from flask import current_app, abort, request
from pymongo import ReadPreference

from widukind_common import constants

__all__ = [
    'col_providers',       
    'col_datasets',
    'col_categories',
    'col_series',
    'col_counters',
    
    'complex_queries_series',
    
    'get_provider',
    'get_dataset',
]

def col_providers(db=None):
    db = db or current_app.widukind_db
    return db[constants.COL_PROVIDERS].with_options(read_preference=ReadPreference.SECONDARY)

def col_datasets(db=None):
    db = db or current_app.widukind_db
    return db[constants.COL_DATASETS].with_options(read_preference=ReadPreference.SECONDARY)

def col_categories(db=None):
    db = db or current_app.widukind_db
    return db[constants.COL_CATEGORIES].with_options(read_preference=ReadPreference.SECONDARY)

def col_series(db=None):
    db = db or current_app.widukind_db
    return db[constants.COL_SERIES].with_options(read_preference=ReadPreference.SECONDARY)

def col_counters(db=None):
    db = db or current_app.widukind_db
    return db[constants.COL_COUNTERS].with_options(read_preference=ReadPreference.SECONDARY)

def complex_queries_series(query={}):

    tags = request.args.get('tags', None)
    
    search_fields = []
    query_and = []
    
    for r in request.args.lists():
        if r[0] in ['limit', 'tags', 'provider', 'dataset']:
            continue
        elif r[0] == 'frequency':
            query['frequency'] = r[1][0]
        else:
            search_fields.append((r[0], r[1][0]))

    if tags and len(tags.split()) > 0:
        tags = tags.split()
        conditions = [{"tags": {"$regex": ".*%s.*" % value.lower()}} for value in tags]
        #query = {"$and": conditions}
        #tags_regexp = [re.compile('.*%s.*' % e, re.IGNORECASE) for e in tags]
        #query["tags"] = {"$all": tags_regexp}
        query_and.append({"$and": conditions})
        
    if search_fields:
        
        query_or_by_field = {}
        query_nor_by_field = {}

        for field, value in search_fields:
            values = value.split()
            value = [v.lower().strip() for v in values]
            
            dim_field = field.lower()
            
            for v in value:
                if v.startswith("!"):
                    if not dim_field in query_nor_by_field:
                        query_nor_by_field[dim_field] = []
                    query_nor_by_field[dim_field].append(v[1:])
                else:
                    if not dim_field in query_or_by_field:
                        query_or_by_field[dim_field] = []
                    query_or_by_field[dim_field].append(v)
        
        for key, values in query_or_by_field.items():
            q_or = {"$or": [
                {"dimensions.%s" % key: {"$in": values}},
                {"attributes.%s" % key: {"$in": values}},
            ]}
            query_and.append(q_or)

        for key, values in query_nor_by_field.items():
            q_or = {"$or": [
                {"dimensions.%s" % key: {"$in": values}},
                {"attributes.%s" % key: {"$in": values}},
            ]}
            query_and.append(q_or)

    if query_and:
        query["$and"] = query_and
            
    print("-----complex query-----")
    pprint(query)    
    print("-----------------------")
        
    return query
    
def get_provider(slug, projection=None):
    projection = projection or {"_id": False}
    provider_doc = col_providers().find_one({'slug': slug, "enable": True}, 
                                            projection=projection)
    if not provider_doc:
        abort(404)
        
    return provider_doc

def get_dataset(slug, projection=None):
    ds_projection = projection or {"_id": False, "slug": True, "name": True,
                     "provider_name": True, "dataset_code": True}    
    dataset_doc = col_datasets().find_one({"enable": True, "slug": slug},
                                          ds_projection)
    
    if not dataset_doc:
        abort(404)
        
    return dataset_doc
    
