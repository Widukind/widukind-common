# -*- coding: utf-8 -*-

import re
import string

import pandas

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError

from widukind_common import constants

#'!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
TAGS_REPLACE_CHARS = []
TAGS_REPLACE_CHARS.extend([s for s in string.punctuation if not s in ["-", "_"]]) 

TAGS_MIN_CHAR = 2

def tags_filter(value):

    if value in constants.TAGS_EXCLUDE_WORDS:
        return False
    
    if not value or len(value.strip()) < TAGS_MIN_CHAR:
        return False
    
    if value in ["-", "_", " "]:
        return False
    
    return True            

def tags_map(value):

    value = value.strip().lower()

    new_value = []
    for v in value:
        if not v in TAGS_REPLACE_CHARS:
            new_value.append(v)
        else:
            new_value.append(" ")
    
    return "".join(new_value).split()
        
def str_to_tags(value_str):
    """Split and filter word - return array of word (to lower) 

    >>> utils.str_to_tags("Bank's of France")
    ['bank', 'france']
    
    >>> utils.str_to_tags("Bank's of & France")
    ['bank', 'france']
    
    >>> utils.str_to_tags("France")
    ['france']
    
    >>> utils.str_to_tags("Bank's")
    ['bank']                
    """    
    tags = tags_map(value_str)
    return [a for a in filter(tags_filter, tags)]
    
def generate_tags(db, doc, doc_type=None, 
                  doc_provider=None, doc_dataset=None):
    """Split and filter datas for return array of tags
    
    Used in update_tags()

    :param pymongo.database.Database db: MongoDB Database instance
    :param doc dict: Document MongoDB        
    :param doc_type str: 
    :param bool is_indexes: Bypass create_or_update_indexes() if False 

    :raises ValueError: if provider_name is None
    """
        
    select_for_tags = []
    tags = []
    
    def search_dataset_dimension_list(key, value, dataset_doc):
        if key in dataset_doc['dimension_list']: 
            dimensions = dataset_doc['dimension_list'][key]
            for d in dimensions:
                if value == d[0]:
                    return d[1] 

    def search_dataset_attribute_list(key, value, dataset_doc):
        if key in dataset_doc['attribute_list']:
            attributes = dataset_doc['attribute_list'][key]
            for a in attributes:
                if value == a[0]:
                    return a[1] 
    
    if doc_type == constants.COL_DATASETS:

        select_for_tags.append(doc['provider'])
        select_for_tags.append(doc['dataset_code'])
        select_for_tags.append(doc['name'])
        
        if 'notes' in doc and len(doc['notes'].strip()) > 0: 
            select_for_tags.append(doc['notes'].strip())
        
        for key, values in doc['dimension_list'].items():            
            #select_for_tags.append(key)        #dimension name:            
            for item in values:               
                #TODO: dimension key ?
                #select_for_tags.append(item[0])
                select_for_tags.append(item[1])

        for key, values in doc['attribute_list'].items():            
            #select_for_tags.append(key)        #attribute name:            
            for item in values:            
                #TODO: attribute key ?
                #select_for_tags.append(item[0])
                select_for_tags.append(item[1])
        
    elif doc_type == constants.COL_SERIES:

        query = {
            "provider": doc['provider'], 
            "dataset_code": doc['dataset_code']
        }        
        dataset = doc_dataset or db[constants.COL_DATASETS].find_one(query)
        
        if not dataset:
            raise Exception("dataset not found for provider[%(provider)s] - dataset_code[%(dataset_code)s]" % query)

        select_for_tags.append(doc['provider'])
        select_for_tags.append(doc['dataset_code'])
        select_for_tags.append(doc['key'])
        select_for_tags.append(doc['name'])
        
        if 'notes' in doc and len(doc['notes'].strip()) > 0: 
            select_for_tags.append(doc['notes'].strip())

        for dimension_key, dimension_code in doc['dimensions'].items():
            #select_for_tags.append(dimension_key)
            if dimension_key and dimension_code:
                dimension_value = search_dataset_dimension_list(dimension_key, 
                                                               dimension_code, 
                                                               dataset)
                if dimension_value:            
                    select_for_tags.append(dimension_value)

        for attribute_key, attribute_code in doc['attributes'].items():            
            #select_for_tags.append(attribute_key)
            if attribute_key and attribute_code:
                attribute_value = search_dataset_attribute_list(attribute_key, 
                                                               attribute_code, 
                                                               dataset)
                if attribute_value:
                    select_for_tags.append(attribute_value)

    for value in select_for_tags:
        tags.extend(str_to_tags(value))
        
    return sorted(list(set(tags)))

def bulk_result_aggregate(bulk_result):
    """Aggregate array of bulk execute to unique dict
    
    >>> bulk_result[0]
    {'upserted': [], 'nUpserted': 10, 'nModified': 0, 'nMatched': 20, 'writeErrors': [], 'nRemoved': 0, 'writeConcernErrors': [], 'nInserted': 0}
    >>> bulk_result[1]
    {'upserted': [], 'nUpserted': 5, 'nModified': 0, 'nMatched': 4, 'writeErrors': [], 'nRemoved': 0, 'writeConcernErrors': [], 'nInserted': 0}
    >>> result = bulk_result_aggregate(bulk_result)
    >>> result
    {'writeErrors': [], 'nUpserted': 15, 'nMatched': 0, 'nModified': 0, 'upserted': [], 'nRemoved': 0, 'writeConcernErrors': [], 'nInserted': 0}    
    """
    
    bulk_dict = {
        "nUpserted": 0,
        "nModified": 0,
        "nMatched": 0,
        "nRemoved": 0,
        "nInserted": 0,
        #"upserted": [],
        "writeErrors": [],
        "writeConcernErrors": [],
    }
    
    for r in bulk_result:
        bulk_dict["nUpserted"] += r["nUpserted"]
        bulk_dict["nModified"] += r["nModified"]
        bulk_dict["nMatched"] += r["nMatched"]
        bulk_dict["nRemoved"] += r["nRemoved"]
        bulk_dict["nInserted"] += r["nInserted"]
        #bulk_dict["upserted"].extend(r["upserted"])
        bulk_dict["writeErrors"].extend(r["writeErrors"])
        bulk_dict["writeConcernErrors"].extend(r["writeConcernErrors"])
    
    return bulk_dict
    

def run_bulk(bulk=None):
    try:
        result = bulk.execute()
        #TODO: bulk.execute({'w': 3, 'wtimeout': 1})
        #pprint(result)
        return result
    except BulkWriteError as err:        
        #pprint(err.details)
        raise
    
def update_tags(db, 
                provider_name=None, dataset_code=None, serie_key=None, 
                col_name=None, max_bulk=20):
    
    #TODO: cumul des results bulk
    bulk = db[col_name].initialize_unordered_bulk_op()
    count = 0
    query = {"provider": provider_name}
    projection = None

    if dataset_code:
        query['dataset_code'] = dataset_code

    if col_name == constants.COL_DATASETS:
        projection = {"doc_href": False}
    
    if col_name == constants.COL_SERIES and serie_key:
        query['key'] = serie_key
        
    if col_name == constants.COL_SERIES:
        projection = {"release_dates": False, "values": False}

    for doc in db[col_name].find(query, projection=projection):
        #TODO: load dataset doc if search series ?
        tags = generate_tags(db, doc, doc_type=col_name)
        
        #projection=projection
        bulk.find({'_id': doc['_id']}).update_one({"$set": {'tags': tags}})
        
        count += 1
        
        if count >= max_bulk:
            run_bulk(bulk)
            bulk = db[col_name].initialize_unordered_bulk_op()
            count = 0

    #bulk delta
    if count > 0:
        run_bulk(bulk)

def search_tags(db, 
               provider_name=None, 
               dataset_code=None, 
               frequency=None,
               projection=None, 
               search_tags=None,
               search_type=constants.COL_DATASETS,
               start_date=None,
               end_date=None,
               sort=None,
               sort_desc=False,                        
               skip=None, limit=None):
    """Search in series by tags field
    
    >>> from dlstats import utils
    >>> db = utils.get_mongo_db()
    
    >>> docs = utils.search_series_tags(db, search_tags=["Belgium", "Euro"])    

    # Search in all provider and dataset
    >>> docs = utils.search_series_tags(db, frequency="A", search_tags=["Belgium", "Euro", "Agriculture"])

    # Filter provider and/or dataset
    >>> docs = utils.search_series_tags(db, provider_name="Eurostat", dataset_code="nama_10_a10", search_tags=["Belgium", "Euro", "Agriculture"])
    
    #print(docs.count())    
    #for doc in docs: print(doc['provider'], doc['dataset_code'], doc['key'], doc['name'])
    """
    
    '''Convert search tag to lower case and strip tag'''
    tags = str_to_tags(search_tags)        

    # Add OR, NOT
    tags_regexp = [re.compile('.*%s.*' % e, re.IGNORECASE) for e in tags]
    #  AND implementation
    query = {"tags": {"$all": tags_regexp}}

    if provider_name:
        if isinstance(provider_name, str):
            providers = [provider_name]
        else:
            providers = provider_name
        query['provider'] = {"$in": providers}
        
    if search_type == "series":

        COL_SEARCH = constants.COL_SERIES

        date_freq = constants.FREQ_ANNUALY

        if frequency:
            query['frequency'] = frequency
            date_freq = frequency
                        
        if dataset_code:
            query['dataset_code'] = dataset_code

        if start_date:
            ordinal_start_date = pandas.Period(start_date, freq=date_freq).ordinal
            query["start_date"] = {"$gte": ordinal_start_date}
        
        if end_date:
            query["end_date"] = {"$lte": pandas.Period(end_date, freq=date_freq).ordinal}

    else:
        COL_SEARCH = constants.COL_DATASETS
        
    cursor = db[COL_SEARCH].find(query, projection=projection)

    if skip:
        cursor = cursor.skip(skip)
    
    if limit:
        cursor = cursor.limit(limit)
    
    if sort:
        sort_direction = ASCENDING
        if sort_desc:
            sort_direction = DESCENDING
        cursor = cursor.sort(sort, sort_direction)
    
    return cursor, query
           
def search_series_tags(db, **kwargs):
    return search_tags(db, search_type=constants.COL_SERIES, **kwargs)

def search_datasets_tags(db, **kwargs):
    return search_tags(db, search_type=constants.COL_DATASETS, **kwargs)

def _aggregate_tags(db, source_col, target_col, max_bulk=20):

    bulk = db[target_col].initialize_unordered_bulk_op()
    count = 0
    
    pipeline = [
      {"$match": {"tags.0": {"$exists": True}}},
      {'$project': { '_id': 0, 'tags': 1, 'provider': 1}},
      {"$unwind": "$tags"},
      {"$group": {"_id": {"tag": "$tags", "provider": "$provider"}, "count": {"$sum": 1}}},
      {'$project': { 'tag': "$_id.tag", 'count': 1, 'provider': {"name": "$_id.provider", "count": "$count"}}},
      {"$group": {"_id": "$tag", "count": {"$sum": "$count"}, "providers":{ "$addToSet": "$provider" } }},
      #{"$sort": SON([("count", -1), ("_id", -1)])}      
    ]
    
    bulk_result = []
    
    result = db[source_col].aggregate(pipeline, allowDiskUse=True)
    
    for doc in result:
        update = {
            '$addToSet': {'providers': {"$each": doc['providers']}},
            "$set": {"count": doc['count']}
        }
        bulk.find({'name': doc['_id']}).upsert().update_one(update)
        count += 1
        
        if count >= max_bulk:
            bulk_result.append(run_bulk(bulk))
            bulk = db[target_col].initialize_unordered_bulk_op()
            count = 0

    #bulk delta
    if count > 0:
        bulk_result.append(run_bulk(bulk))
    
    return bulk_result_aggregate(bulk_result)
    
def aggregate_tags_datasets(db, max_bulk=20):
    """
    >>> pp(list(db.tags.datasets.find().sort([("count", -1)]))[0])
    {'_id': ObjectId('565ade73426049c4cea21c0e'),
     'count': 10,
     'name': 'france',
     'providers': [{'count': 8, 'name': 'BIS'},
                   {'count': 1, 'name': 'OECD'},
                   {'count': 1, 'name': 'Eurostat'}]}
                   
    db.tags.datasets.distinct("name")
    
    TOP 10:
        >>> pp(list(db.tags.datasets.find({}).sort([("count", -1)])[:10]))
        [{'_id': ObjectId('565ade73426049c4cea21c0e'),
          'count': 10,
          'name': 'france',
          'providers': [{'count': 8, 'name': 'BIS'},
                        {'count': 1, 'name': 'OECD'},
                        {'count': 1, 'name': 'Eurostat'}]},
         {'_id': ObjectId('565ade73426049c4cea21c7d'),
          'count': 10,
          'name': 'norway',
          'providers': [{'count': 8, 'name': 'BIS'},
                        {'count': 1, 'name': 'OECD'},
                        {'count': 1, 'name': 'Eurostat'}]},                       
                               
    """
    return _aggregate_tags(db, 
                           constants.COL_DATASETS, 
                           constants.COL_TAGS_DATASETS, 
                           max_bulk=max_bulk)

def aggregate_tags_series(db, max_bulk=20):
    return _aggregate_tags(db, 
                           constants.COL_SERIES, 
                           constants.COL_TAGS_SERIES, 
                           max_bulk=max_bulk)
