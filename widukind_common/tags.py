# -*- coding: utf-8 -*-

import time
import logging
from pprint import pprint
import re
import string

import pandas

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne

from widukind_common import constants

TAGS_REPLACE_CHARS = []
TAGS_REPLACE_CHARS.extend([s for s in string.punctuation if not s in ["-", "_"]])
TABLE_MAP = str.maketrans("".join(TAGS_REPLACE_CHARS), "".join([" " for v in TAGS_REPLACE_CHARS]))
TAGS_MIN_CHAR = 2

logger = logging.getLogger(__name__)

def _translate(s):
    if not s or len(s.strip()) < TAGS_MIN_CHAR:
        return
    return s.translate(TABLE_MAP).strip().lower().split()

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
    tags = _translate(value_str)
    
    if tags:
        return [a for a in tags if not a in [None] + constants.TAGS_EXCLUDE_WORDS + ["-", "_", " "] and len(a.strip()) >= TAGS_MIN_CHAR]    
    return []

def get_categories_tags_for_dataset(db, provider_name, dataset_code):
    tags = []

    query = {"provider_name": provider_name, "enable": True,
              "datasets.dataset_code": dataset_code}
    projection = {"tags": True, "_id": False}        
    categories = db[constants.COL_CATEGORIES].find(query, projection)
    
    for doc in categories:
        if doc.get("tags"):
            tags.extend(doc["tags"])
    return tags
    
def generate_tags_categories(db, doc, doc_provider):

    select_for_tags = []
    tags = []
    
    select_for_tags.append(doc_provider['name'])
    select_for_tags.append(doc_provider['long_name'])
    select_for_tags.append(doc_provider['region'])
    select_for_tags.append(doc['category_code'])
    select_for_tags.append(doc['name'])

    if doc.get("all_parents"):
        _query = {"provider_name": doc["provider_name"], "enable": True,
                  "category_code": {"$in": doc["all_parents"]}}
        _projection = {"datasets": False}
        
        all_parents = db[constants.COL_CATEGORIES].find(_query,
                                                        _projection)
        
        for parent in all_parents:
            _tags = generate_tags_categories(db, parent, doc_provider)
            select_for_tags.extend(_tags)

    for value in select_for_tags:
        tags.extend(str_to_tags(value))
        
    return sorted(list(set(tags)))

def generate_tags_dataset(db, doc, doc_provider, categories_tags=[]):
    """Split and filter datas for return array of tags
    
    Used in update_tags()

    :param pymongo.database.Database db: MongoDB Database instance
    :param doc dict: Document MongoDB        
    :param doc_provider dict: Document MongoDB        
    """
        
    select_for_tags = []
    tags = []
    select_for_tags.append(doc_provider['name'])
    select_for_tags.append(doc_provider['long_name'])
    select_for_tags.append(doc_provider['region'])    
    select_for_tags.append(doc['dataset_code'])
    select_for_tags.append(doc['name'])
    
    if 'notes' in doc and doc['notes'] and len(doc['notes']) > 0: 
        select_for_tags.append(doc['notes'])
        
    if doc.get('concepts'):
        for value in doc['concepts'].values():            
            select_for_tags.append(value)
    
    if doc.get("codelists") and doc.get("dimension_keys"):
        for key in doc["dimension_keys"]:
            if not key in doc["codelists"]:
                continue
            for value in doc['codelists'][key].values():
                select_for_tags.append(value)
    
    if doc.get("codelists") and doc.get("attribute_keys"):
        for key in doc["attribute_keys"]:
            if not key in doc["codelists"]:
                continue
            for value in doc['codelists'][key].values():
                select_for_tags.append(value)

    for value in select_for_tags:
        tags.extend(str_to_tags(value))
        
    return sorted(list(set(tags)))

def generate_tags_series(db, doc, doc_provider, doc_dataset, categories_tags=[]):
    """Split and filter datas for return array of tags
    
    Used in update_tags()

    :param pymongo.database.Database db: MongoDB Database instance
    :param doc dict: Document MongoDB        
    :param doc_provider dict: Document MongoDB        
    """
        
    select_for_tags = []
    tags = []
    
    #if categories_tags:
    #    tags = categories_tags
    
    select_for_tags.append(doc_provider['name'])
    select_for_tags.append(doc_provider['long_name'])
    select_for_tags.append(doc_provider['region'])    
    select_for_tags.append(doc['dataset_code'])
    select_for_tags.append(doc['name'])
    select_for_tags.append(doc['key'])
    select_for_tags.append(doc['name'])

    def search_dataset_concepts(key):
        if doc_dataset.get('concepts'):
            return doc_dataset.get('concepts').get(key)

    def search_dataset_codelists(key, value):
        if key in doc_dataset.get('codelists'): 
            codes = doc_dataset['codelists'].get(key)
            if codes:
                return codes.get(value)
    
    if 'notes' in doc and doc['notes'] and len(doc['notes']) > 0: 
        select_for_tags.append(doc['notes'])

    for field in ["dimensions", "attributes"]:
        if not doc.get(field):
            continue
            
        for key, code in doc[field].items():

            concept = search_dataset_concepts(key)
            if concept:
                select_for_tags.append(concept)

            code_value = search_dataset_codelists(key, code)
            if code_value:            
                select_for_tags.append(code_value)

    if doc['frequency'] in constants.FREQUENCIES_DICT:
        select_for_tags.append(constants.FREQUENCIES_DICT[doc['frequency']])

    for value in select_for_tags:
        tags.extend(str_to_tags(value))
        
    return sorted(list(set(tags)))

def generate_tags_series_async(doc, doc_provider, doc_dataset):
    """Split and filter datas for return array of tags
    
    Used in update_tags()

    :param pymongo.database.Database db: MongoDB Database instance
    :param doc dict: Document MongoDB        
    :param doc_provider dict: Document MongoDB        
    """
        
    tags = []
    
    #if categories_tags:
    #    tags = categories_tags
    
    yield doc_provider['name']
    yield doc_provider['long_name']
    yield doc_provider['region']  
    yield doc['dataset_code']
    yield doc['name']
    yield doc['key']
    yield doc['name']

    def search_dataset_concepts(key):
        if doc_dataset.get('concepts'):
            return doc_dataset.get('concepts').get(key)

    def search_dataset_codelists(key, value):
        if key in doc_dataset.get('codelists'): 
            codes = doc_dataset['codelists'].get(key)
            if codes:
                return codes.get(value)
    
    if 'notes' in doc and doc['notes'] and len(doc['notes']) > 0: 
        yield doc['notes']

    for field in ["dimensions", "attributes"]:
        if not doc.get(field):
            continue
            
        for key, code in doc[field].items():

            concept = search_dataset_concepts(key)
            if concept:
                yield concept

            code_value = search_dataset_codelists(key, code)
            if code_value:            
                yield code_value

    if doc['frequency'] in constants.FREQUENCIES_DICT:
        yield constants.FREQUENCIES_DICT[doc['frequency']]


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
        return result
    except BulkWriteError as err:        
        #pprint(err.details)
        raise

#TODO: remove
def update_tags_categories(db, provider_name=None, 
                           max_bulk=100, update_only=False, dry_mode=False):
    
    return bulk_result_aggregate([])

    doc_provider = db[constants.COL_PROVIDERS].find_one({"enable": True,
                                                         "name": provider_name})

    if not doc_provider:
        logger.error("Provider [%s] not found or disable." % provider_name)
        return
    
    query = {'provider_name': provider_name, "enable": True}
    projection = {"datasets": False, "doc_href": False}
    
    if update_only:
        query["tags.0"] = {"$exists": False}

    bulk = db[constants.COL_CATEGORIES].initialize_unordered_bulk_op()
    count = 0
    
    for doc in db[constants.COL_CATEGORIES].find(query, projection):
        
        tags = generate_tags_categories(db, doc, doc_provider)
        
        if not dry_mode and tags:
            count += 1
            bulk.find({'_id': doc["_id"]}).update_one({"$set": {'tags': tags}})
            
            if logger.isEnabledFor(logging.DEBUG):
                msg = "update tags for category[%s] - provider[%s] - tags%s" 
                logger.debug(msg % (doc["category_code"], 
                                    provider_name,
                                    tags))
        
        elif dry_mode and tags:
            print("--------------------------------")
            print("category[%s] tags[%s]" % (doc["category_code"], ", ".join(tags)))
            print("--------------------------------")
   
    if not dry_mode and count > 0:
        result = run_bulk(bulk)
        return bulk_result_aggregate([result])
    
    return bulk_result_aggregate([])

def update_tags_datasets(db, provider_name=None, dataset_code=None, 
                         max_bulk=100, update_only=False, dry_mode=False):

    doc_provider = db[constants.COL_PROVIDERS].find_one({"enable": True,
                                                         "name": provider_name})

    if not doc_provider:
        logger.error("Provider [%s] not found or disable." % provider_name)
        return
    
    query = {'provider_name': provider_name, "enable": True}
    projection = {"doc_href": False, "dimension_list": False, "attribute_list": False}

    if update_only:
        query["tags.0"] = {"$exists": False}

    if dataset_code:
        query["dataset_code"] = dataset_code

    bulk = db[constants.COL_DATASETS].initialize_unordered_bulk_op()
    bulk_list = []
    bulk_results = []

    for doc in db[constants.COL_DATASETS].find(query, projection):
        
        tags = generate_tags_dataset(db, doc, doc_provider)#, categories_tags=categories_tags)
        
        if not dry_mode and tags:
            bulk_list.append((doc["_id"], tags))

            if logger.isEnabledFor(logging.DEBUG):
                msg = "update tags for dataset[%s] - provider[%s] - tags%s" 
                logger.debug(msg % (doc["dataset_code"], 
                                    provider_name,
                                    tags))
        
        elif dry_mode and tags:
            print("--------------------------------")
            print("dataset[%s] tags[%s]" % (doc["dataset_code"], ", ".join(tags)))
            print("--------------------------------")

        if len(bulk_list) > max_bulk:
            for b in bulk_list:
                bulk.find({'_id': b[0]}).update_one({"$set": {'tags': b[1]}})
            bulk_results.append(run_bulk(bulk))
            bulk = db[constants.COL_DATASETS].initialize_unordered_bulk_op()
            bulk_list = []
            
    if not dry_mode and len(bulk_list) > 0:
        for b in bulk_list:
            bulk.find({'_id': b[0]}).update_one({"$set": {'tags': b[1]}})
        bulk_results.append(run_bulk(bulk))
        
    return bulk_result_aggregate(bulk_results)

def _update_tags_series_unit(db, 
                            provider=None, dataset=None, categories_tags=[], 
                            update_only=False, dry_mode=False):

    provider_name = dataset["provider_name"]
    dataset_code = dataset["dataset_code"]

    series_query = {'provider_name': provider_name, 
                    "dataset_code": dataset_code}
    series_projection = {"values": False}

    if update_only:
        series_query["tags.0"] = {"$exists": False}
    
    for doc in db[constants.COL_SERIES].find(series_query, 
                                             series_projection,
                                             #no_cursor_timeout=True
                                             ):

        tags = generate_tags_series(db, doc, provider, dataset)#, categories_tags)
    
        if logger.isEnabledFor(logging.DEBUG):
            msg = "update tags for series[%s] - dataset[%s] - provider[%s] - tags%s" 
            logger.debug(msg % (doc["key"],
                                doc["dataset_code"], 
                                provider_name,
                                tags))

        yield doc, tags

def _update_tags_series(db, provider_name=None, dataset_code=None, 
                       update_only=False, dry_mode=False):

    doc_provider = db[constants.COL_PROVIDERS].find_one({"enable": True,
                                                         "name": provider_name})

    if not doc_provider:
        logger.error("Provider [%s] not found or disable." % provider_name)
        return
    
    dataset_query = {'provider_name': provider_name, "enable": True}
    dataset_projection = {"doc_href": False, 
                          "dimension_list": False, "attribute_list": False}

    if dataset_code:
        dataset_query["dataset_code"] = dataset_code

    for doc_dataset in db[constants.COL_DATASETS].find(dataset_query, 
                                                       dataset_projection,
                                                       no_cursor_timeout=True):

        for doc, tags in _update_tags_series_unit(db, doc_provider, doc_dataset, update_only=update_only, dry_mode=dry_mode):
            yield doc, tags

def _update_tags_series_sync(db, provider_name=None, dataset_code=None, 
                            max_bulk=100, update_only=False, dry_mode=False):

    bulk_list = []
    bulk_results = []
    
    for doc, tags in _update_tags_series(db, provider_name=provider_name, 
                                         dataset_code=dataset_code, 
                                         update_only=update_only, dry_mode=dry_mode):

        if not dry_mode and tags:
            #bulk_list.append((doc["_id"], tags))
            bulk_list.append(UpdateOne({'_id': doc["_id"]}, {"$set": {'tags': tags}}))
        
        elif dry_mode and tags:
            print("--------------------------------")
            print("dataset[%s] - series[%s] - tags[%s]" % (dataset_code,
                                                           doc["key"], ", ".join(tags)))
            print("--------------------------------")

        if logger.isEnabledFor(logging.DEBUG):
            msg = "update tags for series[%s] - dataset[%s] - provider[%s] - tags%s" 
            logger.debug(msg % (doc["key"],
                                doc["dataset_code"], 
                                provider_name,
                                tags))

        if len(bulk_list) >= max_bulk:
            result = db[constants.COL_SERIES].bulk_write(bulk_list)
            bulk_results.append(result)
            bulk_list = []
        
    if not dry_mode and len(bulk_list) > 0:
        result = db[constants.COL_SERIES].bulk_write(bulk_list)
        bulk_results.append(result)

    bulk_dict = {
        "nUpserted": 0, 
        "nModified": 0, #modified_count
        "nMatched": 0, #matched_count
        "nRemoved": 0,
        "nInserted": 0,
    }
    for b in bulk_results:
        bulk_dict["nMatched"] += b.matched_count
        bulk_dict["nModified"] += b.modified_count

    return bulk_dict

def _update_tags_series_async_gevent(db, provider_name=None, dataset_code=None, 
                               max_bulk=100, update_only=False, dry_mode=False):
    

    import gevent
    from gevent.pool import Pool
    from gevent.queue import Queue

    pool = Pool(10)
    queue = Queue()

    count_errors = 0
    count_success = 0
    
    def _queue_process():

        _requests = []
        count_series = 0
        count_modified = 0
    
        def _process_requests():
            result = db[constants.COL_SERIES].bulk_write(_requests, ordered=False, bypass_document_validation=True)
            return result.modified_count

        try:
            while True:
                _id, _tags = queue.get()
                if not _id:
                    break
                 
                count_series += 1
                
                _requests.append(UpdateOne({'_id': _id}, {"$set": {'tags': _tags}}))
                
                if len(_requests) >= max_bulk:
                    count_modified += _process_requests()
                    _requests = []
        finally:
            if len(_requests) > 0:
                count_modified += _process_requests()

        return count_series, count_modified

    doc_provider = db[constants.COL_PROVIDERS].find_one({"enable": True,
                                                         "name": provider_name})

    if not doc_provider:
        logger.error("Provider [%s] not found or disable." % provider_name)
        return
    
    dataset_query = {'provider_name': provider_name, "enable": True}
    dataset_projection = {"doc_href": False, 
                          "dimension_list": False, "attribute_list": False}

    if dataset_code:
        dataset_query["dataset_code"] = dataset_code

    def _series_list_process(doc_dataset):
        
        series_query = { "provider_name": doc_dataset["provider_name"], 
                         "dataset_code": doc_dataset["dataset_code"]}
        series_projection = {"values": False}
    
        if update_only:
            series_query["tags.0"] = {"$exists": False}

        _modified_count = 0
        
        pool_series = Pool(200)

        def _serie_process(doc):
            select_for_tags = [tag for tag in generate_tags_series_async(doc, doc_provider, doc_dataset)]

            tags = []
            for value in select_for_tags:
                tags.extend(str_to_tags(value))

            queue.put((doc["_id"], sorted(list(set(tags)))))
            
        for doc in db[constants.COL_SERIES].find(series_query, series_projection):
            pool_series.spawn(_serie_process, doc)
            
        pool_series.join()
        
    def _process_ds():
        count_ds = 0
        
        for doc_dataset in db[constants.COL_DATASETS].find(dataset_query, 
                                                           dataset_projection):
    
            count_ds += 1
            pool.spawn(_series_list_process, doc_dataset)
            
        pool.join()

        queue.put((None, None))
        return count_ds

    queue_green = gevent.spawn(_queue_process) 
    ds_green = gevent.spawn(_process_ds)
    try:
        gevent.joinall([ds_green, queue_green])
    except KeyboardInterrupt:
        pass 

    count_stats = dict(
        count_ds = ds_green.value,
        count_series = queue_green.value[0],
        count_errors = count_errors,
        count_success = count_success,
        count_modified = queue_green.value[1]
    )

    msg = "modified[%(count_modified)s] - errors[%(count_errors)s] - success[%(count_success)s] - datasets[%(count_ds)s] - series[%(count_series)s]"
    logger.info(msg % count_stats)
    
def update_tags_series(db, provider_name=None, dataset_code=None, max_bulk=100, 
                       update_only=False, dry_mode=False, async_mode=None):

    if not async_mode:
        return _update_tags_series_sync(db, 
                                        provider_name=provider_name, 
                                        dataset_code=dataset_code, max_bulk=max_bulk, 
                                        update_only=update_only, 
                                        dry_mode=dry_mode) 
    elif async_mode == "gevent":
        return _update_tags_series_async_gevent(db, 
                                        provider_name=provider_name, 
                                        dataset_code=dataset_code, max_bulk=max_bulk, 
                                        update_only=update_only, 
                                        dry_mode=dry_mode) 
    else:
        raise Exception("not supported async mode[%s]" % async_mode)
    
def search_tags(db, provider_name=None, dataset_code=None, 
                frequency=None,
                projection=None, 
                search_tags=None, search_type=None,
                start_date=None, end_date=None,
                sort=None, sort_desc=False,                        
                skip=None, limit=None):
    """Search in datasets or series by tags field
    
    >>> from dlstats import utils
    >>> db = utils.get_mongo_db()
    
    >>> docs = utils.search_series_tags(db, search_tags=["Belgium", "Euro"])    

    # Search in all provider and dataset
    >>> docs = utils.search_series_tags(db, frequency="A", search_tags=["Belgium", "Euro", "Agriculture"])

    # Filter provider and/or dataset
    >>> docs = utils.search_series_tags(db, provider_name="Eurostat", dataset_code="nama_10_a10", search_tags=["Belgium", "Euro", "Agriculture"])
    
    #print(docs.count())    
    #for doc in docs: print(doc['provider_name'], doc['dataset_code'], doc['key'], doc['name'])
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
        query['provider_name'] = {"$in": providers}
        
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
        query["enable"] = True
        
    cursor = db[COL_SEARCH].find(query, projection)

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
    projection = {"dimension_list": False, "attribute_list": False,
                  "concepts": False, "codelists": False}
    kwargs.setdefault("projection", projection)
    return search_tags(db, search_type=constants.COL_DATASETS, **kwargs)

def _aggregate_tags(db, source_col, target_col, add_match=None, max_bulk=20):

    bulk = db[target_col].initialize_unordered_bulk_op()
    count = 0
    
    match = {"tags.0": {"$exists": True}}
    if add_match:
        match.update(add_match)
    
    pipeline = [
      {"$match": match},
      {'$project': { '_id': 0, 'tags': 1, 'provider_name': 1}},
      {"$unwind": "$tags"},
      {"$group": {"_id": {"tag": "$tags", 'provider_name': "$provider"}, "count": {"$sum": 1}}},
      {'$project': { 'tag': "$_id.tag", 'count': 1, 'provider_name': {"name": "$_id.provider", "count": "$count"}}},
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
                           add_match={"enable": True}, 
                           max_bulk=max_bulk)

def aggregate_tags_series(db, max_bulk=20):
    #FIXME: dataset enable !
    return _aggregate_tags(db, 
                           constants.COL_SERIES, 
                           constants.COL_TAGS_SERIES, 
                           max_bulk=max_bulk)
