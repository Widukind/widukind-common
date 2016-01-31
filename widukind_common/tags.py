# -*- coding: utf-8 -*-

import logging
from pprint import pprint
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

logger = logging.getLogger(__name__)

def tags_filter(value):
    
    if not value:
        return False

    if value in constants.TAGS_EXCLUDE_WORDS:
        return False
    
    if not value or len(value.strip()) < TAGS_MIN_CHAR:
        return False
    
    if value in ["-", "_", " "]:
        return False
    
    return True            

def tags_map(value):
    
    if not value:
        return

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
    if tags:
        return [a for a in filter(tags_filter, tags)]
    return []

def get_categories_tags_for_dataset(db, provider_name, dataset_code):
    tags = []
    query = {"provider_name": provider_name, "enable": True,
              "datasets.dataset_code": dataset_code}
    projection = {"datasets": False, "_id": False, 
                  "doc_href": False, "metadata": False}        
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
        
        all_parents = db[constants.COL_CATEGORIES].find(_query)
        
        for parent in all_parents:
            _tags = generate_tags_categories(db, parent, doc_provider)
            select_for_tags.extend(_tags)

    for value in select_for_tags:
        tags.extend(str_to_tags(value))
        
    return sorted(list(set(tags)))

def generate_tags_dataset(db, doc, doc_provider):
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
    
    if doc.get("codelists"):
        if doc.get("dimension_keys"):
            for key in doc["dimension_keys"]:
                if not key in doc["codelists"]:
                    continue
                for value in doc['codelists'][key].values():
                    select_for_tags.append(value)
    
        if doc.get("attribute_keys"):
            for key in doc["attribute_keys"]:
                if not key in doc["codelists"]:
                    continue
                for value in doc['codelists'][key].values():
                    select_for_tags.append(value)

    categories_tags = get_categories_tags_for_dataset(db, 
                                                      doc["provider_name"], 
                                                      doc["dataset_code"])
    if categories_tags:
        tags.extend(categories_tags)
    else:
        logger.warning("Not categories tags for provider[%s] - dataset[%s]" % (doc_provider['name'], 
                                                                               doc["dataset_code"]))
    
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
    
    if categories_tags:
        tags = categories_tags
    
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

def update_tags_categories(db, provider_name=None, 
                           max_bulk=100, dry_mode=False):

    doc_provider = db[constants.COL_PROVIDERS].find_one({"enable": True,
                                                         "name": provider_name})

    if not doc_provider:
        logger.error("Provider [%s] not found or disable." % provider_name)
        return
    
    query = {'provider_name': provider_name, "enable": True}
    projection = {"datasets": False, "doc_href": False}

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
        run_bulk(bulk)

def update_tags_datasets(db, provider_name=None, dataset_code=None, 
                         max_bulk=100, dry_mode=False):

    doc_provider = db[constants.COL_PROVIDERS].find_one({"enable": True,
                                                         "name": provider_name})

    if not doc_provider:
        logger.error("Provider [%s] not found or disable." % provider_name)
        return
    
    query = {'provider_name': provider_name, "enable": True}
    projection = {"doc_href": False, "dimension_list": False, "attribute_list": False}
    
    if dataset_code:
        query["dataset_code"] = dataset_code

    bulk = db[constants.COL_DATASETS].initialize_unordered_bulk_op()
    bulk_list = []
    
    for doc in db[constants.COL_DATASETS].find(query, projection):
        tags = generate_tags_dataset(db, doc, doc_provider)

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
            run_bulk(bulk)
            bulk = db[constants.COL_DATASETS].initialize_unordered_bulk_op()
            bulk_list = []
            
    if not dry_mode and len(bulk_list) > 0:
        for b in bulk_list:
            bulk.find({'_id': b[0]}).update_one({"$set": {'tags': b[1]}})
        run_bulk(bulk)

def update_tags_series(db, provider_name=None, dataset_code=None, 
                       max_bulk=100, dry_mode=False):

    doc_provider = db[constants.COL_PROVIDERS].find_one({"enable": True,
                                                         "name": provider_name})

    if not doc_provider:
        logger.error("Provider [%s] not found or disable." % provider_name)
        return
    
    dataset_query = {'provider_name': provider_name, "enable": True}
    dataset_projection = {"doc_href": False, 
                          "dimension_list": False, "attribute_list": False}

    series_query = {'provider_name': doc_provider["name"]}
    series_projection = {"values": False}
    
    if dataset_code:
        dataset_query["dataset_code"] = dataset_code
        series_query["dataset_code"] = dataset_code

    bulk = db[constants.COL_SERIES].initialize_unordered_bulk_op()
    bulk_list = []
        
    for doc_dataset in db[constants.COL_DATASETS].find(dataset_query, dataset_projection):

        categories_tags = get_categories_tags_for_dataset(db, 
                                                          provider_name, 
                                                          doc_dataset["dataset_code"])
        if not categories_tags:
            logger.warning("Not categories tags for provider[%s] - dataset[%s]" % (provider_name, 
                                                                                   doc_dataset["dataset_code"]))
        
        for doc in db[constants.COL_SERIES].find(series_query, series_projection):

            tags = generate_tags_series(db, doc, doc_provider, doc_dataset, categories_tags)
        
            if not dry_mode and tags:
                bulk_list.append((doc["_id"], tags))

            if logger.isEnabledFor(logging.DEBUG):
                msg = "update tags for series[%s] - dataset[%s] - provider[%s] - tags%s" 
                logger.debug(msg % (doc["key"],
                                    doc["dataset_code"], 
                                    provider_name,
                                    tags))

            elif dry_mode and tags:
                print("--------------------------------")
                print("dataset[%s] - series[%s] - tags[%s]" % (doc_dataset["dataset_code"],
                                                               doc["key"], ", ".join(tags)))
                print("--------------------------------")
    
            if len(bulk_list) > max_bulk:
                for b in bulk_list:
                    bulk.find({'_id': b[0]}).update_one({"$set": {'tags': b[1]}})
                run_bulk(bulk)
                bulk = db[constants.COL_SERIES].initialize_unordered_bulk_op()
                bulk_list = []
            
    if not dry_mode and len(bulk_list) > 0:
        for b in bulk_list:
            bulk.find({'_id': b[0]}).update_one({"$set": {'tags': b[1]}})
        run_bulk(bulk)
    
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
