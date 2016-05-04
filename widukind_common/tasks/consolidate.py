# -*- coding: utf-8 -*-

import logging
import json
import hashlib

import pymongo
from pymongo import UpdateOne

from widukind_common import utils
from widukind_common import constants

logger = logging.getLogger(__name__)

def _run_bulk(db, bulk_requests):
    try:
        return db[constants.COL_DATASETS].bulk_write(bulk_requests, ordered=False)
    except pymongo.errors.BulkWriteError as err:
        logger.critical(str(err.details))

def hash_dict(d):
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()     
    
def consolidate_all_dataset(provider_name=None, db=None, max_bulk=20):
    
    db = db or utils.get_mongo_db()
    
    query = {"provider_name": provider_name}
    projection = {"_id": False, "dataset_code": True}
    
    cursor = db[constants.COL_DATASETS].find(query, projection)

    bulk_requests = []
    results = []
    
    for dataset in cursor:

        query, query_modify = consolidate_dataset(provider_name, dataset["dataset_code"], db=db, execute=False)
        
        if not query:
            logger.warning("bypass dataset [%s]" % dataset["dataset_code"])
            continue
        
        bulk_requests.append(UpdateOne(query, query_modify))
    
        if len(bulk_requests) > max_bulk:
            result = _run_bulk(db, bulk_requests)
            if result:
                results.append(result)
            bulk_requests = []
    
    if len(bulk_requests) > 0:
        result = _run_bulk(db, bulk_requests)
        if result:
            results.append(result)
        
    results_details = {
        "matched_count": 0,
        "modified_count": 0,
    }
    for r in results:
        results_details["matched_count"] += r.matched_count
        results_details["modified_count"] += r.modified_count

    return results_details

def consolidate_dataset(provider_name=None, dataset_code=None, db=None, execute=True):
    db = db or utils.get_mongo_db()
    
    logger.info("START consolidate provider[%s] - dataset[%s]" % (provider_name, dataset_code))
    
    query = {"provider_name": provider_name, "dataset_code": dataset_code}
    projection = {"_id": False, "dimensions": True, "attributes": True, "values.attributes": True}
    
    cursor = db[constants.COL_SERIES].find(query, projection)

    projection = {"_id": True, "concepts": True, "codelists": True, "dimension_keys": True, "attribute_keys": True}             
    dataset = db[constants.COL_DATASETS].find_one(query, projection)
    
    codelists = {}
    
    for series in cursor:
        for k, v in series.get("dimensions").items():
            if not k in codelists: codelists[k] = []
            if not v in codelists[k]: codelists[k].append(v)
        
        if series.get("attributes"):
            for k, v in series.get("attributes").items():
                if not k in codelists: codelists[k] = []
                if not v in codelists[k]: codelists[k].append(v)
            
        for v in series.get("values"):
            if v.get("attributes"):
                for k1, v1 in v.get("attributes").items():
                    if not k1 in dataset["codelists"]: continue# or not v1: continue
                    if not k1 in codelists: codelists[k1] = []
                    if not v1 in codelists[k1]: codelists[k1].append(v1)
    
    if logger.isEnabledFor(logging.DEBUG):
        for k, v in dataset["codelists"].items():
            logger.debug("BEFORE - codelist[%s]: %s" % (k, len(v)))
        logger.debug("BEFORE - concepts[%s]" % list(dataset["concepts"].keys()))
    
    new_codelists = {}
    new_concepts = {}
    new_dimension_keys = []
    new_attribute_keys = []
    
    for k, values in dataset["codelists"].items():
        if k in codelists:
            new_values = {}
            for v1 in codelists[k]:
                if v1 in values:
                    new_values[v1] = values[v1]
            
            new_codelists[k] = new_values
            new_concepts[k] = dataset["concepts"].get(k)
            
            if k in dataset["dimension_keys"]:
                new_dimension_keys.append(k)
            elif k in dataset["attribute_keys"]:
                new_attribute_keys.append(k)
    
    if logger.isEnabledFor(logging.DEBUG):
        for k, v in new_codelists.items():
            logger.debug("AFTER - codelist[%s]: %s" % (k, len(v)))
        logger.debug("AFTER - concepts[%s]" % list(new_concepts.keys()))
    
    is_modify = hash_dict(new_codelists) == hash_dict(codelists)

    if not is_modify and hash_dict(new_concepts) != hash_dict(dataset["concepts"]):
        is_modify = True
    
    if not is_modify:
        if execute:
            return None
        else:
            return None, None

    query = {"_id": dataset["_id"]}
    query_modify = {"$set": {
        "codelists": new_codelists, 
        "concepts": new_concepts,
        "dimension_keys": new_dimension_keys,
        "attribute_keys": new_attribute_keys
    }}
    
    if execute:
        return db[constants.COL_DATASETS].update_one(query, query_modify).modified_count
    else:
        return query, query_modify
    
    
    