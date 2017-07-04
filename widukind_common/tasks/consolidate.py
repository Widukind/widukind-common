# -*- coding: utf-8 -*-

import logging
import json
import hashlib

import pymongo

from widukind_common import utils
from widukind_common import constants

logger = logging.getLogger(__name__)

def _run_bulk(db, bulk_requests):
    try:
        return bulk_requests.execute()
    except pymongo.errors.BulkWriteError as err:
        logger.critical(str(err.details))
    except Exception as err:
        logger.critical(str(err))

def hash_dict(d):
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()

def consolidate_all_dataset(provider_name=None, db=None, max_bulk=20):

    db = db or utils.get_mongo_db()

    query = {"provider_name": provider_name}
    projection = {"_id": True, "dataset_code": True}

    cursor = db[constants.COL_DATASETS].find(query, projection)
    dataset_codes = [doc["dataset_code"] for doc in cursor]

    bulk_requests = db[constants.COL_DATASETS].initialize_unordered_bulk_op()
    bulk_size = 0
    results = []

    for dataset_code in dataset_codes:

        query, query_modify = consolidate_dataset(provider_name, dataset_code, db=db, execute=False)

        if not query:
            logger.warning("bypass dataset [%s]" % dataset_code)
            continue

        bulk_size += 1
        bulk_requests.find(query).update_one(query_modify)

        if bulk_size > max_bulk:
            result = _run_bulk(db, bulk_requests)
            if result:
                results.append(result)
            bulk_requests = db[constants.COL_DATASETS].initialize_unordered_bulk_op()
            bulk_size = 0

    if bulk_size > 0:
        result = _run_bulk(db, bulk_requests)
        if result:
            results.append(result)

    results_details = {
        "matched_count": 0,
        "modified_count": 0,
    }
    for r in results:
        results_details["matched_count"] += r["nMatched"]
        results_details["modified_count"] += r["nModified"]

    return results_details

def consolidate_dataset(provider_name=None, dataset_code=None, db=None, execute=True):
    db = db or utils.get_mongo_db()

    logger.info("START consolidate provider[%s] - dataset[%s]" % (provider_name, dataset_code))

    query = {"provider_name": provider_name, "dataset_code": dataset_code}
    projection = {"_id": False, "dimensions": True, "attributes": True, "values.attributes": True}

    cursor = db[constants.COL_SERIES].find(query, projection)

    projection = {"_id": True, "concepts": True, "codelists": True, "dimension_keys": True, "attribute_keys": True}
    dataset = db[constants.COL_DATASETS].find_one(query, projection)
    if dataset is None:
        if execute:
            return None
        else:
            return None, None

    old_codelists = dataset.get("codelists") or {}
    old_concepts = dataset.get("concepts") or {}
    old_dimension_keys = dataset.get("dimension_keys") or []
    old_attribute_keys = dataset.get("attribute_keys") or []

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
                    if not k1 in old_codelists: continue
                    if not k1 in codelists: codelists[k1] = []
                    if not v1 in codelists[k1]: codelists[k1].append(v1)

    if logger.isEnabledFor(logging.DEBUG):
        for k, v in old_codelists.items():
            logger.debug("BEFORE - codelist[%s]: %s" % (k, len(v)))
        logger.debug("BEFORE - concepts[%s]" % list(old_concepts.keys()))
        logger.debug("BEFORE - dimension_keys[%s]" % old_dimension_keys)
        logger.debug("BEFORE - attribute_keys[%s]" % old_attribute_keys)

    new_codelists = {}
    new_concepts = {}
    new_dimension_keys = []
    new_attribute_keys = []

    for k, values in old_codelists.items():
        '''if entry in codelists from series'''
        if k in codelists:
            new_values = {}
            for v1 in codelists[k]:
                '''if codelist value in codelists from dataset'''
                if v1 in values:
                    new_values[v1] = values[v1]

            new_codelists[k] = new_values
            new_concepts[k] = old_concepts.get(k)

            if k in old_dimension_keys:
                '''unordered dimension_keys'''
                new_dimension_keys.append(k)
            elif k in old_attribute_keys:
                '''unordered attribute_keys'''
                new_attribute_keys.append(k)

    '''original ordered for dimension_keys'''
    dimension_keys = [k for k in old_dimension_keys if k in new_dimension_keys]
    '''original ordered for attribute_keys'''
    attribute_keys = [k for k in old_attribute_keys if k in new_attribute_keys]

    if logger.isEnabledFor(logging.DEBUG):
        for k, v in new_codelists.items():
            logger.debug("AFTER - codelist[%s]: %s" % (k, len(v)))
        logger.debug("AFTER - concepts[%s]" % list(new_concepts.keys()))
        logger.debug("AFTER - dimension_keys[%s]" % dimension_keys)
        logger.debug("AFTER - attribute_keys[%s]" % attribute_keys)

    '''verify change in codelists'''
    #is_modify = hash_dict(new_codelists) == hash_dict(old_codelists)
    is_modify = new_codelists != old_codelists

    '''verify change in concepts'''
    #if not is_modify and hash_dict(new_concepts) != hash_dict(old_concepts):
    if is_modify is False and new_concepts != old_concepts:
        is_modify = True

    if is_modify is False:
        if execute:
            return None
        else:
            return None, None

    query = {"_id": dataset["_id"]}
    query_modify = {"$set": {
        "codelists": new_codelists or None,
        "concepts": new_concepts or None,
        "dimension_keys": dimension_keys or None,
        "attribute_keys": attribute_keys or None,
    }}

    if execute:
        return db[constants.COL_DATASETS].update_one(query, query_modify).modified_count
    else:
        return query, query_modify
