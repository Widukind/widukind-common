# -*- coding: utf-8 -*-

import sys
import logging
import logging.config
    
from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING

from widukind_common import constants

def get_mongo_url():
    return constants.MONGODB_URL

def get_mongo_client(url=None, connect=False):
    # TODO: tz_aware
    url = url or get_mongo_url()
    client = MongoClient(url, connect=connect)
    return client

def get_mongo_db(url=None, **kwargs):
    # TODO: tz_aware
    url = url or get_mongo_url()
    client = get_mongo_client(url, **kwargs)
    return client.get_default_database()

UPDATE_INDEXES = False

def create_or_update_indexes(db, force_mode=False):
    """Create or update MongoDB indexes"""
    
    global UPDATE_INDEXES
    
    if not force_mode and UPDATE_INDEXES:
        return

    '''********* PROVIDERS ********'''

    db[constants.COL_PROVIDERS].create_index([
        ("name", ASCENDING)], 
        name="name_idx", unique=True)

    db[constants.COL_PROVIDERS].create_index([
        ('enable', ASCENDING)], 
        name="enable_idx")

    '''********* CATEGORIES *******'''
    
    db[constants.COL_CATEGORIES].create_index([
        ('provider_name', ASCENDING), 
        ("category_code", ASCENDING)], 
        name="provider_category_idx", unique=True)

    db[constants.COL_CATEGORIES].create_index([
        ("parent", ASCENDING)], 
        name="parent_idx")

    db[constants.COL_CATEGORIES].create_index([
        ("all_parents", ASCENDING)], 
        name="all_parents_idx")

    db[constants.COL_CATEGORIES].create_index([
        ("provider_name", ASCENDING)], 
        name="provider_idx")
        
    db[constants.COL_CATEGORIES].create_index([
        ("datasets.dataset_code", ASCENDING)], 
        name="datasets_idx")
    
    '''********* DATASETS *********'''
    
    db[constants.COL_DATASETS].create_index([
        ('provider_name', ASCENDING), 
        ("dataset_code", ASCENDING)], 
        name="provider_dataset_idx", unique=True)

    db[constants.COL_DATASETS].create_index([
        ('enable', ASCENDING)], 
        name="enable_idx")

    db[constants.COL_DATASETS].create_index([
        ("provider_name", ASCENDING)], 
        name="provider_idx")
        
    db[constants.COL_DATASETS].create_index([
        ("name", ASCENDING)], 
        name="name_idx")

    db[constants.COL_DATASETS].create_index([
        ('enable', ASCENDING), 
        ("last_update", DESCENDING)], 
        name="last_update_idx")

    '''********* SERIES *********'''

    db[constants.COL_SERIES].create_index([
        ('provider_name', ASCENDING), 
        ("dataset_code", ASCENDING), 
        ("key", ASCENDING)], 
        name="provider_dataset_key_idx", unique=True)

    db[constants.COL_SERIES].create_index([
        ('provider_name', ASCENDING), 
        ("dataset_code", ASCENDING)], 
        name="provider_dataset_idx")

    db[constants.COL_SERIES].create_index([
        ("provider_name", ASCENDING)], 
        name="provider_idx")

    db[constants.COL_SERIES].create_index([
        ("dataset_code", ASCENDING)], 
        name="dataset_code_idx")

    db[constants.COL_SERIES].create_index([
        ("key", ASCENDING)], 
        name="key_idx")

    db[constants.COL_SERIES].create_index([
        ("name", ASCENDING)], 
        name="name_idx")
    
    db[constants.COL_SERIES].create_index([
        ("frequency", DESCENDING)], 
        name="frequency_idx")

    db[constants.COL_SERIES].create_index([
        ("values.revisions", ASCENDING)], 
        name="valuesrevisions_idx")

    '''Search form'''
    db[constants.COL_SERIES].create_index([
        ('provider_name', ASCENDING), 
        ("tags", ASCENDING)], 
        name="provider_tags_idx")

    db[constants.COL_SERIES].create_index([
        ('provider_name', ASCENDING), 
        ("dataset_code", ASCENDING), 
        ("tags", ASCENDING)], 
        name="provider_dataset_code_tags_idx")
    
    db[constants.COL_SERIES].create_index([
        ('provider_name', ASCENDING), 
        ("tags", ASCENDING), 
        ("frequency", DESCENDING)], 
        name="provider_tags_frequency_idx")
    
    db[constants.COL_SERIES].create_index([
        ('provider_name', ASCENDING), 
        ("dataset_code", ASCENDING), 
        ("dimensions", ASCENDING)], 
        name="provider_dataset_code_dimensions_idx")

    db[constants.COL_SERIES].create_index([
        ('provider_name', ASCENDING), 
        ("dataset_code", ASCENDING), 
        ("attributes", ASCENDING)], 
        name="provider_dataset_code_attributes_idx")

    '''********* TAGS ***********'''

    db[constants.COL_CATEGORIES].create_index([
        ("tags", ASCENDING)], 
        name="tags_idx")

    db[constants.COL_DATASETS].create_index([
        ("tags", ASCENDING)], 
        name="tags_idx")
    
    db[constants.COL_SERIES].create_index([
        ("tags", ASCENDING)], 
        name="tags_idx")
    
    db[constants.COL_TAGS_DATASETS].create_index([
        ("name", ASCENDING)], 
        name="name_idx", unique=True)

    db[constants.COL_TAGS_DATASETS].create_index([
        ("providers.name", ASCENDING)], 
        name="providers_name_idx")

    db[constants.COL_TAGS_SERIES].create_index([
        ("name", ASCENDING)], 
        name="name_idx", unique=True)

    db[constants.COL_TAGS_SERIES].create_index([
        ("providers.name", ASCENDING)], 
        name="providers_name_idx")
    
    '''********* SLUG ***********'''

    db[constants.COL_PROVIDERS].create_index([
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True)

    db[constants.COL_CATEGORIES].create_index([
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True)

    db[constants.COL_DATASETS].create_index([
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True)

    db[constants.COL_SERIES].create_index([
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True)

    
    
    UPDATE_INDEXES = True

def configure_logging(debug=False, stdout_enable=True, config_file=None,
                      level="INFO"):

    if config_file:
        logging.config.fileConfig(config_file, disable_existing_loggers=True)
        return logging.getLogger('')

    #TODO: handler file ?    
    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'debug': {
                'format': 'line:%(lineno)d - %(asctime)s %(name)s: [%(levelname)s] - [%(process)d] - [%(module)s] - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
            'simple': {
                'format': '[%(process)d] - %(asctime)s %(name)s: [%(levelname)s] - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
        },    
        'handlers': {
            'null': {
                'level':level,
                'class':'logging.NullHandler',
            },
            'console':{
                'level':level,
                'class':'logging.StreamHandler',
                'formatter': 'simple',
                'stream': sys.stdout
            },      
        },
        'loggers': {
            'requests.packages.urllib3': {
                'handlers': ['null'],
                'level': 'NOTSET',
            },
            '': {
                'handlers': [],
                'level': level,
                'propagate': False,
            },
    
        },
    }
    
    if stdout_enable:
        if not 'console' in LOGGING['loggers']['']['handlers']:
            LOGGING['loggers']['']['handlers'].append('console')

    '''if handlers is empty'''
    if not LOGGING['loggers']['']['handlers']:
        LOGGING['loggers']['']['handlers'] = ['console']
    
    if debug:
        LOGGING['loggers']['']['level'] = 'DEBUG'
        for handler in LOGGING['handlers'].keys():
            if handler != 'null':
                LOGGING['handlers'][handler]['formatter'] = 'debug'
                LOGGING['handlers'][handler]['level'] = 'DEBUG' 

    logging.config.dictConfig(LOGGING)
    return logging.getLogger()

