# -*- coding: utf-8 -*-

import sys
import logging
import logging.config
    
from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING

from widukind_common import constants

def get_mongo_url():
    return constants.MONGODB_URL

def get_mongo_client(url=None):
    # TODO: tz_aware
    url = url or get_mongo_url()
    client = MongoClient(url)
    return client

def get_mongo_db(url=None):
    # TODO: tz_aware
    url = url or get_mongo_url()
    client = get_mongo_client(url)
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
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True)

    '''********* CATEGORIES *******'''
    
    db[constants.COL_CATEGORIES].create_index([
        ("provider", ASCENDING), 
        ("categoryCode", ASCENDING)], 
        name="provider_categoryCode_idx", unique=True)

    db[constants.COL_CATEGORIES].create_index([
        ("tags", ASCENDING)], 
        name="tags_idx")
         
    
    '''********* DATASETS *********'''
    
    #TODO: last_update DESCENDING ?
    db[constants.COL_DATASETS].create_index([
        ("provider", ASCENDING), 
        ("dataset_code", ASCENDING)], 
        name="provider_dataset_code_idx", unique=True)

    db[constants.COL_DATASETS].create_index([
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True)
        
    db[constants.COL_DATASETS].create_index([
        ("name", ASCENDING)], 
        name="name_idx")

    db[constants.COL_DATASETS].create_index([
        ("tags", ASCENDING)], 
        name="tags_idx")
    
    db[constants.COL_DATASETS].create_index([
        ("last_update", DESCENDING)], 
        name="last_update_idx")

    '''********* SERIES *********'''

    db[constants.COL_SERIES].create_index([
        ("provider", ASCENDING), 
        ("dataset_code", ASCENDING), 
        ("key", ASCENDING)], 
        name="provider_dataset_code_key_idx", unique=True)

    db[constants.COL_SERIES].create_index([
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True)

    db[constants.COL_SERIES].create_index([
        ("key", ASCENDING)], 
        name="key_idx")

    db[constants.COL_SERIES].create_index([
        ("provider", ASCENDING), 
        ("dataset_code", ASCENDING)], 
        name="provider_dataset_code_idx")

    db[constants.COL_SERIES].create_index([
        ("dataset_code", ASCENDING)], 
        name="dataset_code_idx")

    db[constants.COL_SERIES].create_index([
        ("provider", ASCENDING)], 
        name="provider_idx")    

    db[constants.COL_SERIES].create_index([
        ("tags", ASCENDING)], 
        name="tags_idx")
    
    db[constants.COL_SERIES].create_index([
        ("name", ASCENDING)], 
        name="name_idx")
    
    db[constants.COL_SERIES].create_index([
        ("frequency", DESCENDING)], 
        name="frequency_idx")

    db[constants.COL_SERIES].create_index([
        ("provider", ASCENDING), 
        ("tags", ASCENDING), 
        ("frequency", DESCENDING)], 
        name="provider_tags_frequency_idx")

    '''********* TAGS ***********'''

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
                'format': '%(asctime)s %(name)s: [%(levelname)s] - %(message)s',
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
            '': {
                'handlers': [],
                'level': 'INFO',
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
            LOGGING['handlers'][handler]['formatter'] = 'debug'
            LOGGING['handlers'][handler]['level'] = 'DEBUG' 

    logging.config.dictConfig(LOGGING)
    return logging.getLogger()

