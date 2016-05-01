# -*- coding: utf-8 -*-

import sys
import logging.config

import arrow
    
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

def create_or_update_indexes(db, force_mode=False, background=True):
    """Create or update MongoDB indexes"""
    
    global UPDATE_INDEXES
    
    if not force_mode and UPDATE_INDEXES:
        return

    '''********* CALENDARS ********'''

    db[constants.COL_CALENDARS].create_index([
        ("key", ASCENDING)], 
        name="key_idx", unique=True, background=background)

    '''********* PROVIDERS ********'''

    db[constants.COL_PROVIDERS].create_index([
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True, background=background)

    db[constants.COL_PROVIDERS].create_index([
        ("name", ASCENDING)], 
        name="name_idx", 
        background=background)

    '''********* CATEGORIES *******'''

    db[constants.COL_CATEGORIES].create_index([
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True, background=background)

    db[constants.COL_CATEGORIES].create_index([
        ('provider_name', ASCENDING), 
        ("category_code", ASCENDING)], 
        name="provider_category_idx", 
        #unique=True, 
        background=background)
    
    db[constants.COL_CATEGORIES].create_index([
        ("tags", ASCENDING)], 
        name="tags_idx", background=background)

    '''********* DATASETS *********'''
    
    db[constants.COL_DATASETS].create_index([
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True, background=background)

    db[constants.COL_DATASETS].create_index([
        ("tags", ASCENDING)], 
        name="tags_idx", background=background)
    
    db[constants.COL_DATASETS].create_index([
        ('provider_name', ASCENDING), 
        ("dataset_code", ASCENDING)], 
        name="datasets1", 
        background=background)

    db[constants.COL_DATASETS].create_index([
        ('provider_name', ASCENDING), 
        ("dataset_code", ASCENDING),
        ("tags", ASCENDING)], 
        name="datasets2", background=background)

    db[constants.COL_DATASETS].create_index([
        ("last_update", ASCENDING)], 
        name="datasets3")

    '''********* SERIES *********'''

    db[constants.COL_SERIES].create_index([
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True, background=background)

    db[constants.COL_SERIES].create_index([
        ('provider_name', ASCENDING), 
        ("dataset_code", ASCENDING), 
        #("key", ASCENDING)
        ], 
        name="series1", 
        background=background)

    db[constants.COL_SERIES].create_index([
        ("dimensions", ASCENDING)], 
        name="series2", background=background)

    db[constants.COL_SERIES].create_index([
        ("attributes", ASCENDING)], 
        name="series3", background=background)

    db[constants.COL_SERIES].create_index([
        ("tags", ASCENDING)], 
        name="series4", background=background)

    #db[constants.COL_SERIES].create_index([
    #    ("tags", ASCENDING),
    #    ('provider_name', ASCENDING)], 
    #    name="series5", background=background)

    #db[constants.COL_SERIES].create_index([
    #    ("tags", ASCENDING),
    #    ('provider_name', ASCENDING), 
    #    ("dataset_code", ASCENDING)], 
    #    name="series6", background=background)

    db[constants.COL_SERIES].create_index([
        ("frequency", ASCENDING)], 
        name="series7", background=background)

    db[constants.COL_SERIES].create_index([
        ("start_ts", ASCENDING),        
        ("end_ts", ASCENDING)], 
        name="series8", background=background)

    '''********* TAGS ***********'''

    db[constants.COL_TAGS].create_index([
        ("name", ASCENDING)], 
        name="name_idx", unique=True, background=background)

    db[constants.COL_TAGS].create_index([
        ("count", DESCENDING)], 
        name="count_idx", background=background)

    db[constants.COL_TAGS].create_index([
        ("count_datasets", DESCENDING)], 
        name="count_datasets_idx", 
        background=background,
        partialFilterExpression={"count_datasets": {"$exists": True}})

    db[constants.COL_TAGS].create_index([
        ("count_series", DESCENDING)], 
        name="count_series_idx", 
        background=background,
        partialFilterExpression={"count_series": {"$exists": True}})

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

def utcnow():
    return arrow.utcnow().datetime
