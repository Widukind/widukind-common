# -*- coding: utf-8 -*-

import sys
import logging
import logging.config
from functools import wraps
import time

import arrow
    
from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING, TEXT
from pymongo.errors import AutoReconnect
from bson import json_util

import six
import zlib
import base64

from widukind_common import constants

logger = logging.getLogger(__name__)

def get_mongo_url():
    return constants.MONGODB_URL.strip('"')

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

INDEXES = {
    constants.COL_CALENDARS:
        {
            "name": "key_idx",
            "ns": constants.COL_CALENDARS,
            "key": [("key", ASCENDING)],
            "unique": True, 
        },
}

def create_or_update_indexes(db, force_mode=False, background=False):
    """Create or update MongoDB indexes"""
    
    global UPDATE_INDEXES
    
    if not force_mode and UPDATE_INDEXES:
        return
    
    indexes = {}
    collection_names = db.collection_names()
    for col in constants.COL_ALL:
        if col in collection_names:
            indexes[col] = db[col].index_information()
    
    '''********* CALENDARS ********'''

    logger.info("create calendars indexes...")
    if not constants.COL_CALENDARS in indexes or not "key_idx" in indexes[constants.COL_CALENDARS]:
        db[constants.COL_CALENDARS].create_index([
            ("key", ASCENDING)], 
            name="key_idx", unique=True, background=background)
    else:
        pass

    '''********* PROVIDERS ********'''

    logger.info("create providers indexes...")

    db[constants.COL_PROVIDERS].create_index([
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True, background=background)

    db[constants.COL_PROVIDERS].create_index([
        ("name", ASCENDING)], 
        name="name_idx", 
        background=background)

    '''********* CATEGORIES *******'''

    logger.info("create categories indexes...")

    db[constants.COL_CATEGORIES].create_index([
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True, background=background)

    db[constants.COL_CATEGORIES].create_index([
        ('provider_name', ASCENDING), 
        ("category_code", ASCENDING)], 
        name="provider_category_idx", 
        background=background)
    
    #db[constants.COL_CATEGORIES].create_index([
    #    ("tags", ASCENDING)], 
    #    name="tags_idx", background=background)

    '''********* DATASETS *********'''
    
    logger.info("create datasets indexes...")
    
    db[constants.COL_DATASETS].create_index([
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True, background=background)

    db[constants.COL_DATASETS].create_index([
        ('provider_name', ASCENDING), 
        ("dataset_code", ASCENDING)], 
        unique=True, name="datasets1", 
        background=background)

    db[constants.COL_DATASETS].create_index([
        ('provider_name', ASCENDING), 
        ("tags", ASCENDING)], 
        name="datasets2", background=background)

    db[constants.COL_DATASETS].create_index([
        ("last_update", ASCENDING)], 
        name="datasets3")

    db[constants.COL_DATASETS].create_index([
        ("enable", ASCENDING)], 
        name="disable_datasets",
        partialFilterExpression={"enable": False})

    '''********* SERIES *********'''

    logger.info("create series indexes...")

    db[constants.COL_SERIES].create_index([
        ("slug", ASCENDING)], 
        name="slug_idx", unique=True, background=background)

    db[constants.COL_SERIES].create_index([
        ('provider_name', ASCENDING), 
        ("dataset_code", ASCENDING),
        ("key", ASCENDING)], 
        unique=True, name="series1", 
        background=background)

    db[constants.COL_SERIES].create_index([
        #TODO: ('provider_name', ASCENDING),
        ("dimensions", ASCENDING)], 
        name="series2", background=background)

    db[constants.COL_SERIES].create_index([
        #TODO: ('provider_name', ASCENDING),
        ("attributes", ASCENDING)], 
        name="series3", background=background)

    db[constants.COL_SERIES].create_index([
        ("tags", ASCENDING)], 
        name="series4", background=background)

    db[constants.COL_SERIES].create_index([
        ("provider_name", ASCENDING),
        ('dataset_code', TEXT),
        ('name', TEXT),
        ('key', TEXT),
        ('slug', TEXT),
        ('tags', TEXT),
        ('dimensions', TEXT),
        ('attributes', TEXT),
        ('notes', TEXT),
        ('codelists', TEXT),
        ], 
        name="fulltext",
        default_language="english", 
        weights= {
            "notes":1, 
            "key": 2, 
            "attributes": 2,
            "tags":3, 
            "dataset_code": 4, 
            "dimensions": 4, 
            "codelists": 4,
            "name": 5, 
        }, 
        background=background)
    
    db[constants.COL_SERIES].create_index([
        ("frequency", ASCENDING)], 
        name="series7", background=background)

    db[constants.COL_SERIES].create_index([
        ("dataset_code", ASCENDING)], 
        name="series9", background=background)

    '''********* SERIES ARCHIVES *********'''

    logger.info("create series_archives indexes...")

    db[constants.COL_SERIES_ARCHIVES].create_index([
        ("slug", ASCENDING),
        ("version", DESCENDING)], 
        name="slug_idx", background=background)

    db[constants.COL_SERIES_ARCHIVES].create_index([
        ('provider_name', ASCENDING), 
        ("dataset_code", ASCENDING)], 
        name="series1", 
        background=background)

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
            '': {
                'handlers': [],
                'level': level,
                'propagate': False,
            },    
        },
    }
    
    logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.ERROR)
    logging.getLogger("dlstats.fetchers.insee").setLevel(logging.INFO)
    
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

def load_klass(name):
    
    if not name:
        raise ValueError("name is required")

    module_name = ".".join(name.split(".")[:-1])
    klass_name = name.split(".")[-1]
    
    if not module_name in sys.modules:
        __import__(module_name)
        
    mod = sys.modules[module_name]
    
    return getattr(mod, klass_name)

def retry_on_reconnect_error(retry_count=2, exponential_delay=True):
    """
    Automatic retry on PyMongo AutoReconnect exceptions.
    Inspired by https://gist.github.com/inactivist/9086391
    
    Usage:
    
    @retry_on_reconnect_error()
    def my_function():
       ...
    
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if retry_count < 1:
                raise ValueError("retry_count must be 1 or higher.")
            for i in range(retry_count):
                try:
                    logger.debug('trying db request %s', f)
                    v = f(*args, **kwargs)
                    logger.debug('db request %s success', f)
                    return v
                except AutoReconnect as e:
                    if exponential_delay:
                        method = "exponential"
                        delay = pow(2, i)
                    else:
                        method = "simple"
                        delay =.1

                    logger.warn('Transient error %s (retry #%d)'
                                                  ', method=%s sleeping %f',
                                                  e,
                                                  i+1,
                                                  method,
                                                  delay)
                    time.sleep(delay)
            msg = 'AutoReconnect retry failed.'
            logger.error(msg)
            raise Exception(msg)
        return decorated_function
    return decorator

def series_archives_store(series):
    '''Compress one series document for store in mongodb'''
    store = {
        "slug": series.pop("slug"),
        "version": series.pop("version", 0),
        "provider_name": series["provider_name"],
        "dataset_code": series["dataset_code"],
        "datas": zlib.compress(json_util.dumps(series).encode())
    }
    return store

def series_archives_load(store):
    '''Uncompress series archives and return dict'''
    series = json_util.loads(zlib.decompress(store['datas']).decode())
    series["slug"] = store["slug"]
    series["version"] = store["version"]
    return series
    

