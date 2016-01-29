# -*- coding: utf-8 -*-

import tempfile
import time
import logging
import csv

import pandas
import gridfs

from widukind_common.utils import get_mongo_db
from widukind_common import constants

logger = logging.getLogger(__name__)

def generate_filename(provider_name=None, dataset_code=None, key=None, 
                      slug=None, prefix=None):
    """Generate filename for file (csv, pdf, ...)
    """    
    if slug:
        filename = "widukind-%s-%s" % (prefix, slug)
    elif key:
        filename = "widukind-%s-%s-%s-%s" % (prefix, provider_name, dataset_code, key)
    else:
        filename = "widukind-%s-%s-%s" % (prefix, provider_name, dataset_code)
        
    return filename.lower().replace(" ", "-")

def generate_filename_csv(**kwargs):
    return "%s.csv" % generate_filename(**kwargs)

def export_series(series):
    """Export one serie (Period and Frequency only)
    """
    #series = dict (doc mongo)
    values = []
    values.append(["Period", "Value"])
    for val in series['values']:
        values.append([val["period"], val["value"]])
    return values

def export_dataset(db, dataset):
    """Export all series for one Dataset
    
    Return array - one line by serie    
    """
    #TODO: Utiliser une queue Redis car trop de code en RAM ?
    
    start = time.time()
    
    ck = list(dataset['dimension_list'].keys())
    
    cl = sorted(ck, key=lambda t: t.lower())
    #['freq', 'geo', 'na_item', 'nace_r2', 'unit']
    
    headers = ['key'] + cl    
    #['key', 'freq', 'geo', 'na_item', 'nace_r2', 'unit']
    
    # revient à 0 et -1 ?
    dmin = float('inf')
    dmax = -float('inf')

    series_list = db[constants.COL_SERIES].find({'provider_name': dataset['provider_name'],
                                            "dataset_code": dataset['dataset_code']},
                                           #{'revisions': 0, 'release_dates': 0},
                                           )
    
    for s in series_list:
        #collect la première et dernière date trouvé
        """
        Permet d'avoir ensuite une plage de date la plus ancienne à la plus récente
        car chaque série n'a pas toujours les mêmes dates
        """
        if s['start_date'] < dmin:
            dmin = s['start_date']
        if s['end_date'] > dmax:
            dmax = s['end_date']
        freq = s['frequency']
        
    series_list.rewind()

    pDmin = pandas.Period(ordinal=dmin, freq=freq);
    pDmax = pandas.Period(ordinal=dmax, freq=freq);
    headers += list(pandas.period_range(pDmin, pDmax, freq=freq).to_native_types())
    #['key', 'freq', 'geo', 'na_item', 'nace_r2', 'unit', '1995', '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014']

    elements = [headers]
    
    series_list.rewind()
    
    def row_process(s):
        row = [s['key']]
        
        for c in cl:
            if c in s['dimensions']:
                row.append(s['dimensions'][c])
            else:
                row.append('')        
        
        p_start_date = pandas.Period(ordinal=s['start_date'], freq=freq)        
        p_end_date = pandas.Period(ordinal=s['end_date'], freq=freq)
        
        """
        pDmin : pandas.Period() la plus ancienne
        p_start_date-1 : périod en cours -1
            >>> p_start_date -1
            Period('1994', 'A-DEC')
            Bug: ne renvoi rien si
                p_start_date -1 devient identique à pDmin
        """

        # Les None sont pour les périodes qui n'ont pas de valeur correspondantes
        _row = [None for d in pandas.period_range(pDmin, p_start_date-1, freq=freq)]
        row.extend(_row)
        
        _row = [val["value"] for val in s['values']]
        row.extend(_row)

        _row = [None for d in pandas.period_range(p_end_date+1, pDmax, freq=freq)]
        row.extend(_row)
        
        return row
    
    for s in series_list:
        elements.append(row_process(s))
    
    end = time.time() - start
    logger.info("export_dataset - %s : %.3f" % (dataset['dataset_code'], end))
    
    return elements


def record_csv_file(db, values, 
                    provider_name=None, dataset_code=None, key=None, 
                    slug=None, prefix=None):
    """record gridfs and return mongo id of gridfs entry
    """
    
    fs = gridfs.GridFS(db)

    tmp_filepath = tempfile.mkstemp(suffix=".csv", 
                                    prefix='widukind_%s' % prefix, 
                                    text=True)[1]
    
    #TODO: utf8 ?
    #TODO: headers
    with open(tmp_filepath, 'w', newline='', encoding='utf8') as fp:
        writer = csv.writer(fp, quoting=csv.QUOTE_NONNUMERIC)
        for v in values:
            writer.writerow(v)

    filename = "%s.csv" % generate_filename(provider_name=provider_name, 
                                            dataset_code=dataset_code, 
                                            key=key,
                                            slug=slug, 
                                            prefix=prefix)

    metadata = {
        "doc_type": prefix,
        'provider_name': provider_name,
        "dataset_code": dataset_code
    }
    if key: 
        metadata['key'] = key
    if slug: 
        metadata['slug'] = slug

    grid_in = fs.new_file(filename=filename, 
                          contentType="text/csv", 
                          metadata=metadata,
                          encoding='utf8')
    
    with open(tmp_filepath, 'r') as fp:
        rows = iter(fp)
        for row in rows:
            grid_in.write(row)
        
    grid_in.close()
    return grid_in._id

def export_file_csv_series_unit(doc=None, provider=None, dataset_code=None, key=None, slug=None):
    """Create CSV File from one series and record in MongoDB GridFS
    """

    db = get_mongo_db()

    if not doc:
        if slug:
            doc = db[constants.COL_SERIES].find_one({"slug": slug})
        else:
            if not provider:
                raise ValueError("provider is required")
            if not dataset_code:
                raise ValueError("dataset_code is required")
            if not key:
                raise ValueError("key is required")
    
            query = {}
            query['provider_name'] = provider
            query['dataset_code'] = dataset_code
            query['key'] = key
        
            doc = db[constants.COL_SERIES].find_one(query)
            
    if not doc:
        raise Exception("Series not found : %s" % key)
    
    values = export_series(doc)

    return record_csv_file(db, values, 
                           provider_name=doc['provider_name'],
                           dataset_code=doc["dataset_code"],
                           key=doc["key"],
                           slug=doc["slug"], 
                           prefix="series")

def export_file_csv_dataset_unit(doc=None, provider=None, dataset_code=None):
    """Create CSV File from one Dataset and record in MongoDB GridFS
    """

    db = get_mongo_db()
    
    if not doc:
        if not provider:
            raise ValueError("provider is required")
        if not dataset_code:
            raise ValueError("dataset_code is required")
    
        query = {}
        query['provider_name'] = provider
        query['dataset_code'] = dataset_code
    
        doc = db[constants.COL_DATASETS].find_one(query, {'revisions': 0})
    
    if not doc:
        raise Exception("Document not found for provider[%s] - dataset[%s]" % (provider, dataset_code))
    
    values = export_dataset(db, doc)
    
    return record_csv_file(db,
                         values, 
                         provider_name=doc['provider_name'],
                         dataset_code=doc["dataset_code"],
                         slug=doc["slug"], 
                         prefix="dataset")

def export_file_csv_dataset(provider=None, dataset_code=None, slug=None):
    """Create CSV File from one or more Dataset and record in MongoDB GridFS
    """
    
    db = get_mongo_db()
    projection = {'concepts': False, "codelists": False}
    
    query = {}
    if slug:
        query["slug"] = slug
    else:
        query['provider_name'] = provider
        query['dataset_code'] = dataset_code

    datasets = db[constants.COL_DATASETS].find(query, projection)

    return [export_file_csv_dataset_unit(doc=doc) for doc in datasets]

