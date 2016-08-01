# -*- coding: utf-8 -*-

from collections import OrderedDict
import math
from datetime import datetime
from pprint import pprint

from flask import current_app, abort, request

from pymongo import ReadPreference
from pymongo.cursor import Cursor

from widukind_common import constants

__all__ = [
    'col_providers',       
    'col_datasets',
    'col_categories',
    'col_series',
    'col_series_archives',
    'col_counters',
    
    'complex_queries_series',
    
    'get_provider',
    'get_dataset',
    'col_stats_run',
    
    'Pagination',
]

def col_providers(db=None):
    db = db or current_app.widukind_db
    return db[constants.COL_PROVIDERS].with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)

def col_datasets(db=None):
    db = db or current_app.widukind_db
    return db[constants.COL_DATASETS].with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)

def col_categories(db=None):
    db = db or current_app.widukind_db
    return db[constants.COL_CATEGORIES].with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)

def col_series(db=None):
    db = db or current_app.widukind_db
    return db[constants.COL_SERIES].with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)

def col_series_archives(db=None):
    db = db or current_app.widukind_db
    return db[constants.COL_SERIES_ARCHIVES].with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)

def col_counters(db=None):
    db = db or current_app.widukind_db
    return db[constants.COL_COUNTERS].with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)

def col_stats_run(db=None):
    db = db or current_app.widukind_db
    return db[constants.COL_STATS_RUN].with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)

def complex_queries_series(query=OrderedDict(), 
                           search_attributes=True, 
                           bypass_args=['limit', 
                                        'tags', 
                                        'provider', 
                                        'dataset',
                                        'per_page',
                                        'page',
                                        'format']):

    tags = request.args.get('tags', None)
    
    search_fields = []
    query_and = []
    
    for r in request.args.lists():
        if r[0] in bypass_args:
            continue
        elif r[0] == 'frequency':
            query['frequency'] = r[1][0]
        else:
            search_fields.append((r[0], r[1][0]))

    if tags and len(tags.split()) > 0:
        tags = tags.split()
        conditions = [{"tags": {"$regex": ".*%s.*" % value.lower()}} for value in tags]
        #query = {"$and": conditions}
        #tags_regexp = [re.compile('.*%s.*' % e, re.IGNORECASE) for e in tags]
        #query["tags"] = {"$all": tags_regexp}
        query_and.append({"$and": conditions})
        
    if search_fields:
        
        query_or_by_field = {}
        query_nor_by_field = {}

        for field, value in search_fields:
            values = value.split()
            value = [v.lower().strip() for v in values]
            
            dim_field = field.lower()
            
            for v in value:
                if v.startswith("!"):
                    if not dim_field in query_nor_by_field:
                        query_nor_by_field[dim_field] = []
                    query_nor_by_field[dim_field].append(v[1:])
                else:
                    if not dim_field in query_or_by_field:
                        query_or_by_field[dim_field] = []
                    query_or_by_field[dim_field].append(v)
        
        for key, values in query_or_by_field.items():
            q_or = {"$or": [
                {"dimensions.%s" % key: {"$in": values}},
            ]}
            if search_attributes:
                q_or["$or"].append({"attributes.%s" % key: {"$in": values}})
                
            query_and.append(q_or)

        for key, values in query_nor_by_field.items():
            q_or = {"$nor": [
                {"dimensions.%s" % key: {"$in": values}},
            ]}
            if search_attributes:
                q_or["$nor"].append({"attributes.%s" % key: {"$in": values}})
            
            query_and.append(q_or)

    if query_and:
        query["$and"] = query_and
            
    print("-----complex query-----")
    pprint(query)    
    print("-----------------------")
        
    return query
    
def get_provider(slug, projection=None):
    projection = projection or {"_id": False}
    provider_doc = col_providers().find_one({'slug': slug, "enable": True}, 
                                            projection=projection)
    if not provider_doc:
        abort(404)
        
    return provider_doc

def get_dataset(slug, projection=None):
    ds_projection = projection or {"_id": False, "slug": True, "name": True,
                                   "provider_name": True, "dataset_code": True,
                                   "enable": True}
    if "enable" in ds_projection and ds_projection["enable"] is False:
        ds_projection["enable"] = True
    dataset_doc = col_datasets().find_one({"slug": slug},
                                          ds_projection)
    
    if not dataset_doc:
        abort(404)
    if dataset_doc["enable"] is False:
        abort(307, "disable dataset.")
        
    return dataset_doc
    
class Pagination(object):

    def __init__(self, iterable, page, per_page):

        if page < 1:
            abort(404)

        self.iterable = iterable
        self.page = page
        self.per_page = per_page

        if isinstance(iterable, Cursor):
            self.total = iterable.count()
        else:
            self.total = len(iterable)

        start_index = (page - 1) * per_page
        end_index = page * per_page

        self.items = iterable[start_index:end_index]
        #if isinstance(self.items, Cursor):
        #    self.items = self.items.select_related()
        if not self.items and page != 1:
            abort(404)

    @property
    def pages(self):
        """The total number of pages"""
        return int(math.ceil(self.total / float(self.per_page)))

    def prev(self, error_out=False):
        """Returns a :class:`Pagination` object for the previous page."""
        assert self.iterable is not None, ('an object is required '
                                           'for this method to work')
        iterable = self.iterable
        if isinstance(iterable, Cursor):
            iterable.skip(0)
            iterable.limit(0)
        return self.__class__(iterable, self.page - 1, self.per_page)

    @property
    def prev_num(self):
        """Number of the previous page."""
        return self.page - 1

    @property
    def has_prev(self):
        """True if a previous page exists"""
        return self.page > 1

    def next(self, error_out=False):
        """Returns a :class:`Pagination` object for the next page."""
        assert self.iterable is not None, ('an object is required '
                                           'for this method to work')
        iterable = self.iterable
        if isinstance(iterable, Cursor):
            iterable.skip(0)
            iterable.limit(0)
        return self.__class__(iterable, self.page + 1, self.per_page)

    @property
    def has_next(self):
        """True if a next page exists."""
        return self.page < self.pages

    @property
    def next_num(self):
        """Number of the next page"""
        return self.page + 1

    def iter_pages(self, left_edge=2, left_current=2,
                   right_current=5, right_edge=2):
        """Iterates over the page numbers in the pagination.  The four
        parameters control the thresholds how many numbers should be produced
        from the sides.  Skipped page numbers are represented as `None`.
        This is how you could render such a pagination in the templates:

        .. sourcecode:: html+jinja

            {% macro render_pagination(pagination, endpoint) %}
              <div class=pagination>
              {%- for page in pagination.iter_pages() %}
                {% if page %}
                  {% if page != pagination.page %}
                    <a href="{{ url_for(endpoint, page=page) }}">{{ page }}</a>
                  {% else %}
                    <strong>{{ page }}</strong>
                  {% endif %}
                {% else %}
                  <span class=ellipsis>…</span>
                {% endif %}
              {%- endfor %}
              </div>
            {% endmacro %}
        """
        last = 0
        for num in range(1, self.pages + 1):
            if (num <= left_edge or
                (num >= self.page - left_current and
                 num <= self.page + right_current) or
                num > self.pages - right_edge):
                if last + 1 != num:
                    yield None
                yield num
                last = num
        if last != self.pages:
            yield None