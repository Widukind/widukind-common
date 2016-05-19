from datetime import datetime

from flask import json, request
from flask import current_app as app

from bson import json_util
from bson import ObjectId

import arrow

def json_convert(obj):

    if isinstance(obj, ObjectId):
        return str(obj)
    
    elif isinstance(obj, datetime):        
        return arrow.get(obj).for_json() #'2015-11-16T23:38:04.551214+00:00'

    return json_util.default(obj)

def json_response(obj, meta={}):
    indent = None
    if request.is_xhr:
        indent = 4
    context = {"meta": meta, "data": obj}
    value_str = json.dumps(context, default=json_convert, indent=indent)
    #json.loads(obj, object_hook=json_util.object_hook)
    #value_str = json_util.dumps(context, default=json_convert, indent=indent)
    return app.response_class(value_str, mimetype='application/json')

def json_response_async(docs, meta={}):
    indent = None
    if request.is_xhr:
        indent = 4
    
    count = len(docs)    
    
    def generate():
        yield '{"data": ['
        for i, row in enumerate(docs):
            yield json.dumps(row, default=json_convert, indent=indent)
            if i < count -1:
                yield ","
        yield "]"
        if meta:
            yield ', "meta": ' + json.dumps(meta, default=json_convert, indent=indent)
        yield "}"

    return app.response_class(generate(), mimetype='application/json')    

