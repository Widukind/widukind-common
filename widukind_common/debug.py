
import logging
from collections import deque, OrderedDict
import time
from functools import wraps

LOGS = deque()
STATS = OrderedDict()

logger = logging.getLogger(__name__)

TRACE_ENABLE = False

def flush_logs():
    if not TRACE_ENABLE or not LOGS or len(LOGS) == 0:
        return
    
    for i, log in enumerate(LOGS): 
        print("DEBUG: %s:%s" % (i, log))
        
    for k, v in STATS.items():
        print("STATS: %s : %s : %2.4f" % (k, v["count"], v["duration"]))

    LOGS.clear()

def timeit(name, with_args=False, stats_only=False):
    def timeit_wrapped(f):
        @wraps(f)
        def timed(*args, **kw):
            if not TRACE_ENABLE:
                try:
                    #TODO: remonter l'exception original
                    return f(*args, **kw)
                except:
                    raise
            
            if not name in STATS:
                STATS[name] = {"count": 0, "duration": 0}
            STATS[name]["count"] += 1
            
            ts = time.time()
            result = f(*args, **kw)
            te = time.time()
            duration = te-ts
            if with_args is True:
                msg = 'func:%s args:[%r, %r] took: %2.4f sec' % (name, args, kw, duration)
            else:
                msg = 'func:%s took: %2.4f sec' % (name, duration)
            
            STATS[name]["duration"] += duration
            if not stats_only:
                LOGS.append(msg)
            return result
        return timed
    return timeit_wrapped

