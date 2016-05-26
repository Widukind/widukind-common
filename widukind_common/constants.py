# -*- coding: utf-8 -*-

import os

MONGODB_URL = os.environ.get("WIDUKIND_MONGODB_URL", "mongodb://localhost/widukind")

COL_CATEGORIES = "categories"

COL_CALENDARS = "calendars"

COL_PROVIDERS = "providers"

COL_DATASETS = "datasets"

COL_SERIES = "series"

COL_TAGS = "tags"

COL_LOCK = "lock"

COL_COUNTERS = "counters"

COL_QUERIES = "queries"

COL_LOGS = "logs"

COL_STATS_RUN = "stats_run"

COL_ALL = [
    COL_CATEGORIES,
    COL_PROVIDERS,
    COL_DATASETS,
    COL_SERIES,
    COL_TAGS,
    COL_LOCK,
    COL_COUNTERS,
    COL_QUERIES,
    COL_LOGS
]

CACHE_FREQUENCY = [
    "A", 
    "M", 
    "Q", 
    "W",
    "W-SUN",
    "W-MON",
    "W-TUE",
    "W-WED",
    "W-THU",
    "W-FRI",
    "W-SAT",
    #"D", 
]

FREQ_ANNUALY = "A"
FREQ_QUATERLY = "Q"
FREQ_MONTHLY = "M"
FREQ_WEEKLY = "W"
FREQ_WEEKLY_WED = "W-WED"
FREQ_DAILY = "D"
FREQ_HOURLY = "H"

FREQUENCIES = (
    (FREQ_ANNUALY, "Annually"),
    (FREQ_MONTHLY, "Monthly"),
    (FREQ_QUATERLY, "Quarterly"),
    (FREQ_WEEKLY, "Weekly"),
    (FREQ_WEEKLY_WED, "Weekly Wednesday"),
    (FREQ_DAILY, "Daily"),
    (FREQ_HOURLY, "Hourly"),
)

FREQUENCIES_DICT = dict(FREQUENCIES)

"""
FREQUENCIES_CONVERT = {
    'Annually': 'A',
    'annually': 'A',
    'Monthly': 'M',
    'monthly': 'M',
    'Quarterly': 'Q',
    'quarterly': 'Q',
    'Weekly': 'W',
    'weekly': 'W',
    'Daily': 'D',
    'daily': 'D',
    'Hourly': 'H',
    'hourly': 'H',
    'a': 'A',
    'q': 'Q',
    'm': 'M',
    'w': 'W',
    'd': 'D',
    'h': 'H',
}
"""

# nltk stopwords
# http://www.nltk.org/data.html
TAGS_EXCLUDE_WORDS = [
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 
    'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 
    'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 
    'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 
    'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 
    'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 
    'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 
    'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 
    'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 
    'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 
    'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 
    'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 
    'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 
    'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now', 'd', 'll', 
    'm', 'o', 're', 've', 'y', 'ain', 'aren', 'couldn', 'didn', 'doesn', 'hadn', 
    'hasn', 'haven', 'isn', 'ma', 'mightn', 'mustn', 'needn', 'shan', 'shouldn', 
    'wasn', 'weren', 'won', 'wouldn']
