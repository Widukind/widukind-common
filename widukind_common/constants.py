# -*- coding: utf-8 -*-

import os

MONGODB_URL = os.environ.get("WIDUKIND_MONGODB_URL", "mongodb://localhost/widukind")

COL_CATEGORIES = "categories"

COL_PROVIDERS = "providers"

COL_DATASETS = "datasets"

COL_SERIES = "series"

COL_TAGS_DATASETS = "tags.datasets"

COL_TAGS_SERIES = "tags.series"

COL_LOCK = "lock"

COL_COUNTERS = "counters"

COL_QUERIES = "queries"

COL_LOGS = "logs"

COL_ALL = [
    COL_CATEGORIES,
    COL_PROVIDERS,
    COL_DATASETS,
    COL_SERIES,
    COL_TAGS_DATASETS,
    COL_TAGS_SERIES,
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
FREQ_DAILY = "D"
FREQ_HOURLY = "H"

FREQUENCIES = (
    (FREQ_ANNUALY, "Annually"),
    (FREQ_MONTHLY, "Monthly"),
    (FREQ_QUATERLY, "Quarterly"),
    (FREQ_WEEKLY, "Weekly"),
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

TAGS_EXCLUDE_WORDS = [
    "the",
    "to",
    "from",
    "of",
    "on",
    "in"
]
