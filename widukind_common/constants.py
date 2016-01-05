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

FREQ_ANNUALY = "A"
FREQ_MONTHLY = "M"
FREQ_QUATERLY = "Q"
FREQ_WEEKLY = "W"
FREQ_DAILY = "D"

FREQUENCIES = (
    (FREQ_ANNUALY, "Annualy"),
    (FREQ_MONTHLY, "Monthly"),
    (FREQ_QUATERLY, "Quaterly"),
    (FREQ_WEEKLY, "Weekly"),
    (FREQ_DAILY, "Daily"),
)

FREQUENCIES_DICT = dict(FREQUENCIES)

FREQUENCIES_CONVERT = {
    'Annualy': 'A',
    'annualy': 'A',
    'Monthly': 'M',
    'monthly': 'M',
    'Quaterly': 'Q',
    'quaterly': 'Q',
    'Weekly': 'W',
    'weekly': 'W',
    'Daily': 'D',
    'daily': 'D',
    'a': 'A',
    'd': 'D',
    'm': 'M',
    'q': 'Q',
    'w': 'W',
}

TAGS_EXCLUDE_WORDS = [
    "the",
    "to",
    "from",
    "of",
    "on",
    "in"
]
