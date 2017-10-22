__author__ = 'fridman'

import os
from datetime import datetime, timedelta

def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta

def save_date(filename, pull_date):
    """ Save pull_date to filename """
    with open(filename, 'w') as f:
        f.write(pull_date.strftime('%Y-%m-%d %H:%M'))

def load_date(filename, default_date):
    """ Load date in filename or return default_date if file doesn't exist"""
    if not os.path.exists(filename):
        return default_date

    with open(filename, 'r') as f:
        string = f.readline().strip()
        return datetime.strptime(string, '%Y-%m-%d %H:%M')

def date_range_hourly(start_date):
    """Returns hourly date ranges from start date to now """
    lst = []
    end_date = datetime.now()
    for dt in perdelta(start_date, end_date, timedelta(hours=1)):
        lst.append(dt)
    return lst

def latest_date(dates):
    """Returns most recent date in list of dates """
    return sorted(dates)[-1]

def extract_filename(path):
    return path.split('/')[-1]

def extract_log_type(path):
    log_types = ['impressions', 'clicks', 'conversions', 'videoevents']
    for type in log_types:
        if type in path:
            return type
    return None
